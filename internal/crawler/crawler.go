package crawler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"driftnode/internal/core"
	"driftnode/internal/store"
)

// Crawler performs a breadth-first walk of the public follow-graph, syncing
// only Profile logs (section 9.3). Profile logs contain no post history, so
// crawling thousands of accounts never touches anyone's content stream.
//
// The set of visited identities is a persisted LRU in the store
// (bucketCrawlLRU), not an in-memory map. Touching an identity on crawl moves
// it to the hot end; when the on-disk cache exceeds maxCache the coldest
// entries are evicted, so the cache stays bounded across runs and restarts.
type Crawler struct {
	store    *store.Store
	logger   *slog.Logger
	mu       sync.Mutex
	queue    []core.Identity
	maxDepth int
	maxCache int
	running  bool
	lastRun  time.Time
}

// New creates a Crawler backed by the given store.
func New(s *store.Store, logger *slog.Logger) *Crawler {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Crawler{
		store:    s,
		logger:   logger,
		maxDepth: 5,
		maxCache: 10000,
	}
}

// SetMaxDepth sets the maximum BFS depth.
func (c *Crawler) SetMaxDepth(d int) { c.maxDepth = d }

// SetMaxCache sets the maximum number of cached profiles.
func (c *Crawler) SetMaxCache(n int) { c.maxCache = n }

// Fetcher is the interface for fetching a remote identity's Profile log
// and follower set. The real implementation uses the sync protocol over a
// tailcat tunnel; the crawler is tested with an in-memory fake.
type Fetcher interface {
	FetchProfileLog(ctx context.Context, id core.Identity) ([]core.SignedEvent, error)
	// FetchFollowers asks the followed zen directly for its own follower
	// set, returned as the signed Follow events it has received. Each
	// event is self-certifying; the crawler extracts the author as a
	// follower.
	FetchFollowers(ctx context.Context, id core.Identity) ([]core.SignedEvent, error)
}

// Seed adds starting identities for the crawl.
func (c *Crawler) Seed(seeds ...core.Identity) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range seeds {
		c.queue = append(c.queue, s)
	}
}

// Run performs a single crawl pass up to maxDepth, fetching Profile logs via
// the given Fetcher. It is safe to call Run again to continue from where the
// last crawl left off; identities tracked in the crawl LRU are not
// re-crawled. The on-disk cache is kept at maxCache via LRU eviction.
func (c *Crawler) Run(ctx context.Context, f Fetcher) (int, error) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return 0, fmt.Errorf("crawl already in progress")
	}
	c.running = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.running = false
		c.lastRun = time.Now()
		c.mu.Unlock()
	}()

	fetched := 0
	for depth := 0; depth < c.maxDepth; depth++ {
		c.mu.Lock()
		batch := c.queue
		c.queue = nil
		c.mu.Unlock()

		if len(batch) == 0 {
			break
		}

		var nextBatch []core.Identity
		for _, id := range batch {
			if ctx.Err() != nil {
				return fetched, ctx.Err()
			}
			visited, err := c.store.IsCrawlVisited(id)
			if err != nil {
				c.logger.Warn("crawl: visit check failed", "identity", id, "err", err)
				continue
			}
			if visited {
				continue
			}
			if err := c.store.TouchCrawl(id, c.nowNanos()); err != nil {
				c.logger.Warn("crawl: touch failed", "identity", id, "err", err)
				continue
			}
			if err := c.store.EvictCrawl(c.maxCache); err != nil {
				c.logger.Warn("crawl: evict failed", "err", err)
			}

			events, err := f.FetchProfileLog(ctx, id)
			if err != nil {
				c.logger.Warn("crawl: fetch profile log failed", "identity", id, "err", err)
				continue
			}
			fetched++

			// Store the crawled Profile log events.
			for _, se := range events {
				if se.Event.Log != core.ProfileLog {
					continue
				}
				if err := se.Verify(); err != nil {
					continue
				}
				if err := c.store.PutCrawledProfile(id, &se); err != nil {
					c.logger.Warn("crawl: store profile failed", "err", err)
				}
			}

			// Build the next frontier from both directions of the
			// follow graph (section 9.3). Follows are the out-edges:
			// targets in this identity's Profile log. Followers are the
			// in-edges: the Follow events the followed zen has received
			// targeting itself. The follower direction asks the
			// followed zen directly via FetchFollowers, rather than
			// scanning the local cache, because a zen knows its own
			// followers as first-class state derived from received events.
			log := core.NewLog(events)
			candidates := make(map[core.Identity]struct{})
			log.FollowSet().Each(func(target [32]byte) {
				candidates[core.IdentityFromPubkey(target[:])] = struct{}{}
			})
			followEvents, err := f.FetchFollowers(ctx, id)
			if err != nil {
				c.logger.Warn("crawl: fetch followers failed", "identity", id, "err", err)
			}
			for _, se := range followEvents {
				if se.Verify() != nil {
					continue
				}
				candidates[se.Author] = struct{}{}
			}
			for nextID := range candidates {
				nextBatch = append(nextBatch, nextID)
			}
		}

		// De-dup the next frontier against the LRU so already-visited
		// identities are not re-enqueued, and cap the queue at maxCache
		// to bound the working set.
		nextBatch = c.filterFrontier(nextBatch)
		c.mu.Lock()
		c.queue = append(c.queue, nextBatch...)
		c.mu.Unlock()
	}
	return fetched, nil
}

// filterFrontier drops identities already tracked in the crawl LRU and caps
// the queue at maxCache entries, so the breadth-first frontier does not grow
// without bound ahead of the eviction that bounds the stored cache.
func (c *Crawler) filterFrontier(batch []core.Identity) []core.Identity {
	seen := make(map[core.Identity]struct{}, len(batch))
	out := make([]core.Identity, 0, len(batch))
	for _, id := range batch {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		visited, err := c.store.IsCrawlVisited(id)
		if err != nil || visited {
			continue
		}
		out = append(out, id)
	}
	return out
}

// nowNanos returns the current time as unix nanoseconds for LRU touch keys.
func (c *Crawler) nowNanos() int64 { return time.Now().UnixNano() }

// VisitedCount returns the number of identities currently tracked in the
// crawl LRU.
func (c *Crawler) VisitedCount() int {
	n, err := c.store.CrawlVisitedCount()
	if err != nil {
		return 0
	}
	return n
}

// LastRun returns the time of the last completed crawl.
func (c *Crawler) LastRun() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastRun
}

// Reset clears the crawl LRU so the crawler can start fresh.
func (c *Crawler) Reset() {
	if err := c.store.ResetCrawl(); err != nil {
		c.logger.Warn("crawl: reset failed", "err", err)
	}
	c.mu.Lock()
	c.queue = nil
	c.mu.Unlock()
}
