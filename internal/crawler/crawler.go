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
type Crawler struct {
	store    *store.Store
	logger   *slog.Logger
	mu       sync.Mutex
	visited  map[core.Identity]bool
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
		visited:  make(map[core.Identity]bool),
		maxDepth: 5,
		maxCache: 10000,
	}
}

// SetMaxDepth sets the maximum BFS depth.
func (c *Crawler) SetMaxDepth(d int) { c.maxDepth = d }

// SetMaxCache sets the maximum number of cached profiles.
func (c *Crawler) SetMaxCache(n int) { c.maxCache = n }

// Fetcher is the interface for fetching a remote identity's Profile log.
// The real implementation uses the sync protocol over a tailcat tunnel; the
// crawler is tested with an in-memory fake.
type Fetcher interface {
	FetchProfileLog(ctx context.Context, id core.Identity) ([]core.SignedEvent, error)
}

// Seed adds starting identities for the crawl.
func (c *Crawler) Seed(seeds ...core.Identity) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range seeds {
		if !c.visited[s] {
			c.queue = append(c.queue, s)
		}
	}
}

// Run performs a single crawl pass up to maxDepth, fetching Profile logs via
// the given Fetcher. It is safe to call Run again to continue from where the
// last crawl left off; visited identities are not re-crawled.
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
			c.mu.Lock()
			if c.visited[id] || len(c.visited) >= c.maxCache {
				c.mu.Unlock()
				continue
			}
			c.visited[id] = true
			c.mu.Unlock()

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

			// Extract follow targets for the next depth level.
			log := core.NewLog(events)
			fs := log.FollowSet()
			fs.Each(func(target [32]byte) {
				nextID := core.IdentityFromPubkey(target[:])
				c.mu.Lock()
				if !c.visited[nextID] {
					nextBatch = append(nextBatch, nextID)
				}
				c.mu.Unlock()
			})
		}

		c.mu.Lock()
		c.queue = append(c.queue, nextBatch...)
		c.mu.Unlock()
	}
	c.logger.Info("crawl complete", "fetched", fetched, "visited", len(c.visited))
	return fetched, nil
}

// VisitedCount returns the number of identities visited so far.
func (c *Crawler) VisitedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.visited)
}

// LastRun returns the time of the last completed crawl.
func (c *Crawler) LastRun() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastRun
}

// Reset clears the crawl state so it can start fresh.
func (c *Crawler) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.visited = make(map[core.Identity]bool)
	c.queue = nil
}
