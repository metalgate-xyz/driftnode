package daemon

import (
	"context"
	"driftnode/internal/core"
	syncproto "driftnode/internal/sync"
	"errors"
	"net"
	"time"
)

// handleFetchLikes dials each known zen and requests Like events targeting
// the given post IDs. Best-effort: only zens currently reachable respond,
// and each returns only what it happens to hold. Received like events are
// stored as crawled events (merged into the feed's source set by dedup).
// Returns all newly-received like events.
func (d *Daemon) handleFetchLikes(targetIDs []core.EventID) ([]core.SignedEvent, error) {
	d.mu.Lock()
	transport := d.transport
	kp := d.unlocked
	d.mu.Unlock()
	refs := d.knownRefsList()
	if transport == nil {
		return nil, errors.New("no transport configured")
	}
	if kp == nil {
		return nil, errors.New("key is locked; run 'driftnode daemon unlock'")
	}
	type result struct {
		events []core.SignedEvent
		err    error
	}
	results := make(chan result, len(refs))
	for _, ref := range refs {
		go func(ref syncproto.ZenRef) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			conn, err := transport.Dial(ctx, ref.Token)
			if err != nil {
				results <- result{err: err}
				return
			}
			defer conn.Close()
			sess := syncproto.NewSession(d.store, d.logger)
			sess.SetKey(kp)
			if _, err := sess.Handshake(conn, conn); err != nil {
				results <- result{err: err}
				return
			}
			cl := syncproto.NewClient(d.store, d.logger)
			likes, err := cl.FetchLikes(conn, conn, targetIDs)
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{events: likes}
		}(ref)
	}
	var all []core.SignedEvent
	for range refs {
		r := <-results
		if r.err != nil {
			d.logger.Debug("fetch likes: zen error", "err", r.err)
			continue
		}
		for _, se := range r.events {
			inserted, err := d.store.PutRemoteEvent(&se, uint64(se.Event.Sequence))
			if err != nil {
				d.logger.Warn("fetch likes: store", "err", err)
				continue
			}
			if inserted {
				all = append(all, se)
			}
		}
	}
	if len(all) > 0 {
		d.invalidateFeed()
	}
	return all, nil
}

// triggerCrawlNow signals the crawl loop to run an immediate crawl pass out
// of the periodic cadence, fired by user actions (post, follow, unfollow)
// and the sync RPC. It is non-blocking: the channel is buffered with
// capacity 1, so a pending signal is dropped if one is already queued.
func (d *Daemon) triggerCrawlNow() {
	select {
	case d.crawlNow <- struct{}{}:
	default:
	}
}

// crawlLoop periodically walks the follow graph fetching Profile logs
// (section 9.3) and fires on demand when crawlNow is signaled. The head
// lane's own continuous cadence delivers snappy sync of the top-K follows,
// so a separate head-sync trigger is not needed.
func (d *Daemon) crawlLoop(ctx context.Context) {
	d.logger.Info("crawl loop started", "interval", crawlInterval)
	defer d.logger.Info("crawl loop stopped")
	ticker := time.NewTicker(crawlInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.crawlNow:
			d.runCrawl(ctx)
		case <-ticker.C:
			d.runCrawl(ctx)
		}
	}
}

// runCrawl runs one crawl pass if the crawler has seeds.
func (d *Daemon) runCrawl(ctx context.Context) {
	d.mu.Lock()
	c := d.crawler
	d.mu.Unlock()
	if c == nil {
		return
	}
	d.logger.Info("crawl pass start")
	start := time.Now()
	fetched, err := c.Run(ctx, &crawlFetcher{d: d})
	if err != nil {
		d.logger.Warn("crawl failed", "err", err)
		return
	}
	d.logger.Info("crawl complete", "fetched", fetched, "visited", c.VisitedCount(), "elapsed", time.Since(start))
}

// crawlFetcher implements crawler.Fetcher by dialing known zens to fetch
// Profile logs.
type crawlFetcher struct{ d *Daemon }

func (f *crawlFetcher) FetchProfileLog(ctx context.Context, id core.Identity) ([]core.SignedEvent, error) {
	return f.d.crawlFetch(ctx, id)
}

func (f *crawlFetcher) FetchFollowers(ctx context.Context, id core.Identity) ([]core.SignedEvent, error) {
	return f.d.crawlFetchFollowers(ctx, id)
}

// crawlFetch fetches a remote identity's ProfileLog by dialing each known
// zen and requesting that identity's Profile log (section 9.3). Returns
// the events from the first zen that has them. Each dial runs the session
// handshake first, so the connection is authenticated before any sync
// traffic.
func (d *Daemon) crawlFetch(ctx context.Context, id core.Identity) ([]core.SignedEvent, error) {
	d.mu.Lock()
	kp := d.unlocked
	transport := d.transport
	d.mu.Unlock()
	if kp == nil {
		return nil, errors.New("key is locked; cannot authenticate crawl session")
	}
	tokens := d.knownRefsList()
	for _, ref := range tokens {
		tok := ref.Token
		conn, err := transport.Dial(ctx, tok)
		if err != nil {
			continue
		}
		sess := syncproto.NewSession(d.store, d.logger)
		sess.SetKey(kp)
		if _, err := sess.Handshake(conn, conn); err != nil {
			d.logger.Warn("crawl: handshake failed", "token", tok, "err", err)
			conn.Close()
			continue
		}
		events, err := syncproto.NewClient(d.store, d.logger).SyncLogRaw(conn, conn, core.ProfileLog, 0, id)
		conn.Close()
		if err != nil {
			continue
		}
		if len(events) > 0 {
			return events, nil
		}
	}
	return nil, nil
}

// crawlFetchFollowers fetches the follower set of the target identity's zen
// by dialing it and issuing MsgFollowers. Unlike crawlFetch, which can ask
// any zen that has replicated a log, the follower set is the followed zen's
// own state, so the request must reach that specific zen. The target's
// bound token is used if known; otherwise known tokens are probed until a
// handshake reveals the target identity.
func (d *Daemon) crawlFetchFollowers(ctx context.Context, id core.Identity) ([]core.SignedEvent, error) {
	d.mu.Lock()
	kp := d.unlocked
	transport := d.transport
	d.mu.Unlock()
	if kp == nil {
		return nil, errors.New("key is locked; cannot authenticate crawl session")
	}
	// Prefer the bound token if one exists.
	if tok, ok, _ := d.store.Routing(id); ok {
		conn, err := transport.Dial(ctx, tok)
		if err == nil {
			events, err := d.fetchFollowersOverConn(ctx, conn, kp)
			if err == nil {
				return events, nil
			}
		}
	}
	// Probe known tokens until a handshake reveals the target identity.
	for _, ref := range d.knownRefsList() {
		tok := ref.Token
		conn, err := transport.Dial(ctx, tok)
		if err != nil {
			continue
		}
		sess := syncproto.NewSession(d.store, d.logger)
		sess.SetKey(kp)
		var zenID core.Identity
		sess.SetAuthed(func(got core.Identity) { zenID = got })
		if _, err := sess.Handshake(conn, conn); err != nil {
			conn.Close()
			continue
		}
		if zenID != id {
			conn.Close()
			continue
		}
		events, err := syncproto.NewClient(d.store, d.logger).FetchFollowers(conn, conn)
		conn.Close()
		if err != nil {
			continue
		}
		return events, nil
	}
	return nil, nil
}

// fetchFollowersOverConn runs a handshake and a FetchFollowers request over
// an already-dialed connection.
func (d *Daemon) fetchFollowersOverConn(ctx context.Context, conn net.Conn, kp *core.KeyPair) ([]core.SignedEvent, error) {
	sess := syncproto.NewSession(d.store, d.logger)
	sess.SetKey(kp)
	if _, err := sess.Handshake(conn, conn); err != nil {
		conn.Close()
		return nil, err
	}
	events, err := syncproto.NewClient(d.store, d.logger).FetchFollowers(conn, conn)
	conn.Close()
	if err != nil {
		return nil, err
	}
	return events, nil
}
