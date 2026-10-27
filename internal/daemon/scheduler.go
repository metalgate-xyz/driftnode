package daemon

import (
	"context"
	"driftnode/internal/core"
	"driftnode/internal/proto/driftnodepb"
	syncproto "driftnode/internal/sync"
	"errors"
	"net"
	"sort"
	"sync"
	"time"
)

// connectAndSync dials a zen by its address token and runs a bidirectional
// sync session (section 7.3): this zen pulls the remote zen's logs, then
// serves its own logs back over the same connection. On success, the
// authenticated zen identity is bound to the token in the routing table so
// future sync rounds can dial it by identity. Zen tokens learned during the
// exchange are recorded for future dials (section 6.1).
//
// The per-dial timeout is derived from ctx: the lanes pass a context
// bounded by their lane dial timeout (head fast-skip 5s, tail 10s) so a
// non-answering follow releases its slot promptly; the on-demand path
// (follow-by-token, bootstrap, pending-resolution) passes a context with
// the full 30s dial timeout. ctx is also what Stop cancels to tear down
// in-flight dials.
func (d *Daemon) connectAndSync(ctx context.Context, token string) {
	d.mu.Lock()
	transport := d.transport
	d.mu.Unlock()
	if transport == nil {
		d.logger.Warn("zen dial failed: no transport configured", "token", token)
		d.setZenStatus(token, "error")
		return
	}
	conn, err := transport.Dial(ctx, token)
	if err != nil {
		d.logger.Warn("zen dial failed", "token", token, "err", err)
		d.setZenStatus(token, "error")
		return
	}
	defer conn.Close()
	d.setZenStatus(token, "connected")
	var zenID core.Identity
	merged, err := d.runSession(conn, true, &zenID)
	if err != nil {
		d.logger.Warn("sync round failed", "token", token, "err", err)
	}
	if zenID != "" {
		if err := d.store.PutRouting(zenID, token); err != nil {
			d.logger.Warn("routing bind failed", "zen", zenID, "err", err)
		}
	}
	// A successful session is a token mention: refresh it so tokens we
	// actually talk to stay hot in the persisted token LRU.
	if err == nil {
		if lerr := d.learnToken(token); lerr != nil {
			d.logger.Warn("learn dialed token failed", "token", token, "err", lerr)
		}
	}
	d.logger.Info("sync round done", "token", token, "zen", zenID, "merged", merged)
}

// assessLoop runs the assess thread on a ticker. One body per pass, in this
// order: resolve pending follows, then recompute the weighted graph. It is
// the only routine that touches weight computation; the lanes derive the
// head/tail split from the persisted weights each pass.
func (d *Daemon) assessLoop(ctx context.Context) {
	d.logger.Info("assess loop started", "period", assessPeriod)
	defer d.logger.Info("assess loop stopped")
	ticker := time.NewTicker(assessPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.assess(ctx)
		}
	}
}

// assess runs one pass of the assess thread: resolve pending follows, then
// recompute and persist the weighted graph. The lanes read the persisted
// weights and routing table to derive head/tail each pass; this is the only
// routine that recomputes weights, so a follow's promote/demote is driven
// by the assess cadence, not by the lanes.
func (d *Daemon) assess(ctx context.Context) {
	d.logger.Info("assess pass start")
	start := time.Now()
	ids, err := d.store.FollowGraph()
	if err != nil {
		d.logger.Warn("assess: read follow graph", "err", err)
		return
	}
	d.logger.Info("assess:", "follows", len(ids))
	// Bound vs. pending from one routing read instead of one per follow.
	routing, err := d.store.AllRouting()
	if err != nil {
		d.logger.Warn("assess: read routing", "err", err)
		return
	}
	var pending []core.Identity
	for _, id := range ids {
		if _, ok := routing[id]; !ok {
			pending = append(pending, id)
		}
	}
	d.logger.Info("assess:", "pending", len(pending))
	// 1. Resolve pending follows: probing known tokens is the only way to
	// learn a binding for an offline-by-pubkey follow, and a resolved
	// follow enters the weighted graph this pass and becomes dialable by
	// the lanes on the next.
	if len(pending) > 0 {
		d.resolvePendingFollows(ctx, pending)
	}
	// 2. Derive weights and re-split. The resolution above may have bound
	// pending follows, so deriveSplit re-reads routing to include them.
	n := d.deriveSplit(ids)
	d.logger.Info("assess:", "weights", n)
	d.logger.Info("assess pass done", "elapsed", time.Since(start))
}

// deriveSplit recomputes the weighted graph from the follow graph's
// interaction signals, then re-derives and caches the head/tail split. It is
// the cheap part of an assess pass: no dials, no pending resolution, just a
// single weight recompute (one bbolt read tx over the follow graph) and a
// sort. assess calls it after resolving pending follows; user-action
// handlers (follow, pin, unfollow, unpin) call it directly so the ranking
// reflects the change on the next lane pass instead of waiting up to
// assessPeriod. Holding splitMu around the recompute and the split keeps the
// lanes on a consistent snapshot.
func (d *Daemon) deriveSplit(ids []core.Identity) int {
	d.splitMu.Lock()
	defer d.splitMu.Unlock()
	weights, err := d.store.RecomputeWeights()
	if err != nil {
		d.logger.Warn("derive split: recompute weights", "err", err)
		return 0
	}
	routing, err := d.store.AllRouting()
	if err != nil {
		d.logger.Warn("derive split: read routing", "err", err)
		return len(weights)
	}
	d.headSet, d.tailSet = splitHeadTail(ids, weights, routing)
	return len(weights)
}

// refreshSplit reads the follow graph and derives the split, for the
// user-action handlers that don't already hold the ids. assess reads the
// graph once and passes the ids to deriveSplit to avoid a second read.
func (d *Daemon) refreshSplit() {
	ids, err := d.store.FollowGraph()
	if err != nil {
		d.logger.Warn("refresh split: read follow graph", "err", err)
		return
	}
	d.deriveSplit(ids)
}

// splitHeadTail splits the bound follows into head and tail: head = the
// top-K highest-weight bound follows, tail = the rest, with the identity
// string as a deterministic tiebreak for stable ordering. Pure: no I/O, no
// receiver state. assess calls it once per pass and caches the result for
// the lanes; computing the split in two places (one per lane) would run the
// sort twice and, worse, could race a weight derivation between the two
// calls so head and tail derive from different weight snapshots.
func splitHeadTail(ids []core.Identity, weights map[core.Identity]float64, routing map[core.Identity]string) (head, tail []core.Identity) {
	type entry struct {
		id core.Identity
		w  float64
	}
	var entries []entry
	for _, id := range ids {
		if _, ok := routing[id]; !ok {
			continue // not bound
		}
		entries = append(entries, entry{id: id, w: weights[id]})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].w != entries[j].w {
			return entries[i].w > entries[j].w
		}
		return entries[i].id < entries[j].id
	})
	// The head is the inner "meaningful" layer: about one-third of the
	// bound follows, capped at maxHeadSize (Dunbar's active network
	// ceiling). Each Dunbar layer is ~3x the one inside it, so a third
	// keeps the head a true inner circle and leaves the tail real work
	// rather than putting the whole graph in the fast lane.
	k := min(len(entries)/3, maxHeadSize)
	if k < 1 && len(entries) > 0 {
		k = 1
	}
	for i, e := range entries {
		if i < k {
			head = append(head, e.id)
		} else {
			tail = append(tail, e.id)
		}
	}
	return head, tail
}

// snapshotSplit returns a copy of the cached head/tail split for a lane pass.
// It holds splitMu only long enough to copy the slices, so a lane dials a
// consistent set even if assess publishes the next split while the pass runs.
func (d *Daemon) snapshotSplit() (head, tail []core.Identity) {
	d.splitMu.Lock()
	head = append(head, d.headSet...)
	tail = append(tail, d.tailSet...)
	d.splitMu.Unlock()
	return head, tail
}

// seedSplit populates the head/tail split from the persisted weights and
// routing table without deriving (no dials, no recompute). Called from Start
// before the lanes launch so their first pass reads a populated split instead
// of empty sets, then waits for the first assess pass. No lock: the lanes
// have not started yet.
func (d *Daemon) seedSplit() {
	ids, err := d.store.FollowGraph()
	if err != nil {
		d.logger.Warn("seed split: read follow graph", "err", err)
		return
	}
	routing, err := d.store.AllRouting()
	if err != nil {
		d.logger.Warn("seed split: read routing", "err", err)
		return
	}
	weights, err := d.store.WeightedGraph()
	if err != nil {
		d.logger.Warn("seed split: read weighted graph", "err", err)
		return
	}
	d.headSet, d.tailSet = splitHeadTail(ids, weights, routing)
}

// resolvePendingFollows dials known tokens to find the routing for followed
// identities that have no bound token. Each token is dialed and a handshake
// reveals the zen's identity; if that identity is in the pending set, the
// token is bound and the identity is synced. Used when a follow was recorded
// by pubkey (offline) and the token is learned later via zen exchange or
// another sync. ctx bounds each dial (and is cancelled on Stop).
//
// Only the handshake runs for non-matching tokens: a full sync session pulls
// the remote zen's PostLog into the local follows cache, so syncing with a
// token that is not a followed identity would leak that zen's posts into the
// feed's source set. Binding the token here makes the confirmed follow
// "bound", and the lanes pull its logs on the next pass.
func (d *Daemon) resolvePendingFollows(ctx context.Context, pending []core.Identity) {
	pendingSet := make(map[core.Identity]bool, len(pending))
	for _, id := range pending {
		pendingSet[id] = true
	}
	d.mu.Lock()
	kp := d.unlocked
	transport := d.transport
	d.mu.Unlock()
	if kp == nil {
		d.logger.Warn("resolve: key locked; cannot probe pending follows")
		return
	}
	for _, ref := range d.knownRefsList() {
		tok := ref.Token
		dialCtx, cancel := context.WithTimeout(ctx, onDemandDialTimeout)
		conn, err := transport.Dial(dialCtx, tok)
		if err != nil {
			cancel()
			continue
		}
		sess := syncproto.NewSession(d.store, d.logger)
		sess.SetKey(kp)
		zenID, err := sess.Handshake(conn, conn)
		conn.Close()
		cancel()
		if err != nil {
			d.logger.Warn("resolve: handshake failed", "token", tok, "err", err)
			continue
		}
		if !pendingSet[zenID] {
			continue
		}
		if err := d.store.PutRouting(zenID, tok); err != nil {
			d.logger.Warn("resolve: routing bind", "zen", zenID, "err", err)
			continue
		}
		// Now that the follow is bound, sync its logs. This is the only
		// sync in this path, and it runs solely for a confirmed follow.
		// The on-demand dial path keeps its 30s timeout derived from the
		// caller's context.
		syncCtx, syncCancel := context.WithTimeout(ctx, onDemandDialTimeout)
		d.connectAndSync(syncCtx, tok)
		syncCancel()
	}
}

// dialSet dials each identity's bound token with the given per-dial timeout
// and fan-out, deriving each dial's context from the shared lane context so
// Stop cancels in-flight dials promptly. A dial that does not answer within
// the timeout releases its slot (fast-skip), so the lane is not starved by
// unresponsive follows. fanout caps the parallel dials per pass. It returns
// the number of dials launched, for per-pass logging.
func (d *Daemon) dialSet(ctx context.Context, ids []core.Identity, timeout time.Duration, fanout int) int {
	if len(ids) == 0 {
		return 0
	}
	if fanout <= 0 {
		fanout = 1
	}
	// Resolve tokens up front so the dial loop does not re-enter the
	// routing table per identity.
	var tokens []string
	for _, id := range ids {
		tok, ok, _ := d.store.Routing(id)
		if !ok {
			continue
		}
		tokens = append(tokens, tok)
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, fanout)
	for _, tok := range tokens {
		wg.Add(1)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Done()
			return len(tokens)
		}
		go func(token string) {
			defer wg.Done()
			defer func() { <-sem }()
			dialCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			d.connectAndSync(dialCtx, token)
		}(tok)
	}
	wg.Wait()
	return len(tokens)
}

// headLane continuously dials the head set with fast-skip: a head follow
// that does not answer is skipped after headDialTimeout, releasing its
// concurrency slot in seconds instead of holding it. After each pass it
// pauses laneBackoff so a pass that completes fast does not busy spin. The
// point is snappy updates from the follows the user cares about.
func (d *Daemon) headLane(ctx context.Context) {
	d.logger.Info("head lane started", "fanout", headFanout, "dial_timeout", headDialTimeout, "backoff", laneBackoff)
	defer d.logger.Info("head lane stopped")
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		d.logger.Info("head lane pass start")
		start := time.Now()
		head, _ := d.snapshotSplit()
		n := d.dialSet(ctx, head, headDialTimeout, headFanout)
		d.logger.Info("head lane pass done", "head", len(head), "dialed", n, "elapsed", time.Since(start))
		select {
		case <-ctx.Done():
			return
		case <-time.After(laneBackoff):
		}
	}
}

// tailLane continuously dials the tail set in the background with a small
// fixed fan-out and a longer per-dial timeout: latency is acceptable here
// (liveness is the goal), but a follow that never answers still releases
// its slot. After each pass it pauses laneBackoff.
func (d *Daemon) tailLane(ctx context.Context) {
	d.logger.Info("tail lane started", "fanout", tailFanout, "dial_timeout", tailDialTimeout, "backoff", laneBackoff)
	defer d.logger.Info("tail lane stopped")
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		d.logger.Info("tail lane pass start")
		start := time.Now()
		_, tail := d.snapshotSplit()
		n := d.dialSet(ctx, tail, tailDialTimeout, tailFanout)
		d.logger.Info("tail lane pass done", "tail", len(tail), "dialed", n, "elapsed", time.Since(start))
		select {
		case <-ctx.Done():
			return
		case <-time.After(laneBackoff):
		}
	}
}

// runSession runs a bidirectional sync session over conn. When initiator is
// true, this side opened the connection and pulls first; otherwise the
// remote side opened it and we serve first. The session handshake
// authenticates both sides' Ed25519 identities before any sync traffic;
// zenID (if non-empty) receives the authenticated zen identity. The
// listener must have its signing key unlocked to participate; a locked
// daemon rejects inbound sessions.
func (d *Daemon) runSession(conn net.Conn, initiator bool, zenID *core.Identity) (int, error) {
	sess := syncproto.NewSession(d.store, d.logger)
	sess.SetZenSource(d.knownRefsList)
	sess.SetZenSink(d.learnZenRefs)
	sess.SetCursorSource(d.store.SyncCursor)
	sess.SetCursorSink(d.store.PutSyncCursor)
	// Collect Post/Reply events and inbound Follows merged during this
	// session so the daemon can push feed and follower diffs to subscribers
	// once the round completes.
	var mergedPosts []core.SignedEvent
	var newFollowers []core.Identity
	ownID, _, _ := d.store.Identity()
	var ownPub [32]byte
	if ownID != "" {
		if pub, err := ownID.PubkeyBytes(); err == nil {
			copy(ownPub[:], pub)
		}
	}
	sess.SetOnMerge(func(se *core.SignedEvent, inserted bool) {
		if !inserted {
			return
		}
		switch se.Event.Log {
		case core.PostLog:
			if se.Event.Kind != core.KindPost {
				return
			}
			mergedPosts = append(mergedPosts, *se)
		case core.ProfileLog:
			if se.Event.Kind == core.KindFollow && se.Event.Follow != nil &&
				ownPub != [32]byte{} && se.Event.Follow.TargetPubkey == ownPub {
				newFollowers = append(newFollowers, se.Author)
			}
		}
	})
	d.mu.Lock()
	kp := d.unlocked
	d.mu.Unlock()
	if kp == nil {
		return 0, errors.New("key is locked; cannot authenticate session")
	}
	sess.SetKey(kp)
	if zenID != nil {
		sess.SetAuthed(func(id core.Identity) { *zenID = id })
	}
	var n int
	var err error
	if initiator {
		n, err = sess.RunInitiator(conn, conn)
	} else {
		n, err = sess.RunListener(conn, conn)
	}
	if len(mergedPosts) > 0 {
		d.invalidateFeed()
		d.emitFeedAdd(d.feedItemsFor(mergedPosts))
	}
	for _, fid := range newFollowers {
		name := d.displayName(fid)
		d.emitFollowersAdd(&driftnodepb.Identity{Identity: fid.String(), Name: name})
	}
	return n, err
}
