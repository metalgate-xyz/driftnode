package daemon

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"testing"
	"time"

	"driftnode/internal/core"
	"driftnode/internal/store"
)

// blockingTransport implements Transport by blocking every Dial until the
// dial context is cancelled, then returning an error. It models a zen that
// never answers: the lane's per-dial timeout (or Stop) cancels the context,
// releasing the concurrency slot instead of holding it indefinitely.
type blockingTransport struct{}

func (blockingTransport) Dial(ctx context.Context, token string) (net.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// laneTransport routes head tokens to working in-process sync servers and
// tail tokens to a dial that blocks until its context is cancelled. This
// lets a single Transport stand up a mixed head/tail scenario for the
// scheduler regression tests.
type laneTransport struct {
	working map[string]pipeTransport
	block   map[string]bool
}

func (l laneTransport) Dial(ctx context.Context, token string) (net.Conn, error) {
	if l.block != nil && l.block[token] {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if srv, ok := l.working[token]; ok {
		return srv.Dial(ctx, token)
	}
	return nil, net.UnknownNetworkError(token)
}

// setupBoundFollow establishes a bound follow edge from alice to the given
// peer: signs a Follow event into alice's ProfileLog, records the follow
// state, optionally pins the edge (for top weight), and binds the routing
// token. It returns the peer's identity. Used to stand up a weighted follow
// graph directly in the store without going through the daemon RPC.
func setupBoundFollow(t *testing.T, s *store.Store, kp *core.KeyPair, peerKP *core.KeyPair, token string, pin bool) core.Identity {
	t.Helper()
	peerPub, err := peerKP.Identity().PubkeyBytes()
	if err != nil {
		t.Fatalf("peer pubkey: %v", err)
	}
	var pub [32]byte
	copy(pub[:], peerPub)
	if _, _, _, err := s.Follow(kp, pub); err != nil {
		t.Fatalf("follow: %v", err)
	}
	if pin {
		if err := s.PinIdentity(peerKP.Identity()); err != nil {
			t.Fatalf("pin: %v", err)
		}
	}
	if err := s.PutRouting(peerKP.Identity(), token); err != nil {
		t.Fatalf("routing: %v", err)
	}
	return peerKP.Identity()
}

// peerServer builds a peer store with a fresh identity and one signed post,
// returning the store, keypair, the post text, and the post's event ID.
func peerServer(t *testing.T, name string) (*store.Store, *core.KeyPair, string, core.EventID) {
	t.Helper()
	s := openTestStore(t)
	kp := initTestIdentityAt(t, s, name+"pass")
	text := name + " post"
	se := mustSignPost(t, kp, text, 1)
	if err := s.AppendOwnEvent(core.PostLog, se); err != nil {
		t.Fatalf("%s post: %v", name, err)
	}
	id, err := se.ID()
	if err != nil {
		t.Fatalf("%s post id: %v", name, err)
	}
	return s, kp, text, id
}

// TestHeadLaneFastSkipUnderUnreachableTail proves the head lane dials the
// head set with fast-skip and is not starved by an unreachable tail. Alice
// has 4 pinned head follows (working servers) and 8 cold-start tail follows
// (a transport that never answers): 12 bound follows, so K scales to 4 and
// the head set is exactly the pinned follows. The tail count meets the
// head fan-out, so a single shared dial pool would be saturated by the
// blocking tail dials and the head dials would wait for the tail timeout
// before running. Every head follow's post must land well within the head
// dial timeout (seconds), proving the head lane has its own concurrency
// pool separate from the tail.
func TestHeadLaneFastSkipUnderUnreachableTail(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	aliceStore := openTestStore(t)
	aliceKP := initTestIdentityAt(t, aliceStore, "alicepass")

	const numHeads = 4
	const numTail = headFanout // saturate a shared pool the size of headFanout
	type peer struct {
		kp   *core.KeyPair
		text string
		tok  string
	}
	heads := make([]peer, numHeads)
	tails := make([]peer, numTail)
	working := make(map[string]pipeTransport, numHeads)
	block := make(map[string]bool, numTail)
	for i := 0; i < numHeads; i++ {
		hs, hkp, htext, _ := peerServer(t, "head"+string(rune('A'+i)))
		tok := "tok-head-" + string(rune('A'+i))
		heads[i] = peer{kp: hkp, text: htext, tok: tok}
		setupBoundFollow(t, aliceStore, aliceKP, hkp, tok, true)
		working[tok] = pipeTransport{serverStore: hs, serverKey: hkp, logger: logger}
	}
	for i := 0; i < numTail; i++ {
		ts, tkp, ttext, _ := peerServer(t, "tail"+string(rune('A'+i)))
		tok := "tok-tail-" + string(rune('A'+i))
		tails[i] = peer{kp: tkp, text: ttext, tok: tok}
		setupBoundFollow(t, aliceStore, aliceKP, tkp, tok, false)
		block[tok] = true
		_ = ts
	}

	// Persist weights so the lanes read a fresh weighted graph on their
	// first pass instead of waiting for the assess thread's 60s ticker.
	if _, err := aliceStore.RecomputeWeights(); err != nil {
		t.Fatalf("recompute weights: %v", err)
	}

	d := New(aliceStore, logger)
	d.SetTransport(laneTransport{working: working, block: block})
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()
	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "alicepass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	// The head posts must land within the head dial timeout (fast-skip),
	// not the tail dial timeout. A shared single pool saturated by the
	// blocking tail would defer the head dials to the tail timeout (~10s),
	// exceeding this bound.
	deadline := time.Now().Add(headDialTimeout + 2*time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, h := range heads {
			if !storeHasPost(aliceStore, h.kp.Identity(), h.text) {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, h := range heads {
		if !storeHasPost(aliceStore, h.kp.Identity(), h.text) {
			t.Fatalf("head post %q not synced within head timeout (head starved by tail?)", h.text)
		}
	}
}

// TestTailLaneIndependence proves the tail lane dials the tail set
// separately from the head lane: a tail follow with a working server syncs
// in the background even though no head pass touched it.
func TestTailLaneIndependence(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	aliceStore := openTestStore(t)
	aliceKP := initTestIdentityAt(t, aliceStore, "alicepass")

	// One head follow (pinned, working) so the head lane has something to
	// do; one tail follow (cold-start, working) that only the tail lane
	// reaches. Two bound follows floor to K=1, so the head lane dials
	// only the pinned follow and the tail lane dials the other.
	headS, headKP, _, _ := peerServer(t, "head")
	tailS, tailKP, tailText, _ := peerServer(t, "tail")
	setupBoundFollow(t, aliceStore, aliceKP, headKP, "tok-head", true)
	setupBoundFollow(t, aliceStore, aliceKP, tailKP, "tok-tail", false)

	if _, err := aliceStore.RecomputeWeights(); err != nil {
		t.Fatalf("recompute weights: %v", err)
	}

	d := New(aliceStore, logger)
	d.SetTransport(multiPipeTransport{
		servers: map[string]pipeTransport{
			"tok-head": {serverStore: headS, serverKey: headKP, logger: logger},
			"tok-tail": {serverStore: tailS, serverKey: tailKP, logger: logger},
		},
	})
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()
	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "alicepass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	// The tail follow is not in the head set, so no head pass touches it.
	// The tail lane must sync it in the background.
	deadline := time.Now().Add(tailDialTimeout + laneBackoff + 3*time.Second)
	for time.Now().Before(deadline) {
		if storeHasPost(aliceStore, tailKP.Identity(), tailText) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !storeHasPost(aliceStore, tailKP.Identity(), tailText) {
		t.Fatalf("tail post %q not synced by the tail lane", tailText)
	}
}

// TestPromoteDemote proves promote/demote is driven solely by the assess
// thread's recomputation. A follow starts in the tail set (cold-start
// weight); after Alice likes one of its posts (lifting its weight above
// cold-start so it outranks a current head follow), RecomputeWeights and a
// head/tail split at K move it into the head set and demote the displaced
// follow to the tail. This validates that promote/demote is driven by the
// recomputation, not by the lanes.
func TestPromoteDemote(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_ = logger
	aliceStore := openTestStore(t)
	aliceKP := initTestIdentityAt(t, aliceStore, "alicepass")

	// Two bound follows. Pin "bob" so it starts in the head; "carol" is
	// cold-start, so it starts in the tail.
	bobS, bobKP, _, _ := peerServer(t, "bob")
	carolS, carolKP, carolText, carolPostID := peerServer(t, "carol")
	setupBoundFollow(t, aliceStore, aliceKP, bobKP, "tok-bob", true)
	setupBoundFollow(t, aliceStore, aliceKP, carolKP, "tok-carol", false)
	_ = bobS
	_ = carolS

	// Seed Alice's crawl bucket with carol's post so a Like targeting it
	// resolves to carol as the author during weight recomputation. The
	// authors index in gatherWeightInputs is built over own posts plus
	// crawled followed authors' posts.
	carolPost, err := carolS.OwnEvents(core.PostLog)
	if err != nil {
		t.Fatalf("carol own events: %v", err)
	}
	for _, se := range carolPost {
		if se.Event.Kind == core.KindPost {
			if _, err := aliceStore.PutCrawledEvent(&se, uint64(se.Event.Sequence)); err != nil {
				t.Fatalf("seed carol post: %v", err)
			}
		}
	}

	// Initial split: bob (pinned) is head, carol (cold-start) is tail.
	// Two bound follows floor to K=1, so the single head slot goes to the
	// higher-weight follow.
	d := New(aliceStore, logger)
	// assess derives weights and splits in one pass; read the derived
	// weights back for the ordering assertion, without a second recompute.
	d.assess(context.Background())
	weights, err := aliceStore.WeightedGraph()
	if err != nil {
		t.Fatalf("weighted graph: %v", err)
	}
	if weights[carolKP.Identity()] >= weights[bobKP.Identity()] {
		t.Fatalf("carol should start below bob: carol=%g bob=%g", weights[carolKP.Identity()], weights[bobKP.Identity()])
	}
	// Assert the before state via the scheduler's own split: bob is in
	// the head, carol is in the tail. This anchors the promotion on the
	// actual ranking, so a split that ignores weights (and orders by
	// identity) is caught when the after-state assertion contradicts it.
	headBefore, tailBefore := d.snapshotSplit()
	if !containsID(headBefore, bobKP.Identity()) {
		t.Fatalf("bob should start in head: head=%v tail=%v", headBefore, tailBefore)
	}
	if !containsID(tailBefore, carolKP.Identity()) {
		t.Fatalf("carol should start in tail: head=%v tail=%v", headBefore, tailBefore)
	}

	// Promote carol: unpin bob so both are on interaction signals, then
	// Alice likes one of carol's posts. A signed Like in Alice's own
	// PostLog targeting carol's post lifts carol's outbound-like count
	// above cold-start, outranking the now-cold-start bob.
	if err := aliceStore.UnpinIdentity(bobKP.Identity()); err != nil {
		t.Fatalf("unpin bob: %v", err)
	}
	like, err := aliceKP.Sign(core.Event{
		Kind:      core.KindLike,
		Log:       core.PostLog,
		Timestamp: core.Now64(),
		Sequence:  1,
		Like:      &core.Like{TargetID: carolPostID},
	})
	if err != nil {
		t.Fatalf("sign like: %v", err)
	}
	if err := aliceStore.AppendOwnEvent(core.PostLog, like); err != nil {
		t.Fatalf("append like: %v", err)
	}

	// Recompute and re-split. Carol (liked) is now above bob (cold-start),
	// so carol enters the head and bob is demoted to the tail.
	d.assess(context.Background())
	head, tail := d.snapshotSplit()
	if !containsID(head, carolKP.Identity()) {
		t.Fatalf("carol should be promoted to head: head=%v tail=%v", head, tail)
	}
	if !containsID(tail, bobKP.Identity()) {
		t.Fatalf("bob should be demoted to tail: head=%v tail=%v", head, tail)
	}
	_ = carolText
}

// containsID reports whether the identity slice contains id.
func containsID(ids []core.Identity, id core.Identity) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

// TestSplitHeadTailKPolicy asserts splitHeadTail over the K policy's edge
// cases: empty, the floor (bound 1-2 collapses K to 1), a mid-size third,
// the cap (bound/3 at maxHeadSize), and above the cap. For each case it
// checks the head/tail sizes and that the head holds the top-K by weight
// while the tail holds the rest (the ranking property a count-only check
// would miss). Weights are non-negative in production (the weight formula
// sums non-negative terms), so this stays within that contract.
func TestSplitHeadTailKPolicy(t *testing.T) {
	// buildFollows makes n bound follows with strictly descending weights:
	// ids[0] heaviest, ids[n-1] lightest. All are bound (routing entry set).
	buildFollows := func(n int) (ids []core.Identity, weights map[core.Identity]float64, routing map[core.Identity]string) {
		ids = make([]core.Identity, n)
		weights = make(map[core.Identity]float64, n)
		routing = make(map[core.Identity]string, n)
		for i := 0; i < n; i++ {
			id := core.Identity(fmt.Sprintf("driftnode:%04d", i))
			ids[i] = id
			weights[id] = float64(n - i)
			routing[id] = "tok"
		}
		return
	}

	cases := []struct {
		name     string
		bound    int
		wantHead int
		wantTail int
	}{
		{"empty", 0, 0, 0},
		{"floor_one", 1, 1, 0},
		{"floor_two", 2, 1, 1},
		{"third_small", 3, 1, 2},
		{"third_mid", 30, 10, 20},
		{"at_cap", maxHeadSize * 3, maxHeadSize, maxHeadSize * 2},
		{"above_cap", maxHeadSize * 4, maxHeadSize, maxHeadSize * 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ids, weights, routing := buildFollows(c.bound)
			head, tail := splitHeadTail(ids, weights, routing)
			if len(head) != c.wantHead || len(tail) != c.wantTail {
				t.Fatalf("head/tail = %d/%d, want %d/%d", len(head), len(tail), c.wantHead, c.wantTail)
			}
			if len(head)+len(tail) != c.bound {
				t.Fatalf("head+tail = %d, want %d (not a partition)", len(head)+len(tail), c.bound)
			}
			if c.bound == 0 {
				return
			}
			// Head holds the top-K by weight. Descending weights mean
			// ids[:k] is the expected head set.
			wantHeadSet := make(map[core.Identity]bool, len(head))
			for _, id := range ids[:c.wantHead] {
				wantHeadSet[id] = true
			}
			for _, id := range head {
				if !wantHeadSet[id] {
					t.Errorf("head contains %v, want only the top-%d by weight", id, c.wantHead)
				}
			}
		})
	}

	// Ties: cold-start follows share a weight and are ordered by the
	// identity-string tiebreak. A boundary that cuts through a tied group
	// splits it deterministically by identity, not arbitrarily.
	t.Run("ties", func(t *testing.T) {
		const n = 6
		ids := make([]core.Identity, n)
		weights := make(map[core.Identity]float64, n)
		routing := make(map[core.Identity]string, n)
		for i := 0; i < n; i++ {
			id := core.Identity(fmt.Sprintf("driftnode:%04d", i))
			ids[i] = id
			weights[id] = 1.0 // all tied
			routing[id] = "tok"
		}
		head, tail := splitHeadTail(ids, weights, routing)
		if len(head) != 2 || len(tail) != 4 {
			t.Fatalf("ties: head/tail = %d/%d, want 2/4", len(head), len(tail))
		}
		// Identity-string tiebreak: the lexicographically smallest 2 ids
		// are the head.
		sortedIDs := append([]core.Identity(nil), ids...)
		sort.Slice(sortedIDs, func(i, j int) bool { return sortedIDs[i] < sortedIDs[j] })
		for _, id := range head {
			if id != sortedIDs[0] && id != sortedIDs[1] {
				t.Errorf("ties: head contains %v, want the two smallest by identity tiebreak", id)
			}
		}
	})
}
