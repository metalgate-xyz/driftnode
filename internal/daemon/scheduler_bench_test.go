package daemon

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"driftnode/internal/core"
	"driftnode/internal/store"
	syncproto "driftnode/internal/sync"
)

// bench scale knobs. Defaults small for harness iteration; override with
// env vars for a real scale run.
var (
	driftnodeBenchPosts = envInt("DRIFTNODE_BENCH_POSTS", 1000)
	driftnodeBenchPeers = envInt("DRIFTNODE_BENCH_PEERS", 200)
	// driftnodeBenchLatencyP50/P95 shape the heavy-tailed dial latency in
	// the fake transport, in milliseconds. 0/0 = instant (throughput
	// ceiling).
	driftnodeBenchLatencyP50 = envInt("DRIFTNODE_BENCH_LAT_P50", 0)
	driftnodeBenchLatencyP95 = envInt("DRIFTNODE_BENCH_LAT_P95", 0)
	// driftnodeBenchUnreachable is the fraction of bound follows whose
	// routing token has no transport entry, so their dial blocks until the
	// lane's per-dial timeout cancels it (the fast-skip path). 0 = all
	// reachable (throughput ceiling); 1 = all unreachable. Range [0,1].
	driftnodeBenchUnreachable = envFloat("DRIFTNODE_BENCH_UNREACHABLE", 0)
)

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// benchPeer is one simulated zen behind benchTransport.
type benchPeer struct {
	kp      *core.KeyPair
	store   *store.Store
	latency time.Duration
}

// benchTransport simulates N zens behind a single Transport. Each Dial
// sleeps a latency drawn from a heavy-tailed distribution, then returns a
// real in-process sync session over net.Pipe against that peer's store, so
// the real sync protocol and merge run end to end. An unknown token blocks
// until the dial context is cancelled (modelling an unreachable zen), which
// exercises the fast-skip timeout path under contention.
type benchTransport struct {
	mu    sync.Mutex
	peers map[string]benchPeer
	rng   *rand.Rand
	p50   time.Duration
	p95   time.Duration
}

func newBenchTransport(p50, p95 time.Duration) *benchTransport {
	return &benchTransport{
		peers: make(map[string]benchPeer),
		rng:   rand.New(rand.NewSource(1)),
		p50:   p50,
		p95:   p95,
	}
}

func (t *benchTransport) add(token string, kp *core.KeyPair, s *store.Store) {
	lat := time.Duration(0)
	if t.p50 > 0 || t.p95 > 0 {
		// Heavy-tailed: 95% of dials around p50, 5% around p95.
		t.mu.Lock()
		if t.rng.Float64() < 0.95 {
			lat = t.p50
		} else {
			lat = t.p95
		}
		t.mu.Unlock()
	}
	t.mu.Lock()
	t.peers[token] = benchPeer{kp: kp, store: s, latency: lat}
	t.mu.Unlock()
}

func (t *benchTransport) Dial(ctx context.Context, token string) (net.Conn, error) {
	t.mu.Lock()
	p, ok := t.peers[token]
	lat := p.latency
	t.mu.Unlock()
	if !ok {
		// Unreachable: block until the dial context is cancelled so the
		// lane's fast-skip timeout releases the slot.
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if lat > 0 {
		select {
		case <-time.After(lat):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	clientConn, serverConn := net.Pipe()
	go func() {
		defer serverConn.Close()
		sess := syncproto.NewSession(p.store, slog.New(slog.NewTextHandler(io.Discard, nil)))
		sess.SetKey(p.kp)
		_, _ = sess.RunListener(serverConn, serverConn)
	}()
	return clientConn, nil
}

// benchFixture builds a daemon with a populated follow graph of
// driftnodeBenchPeers bound follows, each backed by a peer store behind benchTransport. The
// daemon is started so the lanes and assess thread are live. Returns the
// daemon, transport, and the alice keypair (for signing).
//
// The fixture is cached across benchmarks via sync.Once so the expensive
// build runs once per go test -bench invocation.
var (
	daemonOnce      sync.Once
	daemonD         *Daemon
	daemonTransport *benchTransport
	daemonAliceKP   *core.KeyPair
	daemonPeerKPs   []*core.KeyPair
)

func benchDaemon(b *testing.B) (*Daemon, *benchTransport, *core.KeyPair) {
	b.Helper()
	daemonOnce.Do(func() {
		dir, err := os.MkdirTemp("", "driftnode-daemon-bench")
		if err != nil {
			b.Fatalf("bench temp dir: %v", err)
		}
		aliceStore, err := store.Open(filepath.Join(dir, "alice.db"))
		if err != nil {
			b.Fatalf("open alice store: %v", err)
		}
		aliceKP, err := core.NewKeyPair()
		if err != nil {
			b.Fatalf("alice keypair: %v", err)
		}
		enc := core.DefaultKeyEncryption()
		ek, err := enc.Encrypt(aliceKP.Private, []byte("benchpass"))
		if err != nil {
			b.Fatalf("encrypt: %v", err)
		}
		if err := aliceStore.InitIdentity(aliceKP, ek); err != nil {
			b.Fatalf("init identity: %v", err)
		}
		tr := newBenchTransport(
			time.Duration(driftnodeBenchLatencyP50)*time.Millisecond,
			time.Duration(driftnodeBenchLatencyP95)*time.Millisecond,
		)
		n := driftnodeBenchPeers
		// unreachable is the set of peer indices whose routing token has no
		// transport entry, so their dial blocks until the lane's per-dial
		// timeout cancels it (the fast-skip path). The set is drawn uniformly
		// from the bound follows with a fixed seed, independent of weight, so
		// unreachable peers land in the head and the tail in the same
		// proportion and the bench exercises fast-skip in both lanes.
		unreachable := make(map[int]bool, n)
		if f := driftnodeBenchUnreachable; f > 0 {
			uc := int(float64(n) * f)
			if uc > n {
				uc = n
			}
			rng := rand.New(rand.NewSource(1))
			for len(unreachable) < uc {
				unreachable[rng.Intn(n)] = true
			}
		}
		peerKPs := make([]*core.KeyPair, n)
		for i := 0; i < n; i++ {
			peerKP, err := core.NewKeyPair()
			if err != nil {
				b.Fatalf("peer keypair %d: %v", i, err)
			}
			peerKPs[i] = peerKP
			peerStore, err := store.Open(filepath.Join(dir, fmt.Sprintf("peer-%d.db", i)))
			if err != nil {
				b.Fatalf("open peer store %d: %v", i, err)
			}
			peerEnc := core.DefaultKeyEncryption()
			peerEK, err := peerEnc.Encrypt(peerKP.Private, []byte("peerpass"))
			if err != nil {
				b.Fatalf("peer encrypt %d: %v", i, err)
			}
			if err := peerStore.InitIdentity(peerKP, peerEK); err != nil {
				b.Fatalf("peer init %d: %v", i, err)
			}
			// One post per peer so a sync round has something to merge.
			peerPost, err := peerKP.Sign(core.Event{
				Kind:      core.KindPost,
				Log:       core.PostLog,
				Timestamp: core.Now64(),
				Sequence:  1,
				Post:      &core.Post{Text: fmt.Sprintf("peer %d post", i)},
			})
			if err != nil {
				b.Fatalf("peer sign %d: %v", i, err)
			}
			if err := peerStore.AppendOwnEvent(core.PostLog, peerPost); err != nil {
				b.Fatalf("peer append %d: %v", i, err)
			}
			var tok string
			if unreachable[i] {
				// Bound follow with no transport entry: the dial blocks
				// until the lane's per-dial timeout cancels it.
				tok = fmt.Sprintf("tc-bench-unreach-%d", i)
			} else {
				tok = fmt.Sprintf("tc-bench-%d", i)
				tr.add(tok, peerKP, peerStore)
			}
			peerPub, err := peerKP.Identity().PubkeyBytes()
			if err != nil {
				b.Fatalf("peer pubkey %d: %v", i, err)
			}
			var pub [32]byte
			copy(pub[:], peerPub)
			if _, _, _, err := aliceStore.Follow(aliceKP, pub); err != nil {
				b.Fatalf("follow %d: %v", i, err)
			}
			if err := aliceStore.PutRouting(peerKP.Identity(), tok); err != nil {
				b.Fatalf("routing %d: %v", i, err)
			}
		}
		if _, err := aliceStore.RecomputeWeights(); err != nil {
			b.Fatalf("seed recompute: %v", err)
		}
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		d := New(aliceStore, logger)
		d.SetTransport(tr)
		sock := filepath.Join(dir, "ctrl.sock")
		if err := d.Start(sock); err != nil {
			b.Fatalf("Start: %v", err)
		}
		// Unlock so sessions can authenticate.
		d.mu.Lock()
		d.unlocked = aliceKP
		d.lastSignAt = time.Now()
		d.mu.Unlock()
		daemonD = d
		daemonTransport = tr
		daemonAliceKP = aliceKP
		daemonPeerKPs = peerKPs
	})
	return daemonD, daemonTransport, daemonAliceKP
}

// Local-data benchmarks: large store, no peer dialing.

// benchPopulatePosts adds n crawled posts spread across the followed
// identities (round-robin over the daemon's peers), each signed by the
// owning peer so Author matches. Used by the feed benchmarks to exercise
// the full load+sort path at scale.
func benchPopulatePosts(b *testing.B, d *Daemon, n int) {
	b.Helper()
	peers := daemonPeerKPs
	if len(peers) == 0 {
		return
	}
	for i := 0; i < n; i++ {
		peer := peers[i%len(peers)]
		se, err := peer.Sign(core.Event{
			Kind:      core.KindPost,
			Log:       core.PostLog,
			Timestamp: core.Now64(),
			Sequence:  uint64(i + 1),
			Post:      &core.Post{Text: fmt.Sprintf("bench post %d", i)},
		})
		if err != nil {
			b.Fatalf("sign post %d: %v", i, err)
		}
		if _, err := d.store.PutCrawledEvent(se, uint64(i+1)); err != nil {
			b.Fatalf("put post %d: %v", i, err)
		}
	}
	d.invalidateFeed()
}

func BenchmarkRecomputeWeights(b *testing.B) {
	d, _, _ := benchDaemon(b)
	ids, err := d.store.FollowGraph()
	if err != nil {
		b.Fatalf("follow graph: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.store.RecomputeWeights(); err != nil {
			b.Fatal(err)
		}
	}
	_ = ids
}

func BenchmarkSplitHeadTail(b *testing.B) {
	d, _, _ := benchDaemon(b)
	ids, err := d.store.FollowGraph()
	if err != nil {
		b.Fatalf("follow graph: %v", err)
	}
	weights, err := d.store.WeightedGraph()
	if err != nil {
		b.Fatalf("weighted graph: %v", err)
	}
	routing, err := d.store.AllRouting()
	if err != nil {
		b.Fatalf("routing: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = splitHeadTail(ids, weights, routing)
	}
}

func BenchmarkSeedSplit(b *testing.B) {
	d, _, _ := benchDaemon(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.seedSplit()
	}
}

func BenchmarkDeriveSplit(b *testing.B) {
	d, _, _ := benchDaemon(b)
	ids, err := d.store.FollowGraph()
	if err != nil {
		b.Fatalf("follow graph: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.deriveSplit(ids)
	}
}

func BenchmarkHandleFeedPage(b *testing.B) {
	d, _, _ := benchDaemon(b)
	benchPopulatePosts(b, d, driftnodeBenchPosts)
	// Invalidate so the first call is a cache miss.
	d.invalidateFeed()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := d.handleFeedPage(0, false, false); err != nil {
			b.Fatal(err)
		}
		// Each subsequent call hits the cache.
	}
}

func BenchmarkHandleFeedPageCached(b *testing.B) {
	d, _, _ := benchDaemon(b)
	benchPopulatePosts(b, d, driftnodeBenchPosts)
	// Warm the cache once before timing.
	if _, _, err := d.handleFeedPage(0, false, false); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := d.handleFeedPage(0, false, false); err != nil {
			b.Fatal(err)
		}
	}
}

// Peer-scale benchmarks: the fake transport stands in for N zens with a
// heavy-tailed latency distribution. These validate the scheduler's
// throughput under contention at scale.

func BenchmarkHeadLanePass(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping peer-scale bench in -short mode")
	}
	d, _, _ := benchDaemon(b)
	head, _ := d.snapshotSplit()
	if len(head) == 0 {
		b.Skipf("head is empty at driftnodeBenchPeers=%d; raise DRIFTNODE_BENCH_PEERS", driftnodeBenchPeers)
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		head, _ := d.snapshotSplit()
		if n := d.dialSet(ctx, head, headDialTimeout, headFanout); n == 0 {
			b.Fatal("head lane dialed nothing")
		}
	}
}

func BenchmarkTailLanePass(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping peer-scale bench in -short mode")
	}
	d, _, _ := benchDaemon(b)
	_, tail := d.snapshotSplit()
	if len(tail) == 0 {
		b.Skipf("tail is empty at driftnodeBenchPeers=%d; raise DRIFTNODE_BENCH_PEERS", driftnodeBenchPeers)
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, tail := d.snapshotSplit()
		if n := d.dialSet(ctx, tail, tailDialTimeout, tailFanout); n == 0 {
			b.Fatal("tail lane dialed nothing")
		}
	}
}

func BenchmarkAssessPass(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping peer-scale bench in -short mode")
	}
	d, _, _ := benchDaemon(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.assess(ctx)
	}
}
