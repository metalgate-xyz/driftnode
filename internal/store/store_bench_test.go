package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"driftnode/internal/core"
)

// driftnodeBenchPeers and driftnodeBenchPosts control the fixture size for
// the store benchmarks. Defaults are small so iterating on the harness does
// not OOM a dev machine; override with DRIFTNODE_BENCH_PEERS and
// DRIFTNODE_BENCH_POSTS for a real scale run.
var (
	driftnodeBenchPeers = envInt("DRIFTNODE_BENCH_PEERS", 200)
	driftnodeBenchPosts = envInt("DRIFTNODE_BENCH_POSTS", 1000)
)

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// benchState caches one fixture across all benchmarks in the run so the
// expensive build (keypair gen, follow signing, bbolt writes) happens once,
// not per benchmark. Built once per process: different scales require
// separate go test invocations with their own DRIFTNODE_BENCH_* values.
var (
	benchOnce    sync.Once
	benchStore   *Store
	benchAliceKP *core.KeyPair
	benchPeers   []*core.KeyPair
	benchPosts   int
)

func benchSetup(b *testing.B) {
	b.Helper()
	benchOnce.Do(func() {
		dir, err := os.MkdirTemp("", "driftnode-bench")
		if err != nil {
			b.Fatalf("bench temp dir: %v", err)
		}
		s, err := Open(filepath.Join(dir, "bench.db"))
		if err != nil {
			b.Fatalf("open store: %v", err)
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
		if err := s.InitIdentity(aliceKP, ek); err != nil {
			b.Fatalf("init identity: %v", err)
		}
		n := driftnodeBenchPeers
		peers := make([]*core.KeyPair, n)
		for i := 0; i < n; i++ {
			peerKP, err := core.NewKeyPair()
			if err != nil {
				b.Fatalf("peer keypair %d: %v", i, err)
			}
			peers[i] = peerKP
			peerPub, err := peerKP.Identity().PubkeyBytes()
			if err != nil {
				b.Fatalf("peer pubkey %d: %v", i, err)
			}
			var pub [32]byte
			copy(pub[:], peerPub)
			if _, _, _, err := s.Follow(aliceKP, pub); err != nil {
				b.Fatalf("follow %d: %v", i, err)
			}
			if err := s.PutRouting(peerKP.Identity(), fmt.Sprintf("tok-%d", i)); err != nil {
				b.Fatalf("routing %d: %v", i, err)
			}
			if i%6 == 0 {
				if err := s.PinIdentity(peerKP.Identity()); err != nil {
					b.Fatalf("pin %d: %v", i, err)
				}
			}
			// One crawled post per peer, signed by the peer so Author
			// matches the followed identity (AllPostsOneTx and the feed
			// merge key on that). Also seeds a like target for the
			// i%6 == 0 peers.
			peerPost, err := peerKP.Sign(core.Event{
				Kind:      core.KindPost,
				Log:       core.PostLog,
				Timestamp: core.Now64(),
				Sequence:  1,
				Post:      &core.Post{Text: fmt.Sprintf("post %d", i)},
			})
			if err != nil {
				b.Fatalf("peer sign %d: %v", i, err)
			}
			if _, err := s.PutCrawledEvent(peerPost, 1); err != nil {
				b.Fatalf("crawled post %d: %v", i, err)
			}
		}
		// Outbound likes: every sixth peer, alice signs a Like targeting
		// that peer's post into her own PostLog. The weight gatherer
		// resolves the like target author against the crawled post.
		for i := 0; i < n; i += 6 {
			peerPostID, err := firstPostID(peers[i])
			if err != nil {
				b.Fatalf("peer post id %d: %v", i, err)
			}
			like, err := aliceKP.Sign(core.Event{
				Kind:      core.KindLike,
				Log:       core.PostLog,
				Timestamp: core.Now64(),
				Sequence:  uint64(i + 1),
				Like:      &core.Like{TargetID: peerPostID},
			})
			if err != nil {
				b.Fatalf("sign like %d: %v", i, err)
			}
			if err := s.AppendOwnEvent(core.PostLog, like); err != nil {
				b.Fatalf("append like %d: %v", i, err)
			}
		}
		if _, err := s.RecomputeWeights(); err != nil {
			b.Fatalf("seed recompute: %v", err)
		}
		benchStore = s
		benchAliceKP = aliceKP
		benchPeers = peers
		benchPosts = n
	})
}

// firstPostID returns the EventID of the peer's first post, used as a like
// target. Peers sign exactly one post (sequence 1) in benchSetup.
func firstPostID(kp *core.KeyPair) (core.EventID, error) {
	se, err := kp.Sign(core.Event{
		Kind:      core.KindPost,
		Log:       core.PostLog,
		Timestamp: core.Now64(),
		Sequence:  1,
		Post:      &core.Post{Text: "x"},
	})
	if err != nil {
		return core.EventID{}, err
	}
	return se.ID()
}

// addBenchPosts inserts n crawled posts spread across the peer keypairs,
// each signed by the owning peer so Author matches the followed identity.
// Used by the AllPostsOneTx benchmark to build a large feed.
func addBenchPosts(b *testing.B, n int) {
	b.Helper()
	if len(benchPeers) == 0 || benchPosts >= n {
		return
	}
	need := n - benchPosts
	for i := 0; i < need; i++ {
		peer := benchPeers[(benchPosts+i)%len(benchPeers)]
		se, err := peer.Sign(core.Event{
			Kind:      core.KindPost,
			Log:       core.PostLog,
			Timestamp: core.Now64(),
			Sequence:  uint64(benchPosts + i + 1),
			Post:      &core.Post{Text: fmt.Sprintf("bench post %d", benchPosts+i)},
		})
		if err != nil {
			b.Fatalf("sign post %d: %v", i, err)
		}
		if _, err := benchStore.PutCrawledEvent(se, uint64(benchPosts+i+1)); err != nil {
			b.Fatalf("put post %d: %v", i, err)
		}
	}
	benchPosts = n
}

func BenchmarkStoreFollowGraph(b *testing.B) {
	benchSetup(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := benchStore.FollowGraph(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreAllRouting(b *testing.B) {
	benchSetup(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := benchStore.AllRouting(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreWeightedGraph(b *testing.B) {
	benchSetup(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := benchStore.WeightedGraph(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreRecomputeWeights(b *testing.B) {
	benchSetup(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := benchStore.RecomputeWeights(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreAllPostsOneTx(b *testing.B) {
	benchSetup(b)
	addBenchPosts(b, driftnodeBenchPosts)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := benchStore.AllPostsOneTx(); err != nil {
			b.Fatal(err)
		}
	}
}
