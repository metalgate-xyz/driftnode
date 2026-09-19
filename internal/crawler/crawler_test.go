package crawler

import (
	"context"
	"path/filepath"
	"testing"

	"driftnode/internal/core"
	"driftnode/internal/store"
)

type fakeFetcher struct {
	profiles map[core.Identity][]core.SignedEvent
	// followers maps a followed identity to the signed Follow events
	// targeting it that its zen has received, i.e. its follower set.
	followers map[core.Identity][]core.SignedEvent
}

func (f *fakeFetcher) FetchProfileLog(_ context.Context, id core.Identity) ([]core.SignedEvent, error) {
	if events, ok := f.profiles[id]; ok {
		return events, nil
	}
	return nil, nil
}

func (f *fakeFetcher) FetchFollowers(_ context.Context, id core.Identity) ([]core.SignedEvent, error) {
	if events, ok := f.followers[id]; ok {
		return events, nil
	}
	return nil, nil
}

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func makeFollowEvent(kp *core.KeyPair, seq uint64, target core.Identity) core.SignedEvent {
	pub, _ := target.PubkeyBytes()
	se, _ := kp.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: int64(seq),
		Sequence:  seq,
		Follow:    &core.Follow{TargetPubkey: [32]byte(pub)},
	})
	return *se
}

func TestCrawlBasic(t *testing.T) {
	s := newTestStore(t)
	alice, _ := core.NewKeyPair()
	bob, _ := core.NewKeyPair()
	carol, _ := core.NewKeyPair()

	// Alice follows Bob, Bob follows Carol.
	fetcher := &fakeFetcher{profiles: map[core.Identity][]core.SignedEvent{
		alice.Identity(): {makeFollowEvent(alice, 1, bob.Identity())},
		bob.Identity():   {makeFollowEvent(bob, 1, carol.Identity())},
		carol.Identity(): {},
	}}

	c := New(s, nil)
	c.SetMaxDepth(3)
	c.Seed(alice.Identity())

	fetched, err := c.Run(context.Background(), fetcher)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if fetched != 3 {
		t.Fatalf("fetched: want 3, got %d", fetched)
	}
	if c.VisitedCount() != 3 {
		t.Fatalf("visited: want 3, got %d", c.VisitedCount())
	}
}

func TestCrawlDepthLimited(t *testing.T) {
	s := newTestStore(t)
	alice, _ := core.NewKeyPair()
	bob, _ := core.NewKeyPair()
	carol, _ := core.NewKeyPair()

	fetcher := &fakeFetcher{profiles: map[core.Identity][]core.SignedEvent{
		alice.Identity(): {makeFollowEvent(alice, 1, bob.Identity())},
		bob.Identity():   {makeFollowEvent(bob, 1, carol.Identity())},
		carol.Identity(): {},
	}}

	c := New(s, nil)
	c.SetMaxDepth(1) // only alice
	c.Seed(alice.Identity())

	fetched, _ := c.Run(context.Background(), fetcher)
	if fetched != 1 {
		t.Fatalf("fetched: want 1 (depth-limited), got %d", fetched)
	}
}

func TestCrawlDedup(t *testing.T) {
	s := newTestStore(t)
	alice, _ := core.NewKeyPair()
	bob, _ := core.NewKeyPair()

	fetcher := &fakeFetcher{profiles: map[core.Identity][]core.SignedEvent{
		alice.Identity(): {
			makeFollowEvent(alice, 1, bob.Identity()),
			makeFollowEvent(alice, 2, bob.Identity()), // duplicate follow
		},
		bob.Identity(): {},
	}}

	c := New(s, nil)
	c.SetMaxDepth(3)
	c.Seed(alice.Identity())

	fetched, _ := c.Run(context.Background(), fetcher)
	if fetched != 2 {
		t.Fatalf("fetched: want 2, got %d", fetched)
	}
}

func TestCrawlStoresProfiles(t *testing.T) {
	s := newTestStore(t)
	alice, _ := core.NewKeyPair()
	bob, _ := core.NewKeyPair()

	fetcher := &fakeFetcher{profiles: map[core.Identity][]core.SignedEvent{
		alice.Identity(): {makeFollowEvent(alice, 1, bob.Identity())},
		bob.Identity():   {},
	}}

	c := New(s, nil)
	c.SetMaxDepth(3)
	c.Seed(alice.Identity())
	c.Run(context.Background(), fetcher)

	profiles, err := s.CrawledProfiles(alice.Identity())
	if err != nil {
		t.Fatalf("CrawledProfiles: %v", err)
	}
	if len(profiles) != 1 {
		t.Fatalf("want 1 crawled profile event, got %d", len(profiles))
	}
}

// TestCrawlFollowersDirection verifies the crawl walks in-edges: starting
// from bob, who is followed by alice, the crawl discovers alice by asking
// bob's zen for its followers. A follows-only crawl would never reach alice
// from bob, since bob follows nobody.
func TestCrawlFollowersDirection(t *testing.T) {
	s := newTestStore(t)
	alice, _ := core.NewKeyPair()
	bob, _ := core.NewKeyPair()

	// Alice follows Bob. Bob follows nobody.
	aliceFollow := makeFollowEvent(alice, 1, bob.Identity())

	fetcher := &fakeFetcher{
		profiles: map[core.Identity][]core.SignedEvent{
			alice.Identity(): {aliceFollow},
			bob.Identity():   {},
		},
		// Bob's zen reports alice as its follower: it has received
		// alice's Follow{target: bob} event.
		followers: map[core.Identity][]core.SignedEvent{
			bob.Identity(): {aliceFollow},
		},
	}

	c := New(s, nil)
	c.SetMaxDepth(2)
	c.Seed(bob.Identity())

	fetched, err := c.Run(context.Background(), fetcher)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Bob is visited first. FetchFollowers(bob) returns alice's Follow
	// event, so alice is enqueued and visited at depth 1.
	if fetched != 2 {
		t.Fatalf("fetched: want 2 (bob + alice via follower edge), got %d", fetched)
	}
	if c.VisitedCount() != 2 {
		t.Fatalf("visited: want 2, got %d", c.VisitedCount())
	}
	if !c.visited[alice.Identity()] {
		t.Fatalf("alice not visited: follower direction not walked")
	}
}

// TestCrawlBothDirectionsDedup verifies that an identity reachable via both
// a follow edge and a follower edge is visited only once.
func TestCrawlBothDirectionsDedup(t *testing.T) {
	s := newTestStore(t)
	alice, _ := core.NewKeyPair()
	bob, _ := core.NewKeyPair()

	// Alice follows Bob and Bob follows Alice (mutual). Seeding either one
	// must visit both exactly once despite two paths between them.
	aliceFollowsBob := makeFollowEvent(alice, 1, bob.Identity())
	bobFollowsAlice := makeFollowEvent(bob, 1, alice.Identity())
	fetcher := &fakeFetcher{
		profiles: map[core.Identity][]core.SignedEvent{
			alice.Identity(): {aliceFollowsBob},
			bob.Identity():   {bobFollowsAlice},
		},
		followers: map[core.Identity][]core.SignedEvent{
			alice.Identity(): {bobFollowsAlice},
			bob.Identity():   {aliceFollowsBob},
		},
	}

	c := New(s, nil)
	c.SetMaxDepth(3)
	c.Seed(alice.Identity())

	fetched, _ := c.Run(context.Background(), fetcher)
	if fetched != 2 {
		t.Fatalf("fetched: want 2 (mutual follow dedup), got %d", fetched)
	}
}
