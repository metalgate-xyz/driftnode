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
}

func (f *fakeFetcher) FetchProfileLog(_ context.Context, id core.Identity) ([]core.SignedEvent, error) {
	if events, ok := f.profiles[id]; ok {
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
