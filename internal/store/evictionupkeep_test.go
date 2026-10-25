package store

import (
	"testing"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// initOwnStore returns a test store with the local identity initialized.
func initOwnStore(t *testing.T) (*Store, [32]byte) {
	t.Helper()
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	var mePub [32]byte
	copy(mePub[:], me.Public)
	return s, mePub
}

// With the follower map current, a crawl eviction removes the evicted
// follower's record and decrements the count on the spot: the flag stays
// clear, so no from-zero rebuild is ever forced by eviction.
func TestEvictionUpkeepsMapIncrementally(t *testing.T) {
	s, mePub := initOwnStore(t)

	// Prime the map so maintenance is incremental.
	if _, err := s.ReceivedFollowers(); err != nil {
		t.Fatalf("prime read: %v", err)
	}

	alice, _ := core.NewKeyPair()
	putOwnTargetedFollow(t, s, alice, 1, 5e9, mePub)

	if err := s.TouchCrawl(alice.Identity(), 1); err != nil {
		t.Fatalf("touch alice: %v", err)
	}
	bob, _ := core.NewKeyPair()
	if _, err := s.PutRemoteEvent(signRemoteFollowFor(t, bob, 1, 5e9+5, [32]byte(unknownPub(t))), 1); err != nil {
		t.Fatalf("seed bob: %v", err)
	}
	if err := s.TouchCrawl(bob.Identity(), 2); err != nil {
		t.Fatalf("touch bob: %v", err)
	}
	if err := s.EvictCrawl(1); err != nil {
		t.Fatalf("evict: %v", err)
	}
	if s.followersNeedsRebuild.Load() {
		t.Fatal("eviction re-armed the pending-rebuild flag")
	}

	// The evicted follower is gone from the reads and the count is right.
	ids, err := s.ReceivedFollowers()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("evicted follower still claimed: %v", ids)
	}
	var count uint64
	if err := s.db.View(func(tx *bolt.Tx) error {
		count = followerCountTx(tx)
		return nil
	}); err != nil {
		t.Fatalf("count read: %v", err)
	}
	if count != 0 {
		t.Fatalf("count after eviction: want 0, got %d", count)
	}
}

// With the flag set (before the first follower read), eviction is a no-op
// on the stale map and the later rebuild derives the correct state from
// the surviving events.
func TestEvictionWhilePendingRebuildDefersToRebuild(t *testing.T) {
	s, mePub := initOwnStore(t)

	alice, _ := core.NewKeyPair()
	putOwnTargetedFollow(t, s, alice, 1, 5e9, mePub)
	bob, _ := core.NewKeyPair()
	putOwnTargetedFollow(t, s, bob, 1, 5e9+1, mePub)

	if err := s.TouchCrawl(alice.Identity(), 1); err != nil {
		t.Fatalf("touch alice: %v", err)
	}
	if err := s.TouchCrawl(bob.Identity(), 2); err != nil {
		t.Fatalf("touch bob: %v", err)
	}
	if err := s.EvictCrawl(1); err != nil {
		t.Fatalf("evict: %v", err)
	}

	ids, err := s.ReceivedFollowers()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(ids) != 1 || ids[0] != bob.Identity() {
		t.Fatalf("post-eviction rebuild: want only bob, got %v", ids)
	}
}
