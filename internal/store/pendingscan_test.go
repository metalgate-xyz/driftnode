package store

import (
	"testing"

	"driftnode/internal/core"
)

// A pending rebuild defers map maintenance at ingest: the events are stored
// (set union) and the first follower read rebuilds from zero over them, so
// an ingested Follow while the flag is set is never lost. The rebuild must
// also observe the swallowed events, not just the pre-open state.
func TestPendingRebuildDefersIngestMaintenance(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	var mePub [32]byte
	copy(mePub[:], me.Public)

	// The flag is set at open; ingest a Follow targeting me without
	// priming any read.
	follower, _ := core.NewKeyPair()
	putOwnTargetedFollow(t, s, follower, 1, 5e9, mePub)
	if !s.followersNeedsRebuild.Load() {
		t.Fatal("ingest cleared the pending-rebuild flag; ingest must not rebuild")
	}

	ids, err := s.ReceivedFollowers()
	if err != nil {
		t.Fatalf("ReceivedFollowers: %v", err)
	}
	if len(ids) != 1 || ids[0] != follower.Identity() {
		t.Fatalf("rebuild did not observe the event ingested while pending: %v", ids)
	}
	if s.followersNeedsRebuild.Load() {
		t.Fatal("read did not clear the pending-rebuild flag")
	}
}
