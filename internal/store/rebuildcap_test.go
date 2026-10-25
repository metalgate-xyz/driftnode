package store

import (
	"testing"

	"driftnode/internal/core"
)

// The from-zero rebuild enforces the follower cap the same way the
// incremental paths do: unique followers are admitted in replay
// (timestamp + sequence) order until the cap, and later ones are dropped
// without flagging.
func TestRebuildEnforcesFollowerCap(t *testing.T) {
	s, mePub := initOwnStore(t)

	prevCap := followerCap
	followerCap = 2
	defer func() { followerCap = prevCap }()

	// Three unique followers, ordered by timestamp: alice (5e9), bob
	// (6e9), carol (7e9). A rebuild must admit alice and bob and drop
	// carol without flagging her.
	followers := []*core.KeyPair{nil, nil, nil}
	names := []string{"alice", "bob", "carol"}
	for i := range followers {
		followers[i], _ = core.NewKeyPair()
		putOwnTargetedFollow(t, s, followers[i], 1, int64(5e9+i*1e9), mePub)
	}

	ids, err := s.ReceivedFollowers()
	if err != nil {
		t.Fatalf("ReceivedFollowers: %v", err)
	}
	got := make(map[core.Identity]bool, len(ids))
	for _, id := range ids {
		got[id] = true
	}
	if len(ids) != 2 {
		t.Fatalf("want %d admitted followers, got %d (%v)", followerCap, len(ids), ids)
	}
	for i, kp := range followers[:2] {
		if !got[kp.Identity()] {
			t.Fatalf("%s missing from the admitted set: %v", names[i], ids)
		}
	}
	if got[followers[2].Identity()] {
		t.Fatal("over-cap follower admitted by the rebuild")
	}
	for i, kp := range followers {
		flagged, err := s.IsFlagged(kp.Identity())
		if err != nil {
			t.Fatalf("IsFlagged %s: %v", names[i], err)
		}
		if flagged {
			t.Fatalf("%s was flagged by cap enforcement", names[i])
		}
	}
}
