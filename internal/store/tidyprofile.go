package store

import (
	"fmt"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// tidyProfileLogTx removes superseded ProfileLog events from the own log
// before the new event is appended, so the stored log holds only the signed
// events that contribute to the current state.
//
// A Follow for a target removes every prior Follow and Unfollow for that
// target: the new Follow is the sole, current edge. An Unfollow for a
// followed target removes the matching Follow and any prior Unfollow, so
// the log holds only the new Unfollow as the convergence tombstone. A
// Profile removes the previous Profile.
//
// The tidy log holds one event per target: a Follow if currently followed,
// an Unfollow if previously-followed-but-now-unfollowed, and one Profile.
// A follow/unfollow cycle leaves exactly one event (the latest), never one
// per cycle. The lookup is by target pubkey (Follow, Unfollow) or by kind
// (Profile), bounded by the follow cap: at most ~5001 events, so the
// removal is O(n) in the follow set, not in the history.
func tidyProfileLogTx(tx *bolt.Tx, author core.Identity, ev core.Event) error {
	lb := tx.Bucket(bucketOwn).Bucket([]byte(core.ProfileLog))
	if lb == nil {
		return nil
	}

	var keysToDelete [][]byte

	switch ev.Kind {
	case core.KindFollow, core.KindUnfollow:
		if ev.Follow == nil {
			return nil
		}
		target := ev.Follow.TargetPubkey
		err := lb.ForEach(func(k, v []byte) error {
			var se core.SignedEvent
			if err := core.CanonicalDecode(v, &se); err != nil {
				return nil
			}
			if (se.Event.Kind == core.KindFollow || se.Event.Kind == core.KindUnfollow) &&
				se.Event.Follow != nil && se.Event.Follow.TargetPubkey == target {
				keysToDelete = append(keysToDelete, append([]byte(nil), k...))
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("tidy scan: %w", err)
		}

	case core.KindProfile:
		err := lb.ForEach(func(k, v []byte) error {
			var se core.SignedEvent
			if err := core.CanonicalDecode(v, &se); err != nil {
				return nil
			}
			if se.Event.Kind == core.KindProfile {
				keysToDelete = append(keysToDelete, append([]byte(nil), k...))
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("tidy scan: %w", err)
		}
	}

	for _, k := range keysToDelete {
		if err := lb.Delete(k); err != nil {
			return fmt.Errorf("delete superseded: %w", err)
		}
	}
	return nil
}
