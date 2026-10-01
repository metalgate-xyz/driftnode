package store

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

var errStop = errors.New("stop iteration")

// existingLikeID returns the event ID of a KindLike event by author targeting
// postID already present in bucket, if any. One Like per author per post: a
// re-like is silently dropped so the count cannot inflate and a peer cannot
// flood a log with redundant like events.
func existingLikeID(author core.Identity, target core.EventID, bucket bucketLike) (core.EventID, bool) {
	var id core.EventID
	var found bool
	_ = bucket.ForEach(func(k, v []byte) error {
		var se core.SignedEvent
		if err := core.CanonicalDecode(v, &se); err != nil {
			return nil
		}
		if se.Event.Kind == core.KindLike && se.Event.Like != nil &&
			se.Author == author && se.Event.Like.TargetID == target {
			copy(id[:], k)
			found = true
			return errStop
		}
		return nil
	})
	return id, found
}

// bucketLike is the subset of bolt.Bucket used by existingLikeID, so the
// helper can be tested without a real bbolt transaction.
type bucketLike interface {
	ForEach(fn func(k, v []byte) error) error
}

// ownLogBucket returns the user's own log bucket for reading within tx, or
// nil if the log has no events yet.
func ownLogBucket(tx *bolt.Tx, log core.LogName) *bolt.Bucket {
	return tx.Bucket(bucketOwn).Bucket([]byte(log))
}

// SignAndAppend signs an event of the given kind in the given log, appends
// it to the user's own log, and returns the signed event and its event ID.
// This is the single entry point for creating own events: the CLI offline
// path and the daemon handlers share it so the store operation (sequence,
// sign, append, dedup) is defined once. Liking is idempotent: if the author
// already liked the target post, no new event is created and the existing
// like event's ID is returned.
func (s *Store) SignAndAppend(kp *core.KeyPair, log core.LogName, ev core.Event) (*core.SignedEvent, core.EventID, error) {
	if ev.Kind == core.KindLike && ev.Like != nil {
		var existing core.EventID
		var found bool
		_ = s.db.View(func(tx *bolt.Tx) error {
			b := ownLogBucket(tx, core.PostLog)
			if b == nil {
				return nil
			}
			existing, found = existingLikeID(kp.Identity(), ev.Like.TargetID, b)
			return nil
		})
		if found {
			return nil, existing, nil
		}
	}
	seq, err := s.OwnEventCount(log)
	if err != nil {
		return nil, core.EventID{}, fmt.Errorf("get sequence: %w", err)
	}
	ev.Log = log
	ev.Timestamp = core.Now64()
	ev.Sequence = seq + 1
	se, err := kp.Sign(ev)
	if err != nil {
		return nil, core.EventID{}, fmt.Errorf("sign: %w", err)
	}
	if err := s.AppendOwnEvent(log, se); err != nil {
		return nil, core.EventID{}, fmt.Errorf("append: %w", err)
	}
	id, err := se.ID()
	if err != nil {
		return nil, core.EventID{}, fmt.Errorf("event id: %w", err)
	}
	return se, id, nil
}

// Follow signs a Follow event for target and records the local follow edge.
// Routing both the daemon and the offline CLI through this method keeps the
// follow-state cache in sync with the signed, synced ProfileLog. The signed
// event and its ID are returned along with the resolved identity.
func (s *Store) Follow(kp *core.KeyPair, target [32]byte) (*core.SignedEvent, core.EventID, core.Identity, error) {
	if _, id, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: target},
	}); err != nil {
		return nil, core.EventID{}, "", err
	} else {
		identity := core.IdentityFromPubkey(ed25519.PublicKey(target[:]))
		if err := s.putFollowState(identity, followState{followed: true}); err != nil {
			return nil, core.EventID{}, "", fmt.Errorf("follow state: %w", err)
		}
		return nil, id, identity, nil
	}
}

// Unfollow signs an Unfollow event for target and clears the local follow edge,
// including any pin. A pin is tied to the follow edge, so unfollowing implies
// unpinning: routing both the daemon and the offline CLI through this method
// keeps the clear-on-unfollow invariant in one place. The signed event and
// its ID are returned along with the resolved identity.
func (s *Store) Unfollow(kp *core.KeyPair, target [32]byte) (*core.SignedEvent, core.EventID, core.Identity, error) {
	se, id, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindUnfollow,
		Follow: &core.Follow{TargetPubkey: target},
	})
	if err != nil {
		return nil, core.EventID{}, "", err
	}
	identity := core.IdentityFromPubkey(ed25519.PublicKey(target[:]))
	if err := s.deleteFollowState(identity); err != nil {
		return nil, core.EventID{}, "", fmt.Errorf("follow state: %w", err)
	}
	return se, id, identity, nil
}
