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

// ErrHeld is returned by EnqueuePostEvent when the event has been enqueued
// for rate-limited signing. The caller blocks on the returned result channel
// for the signed event and its ID.
var ErrHeld = errors.New("event held for rate-limited signing")

// existingLikeEventID returns the event ID of the author's existing Like
// event targeting ev.Like.TargetID in the own PostLog, if any. One Like per
// author per post: a re-like returns the existing event's ID instead of
// creating a new one.
func (s *Store) existingLikeEventID(kp *core.KeyPair, ev core.Event) (core.EventID, bool) {
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
	return existing, found
}

// stampSignAppend assigns the next sequence number, stamps with the current
// wall clock, signs, and appends to the own log. Shared by the synchronous
// (SignAndAppend) and rate-limited (postQueue.process) paths. The sequence
// counter, the ProfileLog tidy-at-write, and the append run in one
// transaction, so concurrent callers never collide on a sequence number and
// the tidy log is updated atomically with the new event.
func (s *Store) stampSignAppend(kp *core.KeyPair, log core.LogName, ev core.Event) (*core.SignedEvent, core.EventID, error) {
	ev.Log = log
	var se *core.SignedEvent
	var id core.EventID
	err := s.db.Update(func(tx *bolt.Tx) error {
		if log == core.ProfileLog {
			if err := tidyProfileLogTx(tx, kp.Identity(), ev); err != nil {
				return fmt.Errorf("tidy profile log: %w", err)
			}
		}
		seq, err := nextSequence(tx, log)
		if err != nil {
			return fmt.Errorf("get sequence: %w", err)
		}
		ev.Sequence = seq
		ev.Timestamp = core.Now64()
		signed, err := kp.Sign(ev)
		if err != nil {
			return fmt.Errorf("sign: %w", err)
		}
		eventID, err := signed.ID()
		if err != nil {
			return fmt.Errorf("event id: %w", err)
		}
		eventBytes, err := core.CanonicalEncode(signed)
		if err != nil {
			return fmt.Errorf("encode event: %w", err)
		}
		if err := appendOwnEventTx(tx, log, signed, eventID, eventBytes); err != nil {
			return fmt.Errorf("append: %w", err)
		}
		se = signed
		id = eventID
		return nil
	})
	if err != nil {
		return nil, core.EventID{}, err
	}
	return se, id, nil
}

// SignAndAppend signs an event of the given kind in the given log, appends
// it to the user's own log, and returns the signed event and its event ID.
// This is the synchronous entry point for ProfileLog and DetailLog events,
// which are not rate-limited: bulk-follow onboarding is a legitimate burst,
// and ProfileLog volume is bounded by compaction (phase 7). A Follow that
// would push the net follow count past MaxFollows is rejected: the author
// side enforces the cap so a ProfileLog with more than MaxFollows net follows
// is a protocol violation by a signed key. Re-following an already-followed
// target does not change the net count, so it is allowed. Liking is
// idempotent: if the author already liked the target post, no new event is
// created and the existing like event's ID is returned.
func (s *Store) SignAndAppend(kp *core.KeyPair, log core.LogName, ev core.Event) (*core.SignedEvent, core.EventID, error) {
	if ev.Kind == core.KindLike && ev.Like != nil {
		if existing, found := s.existingLikeEventID(kp, ev); found {
			return nil, existing, nil
		}
	}
	if ev.Kind == core.KindFollow && ev.Follow != nil {
		fs, err := s.ownFollowSet()
		if err != nil {
			return nil, core.EventID{}, fmt.Errorf("follow set: %w", err)
		}
		if !fs.Contains(ev.Follow.TargetPubkey) && fs.Len() >= core.MaxFollows {
			return nil, core.EventID{}, core.ErrFollowCapExceeded
		}
	}
	return s.stampSignAppend(kp, log, ev)
}

// ownFollowSet returns the current follow set projected from the user's own
// ProfileLog. Used by the author-side follow cap check in SignAndAppend.
func (s *Store) ownFollowSet() (*core.FollowSet, error) {
	events, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		return nil, err
	}
	return core.NewLog(events).FollowSet(), nil
}

// EnqueuePostEvent enqueues a PostLog event (Post, Like, Delete) for
// rate-limited signing at 1/sec per kind. The actual stamp, sign, and append
// happen in the background goroutine, not in the caller. It returns a result
// channel the caller blocks on for the signed event and its ID, with
// ErrHeld indicating the event was enqueued. Liking is idempotent: if the
// author already liked the target post, the existing like event's ID is
// returned synchronously with no hold. If the queue is full,
// ErrRateLimitQueueFull is returned.
func (s *Store) EnqueuePostEvent(kp *core.KeyPair, log core.LogName, ev core.Event) (chan signResult, error) {
	if ev.Kind == core.KindLike && ev.Like != nil {
		if existing, found := s.existingLikeEventID(kp, ev); found {
			ch := make(chan signResult, 1)
			ch <- signResult{ID: existing}
			return ch, nil
		}
	}
	lastSec, err := s.lastStoredSecondOfKind(ev.Kind)
	if err != nil {
		return nil, fmt.Errorf("last stored second: %w", err)
	}
	ch, err := s.posts.tryEnqueue(kp, log, ev, lastSec)
	if err != nil {
		return nil, err
	}
	return ch, ErrHeld
}

// lastStoredSecondOfKind returns the second of the most recent event of the
// given kind in the own PostLog, or 0 if none. Seeds the rate limiter's
// per-kind second so a restart does not let two events land in the same
// second as the last one before shutdown.
func (s *Store) lastStoredSecondOfKind(kind core.Kind) (int64, error) {
	var maxSec int64
	err := s.db.View(func(tx *bolt.Tx) error {
		b := ownLogBucket(tx, core.PostLog)
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, v []byte) error {
			var se core.SignedEvent
			if err := core.CanonicalDecode(v, &se); err != nil {
				return nil
			}
			if se.Event.Kind == kind {
				sec := secondOf(se.Event.Timestamp)
				if sec > maxSec {
					maxSec = sec
				}
			}
			return nil
		})
	})
	return maxSec, err
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
