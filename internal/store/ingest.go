package store

import (
	"crypto/ed25519"
	"encoding/binary"
	"fmt"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// followState is the set of local flags cached per identity: whether it is
// followed, whether the follow is pinned, and whether the author has been
// flagged for a protocol violation (oversized event, rate-limit breach, etc).
// Stored as a fixed 3-byte value so reads and writes are single-record and
// the flags can't drift apart.
type followState struct {
	followed bool
	pinned   bool
	flagged  bool
}

func (f followState) encode() []byte {
	out := make([]byte, 3)
	if f.followed {
		out[0] = 1
	}
	if f.pinned {
		out[1] = 1
	}
	if f.flagged {
		out[2] = 1
	}
	return out
}

func decodeFollowState(v []byte) followState {
	var f followState
	if len(v) >= 1 && v[0] != 0 {
		f.followed = true
	}
	if len(v) >= 2 && v[1] != 0 {
		f.pinned = true
	}
	if len(v) >= 3 && v[2] != 0 {
		f.flagged = true
	}
	return f
}

// PutRemoteEvent stores a remote event (synced PostLog or crawled Profile
// log), keyed by event ID for set-union deduplication (an event already
// present is not overwritten). Returns true if the event was newly inserted.
// An oversized event (canonical Event bytes over the per-kind cap) is
// dropped and the author is flagged: a valid signature on an oversized
// event means the author's key signed something their own client would
// have blocked. The caller must have already called Verify; this check
// runs on the verified event so the flag is never raised on a transit
// tamper. A PostLog event older than the retention window is dropped
// silently: a stale event is not misconduct, so the author is not flagged.
// A PostLog referencing event (Like, Reply, Delete) is stored only if its
// target resolves to a non-tombstoned event the receiver already holds:
// existence is a receiver-local property, so an orphan is dropped without
// flagging. When a PostLog Delete is stored, every event referencing the
// deleted target is dropped from the store and its index entries removed,
// so the projection stays clean. The existence and reference indexes cover
// PostLog events only; ProfileLog compaction is handled in phase 7.
func (s *Store) PutRemoteEvent(se *core.SignedEvent, seq uint64) (bool, error) {
	canon, err := core.CanonicalEncode(se.Event)
	if err != nil {
		return false, fmt.Errorf("canonical event bytes: %w", err)
	}
	if len(canon) > core.CapForKind(se.Event.Kind) {
		return false, s.flagAuthor(se.Author)
	}
	if !hasRequiredFollowWork(se) {
		return false, s.flagAuthor(se.Author)
	}
	if se.Event.Timestamp > s.now64()+core.MaxFutureSkew.Nanoseconds() {
		return false, nil
	}
	if se.Event.Log == core.PostLog && s.retention > 0 && se.Event.Timestamp < s.now64()-s.retention.Nanoseconds() {
		return false, nil
	}
	id, err := se.ID()
	if err != nil {
		return false, fmt.Errorf("event id: %w", err)
	}
	eventBytes, err := core.CanonicalEncode(se)
	if err != nil {
		return false, fmt.Errorf("encode event: %w", err)
	}
	inserted := false
	err = s.db.Update(func(tx *bolt.Tx) error {
		fb := tx.Bucket(bucketRemote)
		authorBucket, err := fb.CreateBucketIfNotExists([]byte(se.Author))
		if err != nil {
			return fmt.Errorf("create author bucket: %w", err)
		}
		if authorBucket.Get(id[:]) != nil {
			return nil
		}
		if se.Event.Kind == core.KindLike && se.Event.Like != nil {
			if _, dup := existingLikeID(se.Author, se.Event.Like.TargetID, authorBucket); dup {
				return nil
			}
		}
		// Follow cap: an author whose net follows exceed MaxFollows has
		// signed a ProfileLog their own client would have blocked, so the
		// receiver flags them and skips the store. The author's own client
		// blocks the 5001st Follow, so this is a protocol violation by a
		// signed key. Runs only for ProfileLog events; PostLog volume is
		// bounded by retention and the rate limit. An already-flagged
		// author is skipped without re-projecting.
		if se.Event.Log == core.ProfileLog {
			fsb := tx.Bucket(bucketFollowState)
			if st := decodeFollowState(fsb.Get([]byte(se.Author))); st.flagged {
				return nil
			}
			count, err := remoteFollowCount(tx, se.Author, se)
			if err != nil {
				return fmt.Errorf("project follow count: %w", err)
			}
			if count > core.MaxFollows {
				if err := flagAuthorTx(tx, se.Author); err != nil {
					return err
				}
				return nil
			}
			// Follower-map cap: a Follow targeting this zen past the
			// follower cap is dropped without flagging, since the author
			// side cannot enforce in-degree.
			if se.Event.Kind == core.KindFollow && se.Event.Follow != nil {
				ownPub, err := ownPubkeyInTx(tx)
				if err != nil {
					return err
				}
				if se.Event.Follow.TargetPubkey == ownPub && followerCountTx(tx) >= followerCap {
					return nil
				}
			}
		}
		// Existence gate: a PostLog referencing event (Like, Reply, Delete)
		// is stored only if its target exists in the local index.
		// Existence is receiver-local, so an orphan is dropped without
		// flagging. ProfileLog events are not gated: their compaction is
		// handled in phase 7.
		target := referenceTarget(se)
		if se.Event.Log == core.PostLog && !target.IsZero() {
			if _, _, exists := eventExists(tx, target); !exists {
				return nil
			}
		}
		if se.Event.Log == core.PostLog {
			rsb := tx.Bucket(bucketRateSecond)
			key := authorKindKey(se.Author, se.Event.Kind)
			evSec := secondOf(se.Event.Timestamp)
			if raw := rsb.Get(key); raw != nil {
				lastSec := int64(binary.BigEndian.Uint64(raw))
				if evSec == lastSec {
					fsb := tx.Bucket(bucketFollowState)
					st := decodeFollowState(fsb.Get([]byte(se.Author)))
					st.flagged = true
					_ = fsb.Put([]byte(se.Author), st.encode())
					return nil
				}
			}
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], uint64(evSec))
			if err := rsb.Put(key, buf[:]); err != nil {
				return fmt.Errorf("put rate second: %w", err)
			}
		}
		inserted = true
		if err := authorBucket.Put(id[:], eventBytes); err != nil {
			return err
		}
		if se.Event.Log == core.ProfileLog {
			// Follower-map maintenance for a newly stored Follow or
			// Unfollow targeting the local identity. While a rebuild is
			// pending, maintenance is deferred: the event is stored
			// either way, and the first follower read rebuilds from
			// zero over the stored events, so nothing is lost.
			if !s.followersNeedsRebuild.Load() {
				if err := applyFollowMapTx(tx, se); err != nil {
					return fmt.Errorf("follower map: %w", err)
				}
			}
		}
		if se.Event.Log != core.PostLog {
			return nil
		}
		if err := indexEventAdd(tx, id, se.Author, se.Event.Kind); err != nil {
			return err
		}
		// Cascade on Delete: remove the target from the index and drop
		// every event referencing it from the store, so the projection
		// stays clean. The target's bytes stay in the bucket for dedup
		// (a re-receive is skipped by the set-union check). Runs before
		// the Delete's own ref entry is added, so the range scan by target
		// does not find the Delete itself.
		if se.Event.Kind == core.KindDelete && se.Event.Delete != nil {
			if err := cascadeDelete(tx, target); err != nil {
				return err
			}
		}
		if !target.IsZero() {
			if err := indexRefAdd(tx, target, id, se.Author, se.Event.Kind); err != nil {
				return err
			}
		}
		return nil
	})
	return inserted, err
}

// CrawledEvents returns all cached events from a remote identity.
func (s *Store) CrawledEvents(author core.Identity) ([]core.SignedEvent, error) {
	var out []core.SignedEvent
	err := s.db.View(func(tx *bolt.Tx) error {
		authorBucket := tx.Bucket(bucketRemote).Bucket([]byte(author))
		if authorBucket == nil {
			return nil
		}
		return authorBucket.ForEach(func(k, v []byte) error {
			if !isEventKey(k) {
				return nil
			}
			var se core.SignedEvent
			if err := core.CanonicalDecode(v, &se); err != nil {
				return fmt.Errorf("decode event at %x: %w", k, err)
			}
			out = append(out, se)
			return nil
		})
	})
	return out, err
}

// rebuildFollowState reconstructs the follow-state cache from the own
// ProfileLog, clearing and re-deriving which identities are currently
// followed. Pins are not restored here: PutPinnedIdentities applies them
// after the follow edges exist.
func (s *Store) rebuildFollowState() error {
	events, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		return err
	}
	fs := core.NewLog(events).FollowSet()
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFollowState)
		if err := b.ForEach(func(k, v []byte) error { return b.Delete(k) }); err != nil {
			return err
		}
		var idErr error
		fs.Each(func(target [32]byte) {
			if idErr != nil {
				return
			}
			identity := core.IdentityFromPubkey(ed25519.PublicKey(target[:]))
			if err := b.Put([]byte(identity), followState{followed: true}.encode()); err != nil {
				idErr = err
			}
		})
		return idErr
	})
}

// PutCrawledProfile stores a Profile-log event from a crawled (not followed)
// identity in the shared remote bucket. The crawler stores only Profile logs;
// PostLog events arrive via PutRemoteEvent from sync. Both share the same
// per-author dedup by event ID. An oversized event is dropped and the author
// is flagged, as in PutRemoteEvent.
func (s *Store) PutCrawledProfile(author core.Identity, se *core.SignedEvent) error {
	canon, err := core.CanonicalEncode(se.Event)
	if err != nil {
		return fmt.Errorf("canonical event bytes: %w", err)
	}
	if len(canon) > core.CapForKind(se.Event.Kind) {
		return s.flagAuthor(author)
	}
	if !hasRequiredFollowWork(se) {
		return s.flagAuthor(author)
	}
	id, err := se.ID()
	if err != nil {
		return fmt.Errorf("event id: %w", err)
	}
	eventBytes, err := core.CanonicalEncode(se)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		authorBucket, err := tx.Bucket(bucketRemote).CreateBucketIfNotExists([]byte(author))
		if err != nil {
			return err
		}
		// Set union: skip if already present.
		if authorBucket.Get(id[:]) != nil {
			return nil
		}
		// Follow cap: same check as PutRemoteEvent. The crawler stores
		// only ProfileLog events, so every crawled event is subject to
		// the net-follow cap.
		fsb := tx.Bucket(bucketFollowState)
		if st := decodeFollowState(fsb.Get([]byte(author))); st.flagged {
			return nil
		}
		count, err := remoteFollowCount(tx, author, se)
		if err != nil {
			return fmt.Errorf("project follow count: %w", err)
		}
		if count > core.MaxFollows {
			return flagAuthorTx(tx, author)
		}
		// Follower-map cap: a Follow targeting this zen past the follower
		// cap is dropped without flagging, since the author side cannot
		// enforce in-degree.
		if se.Event.Kind == core.KindFollow && se.Event.Follow != nil {
			ownPub, err := ownPubkeyInTx(tx)
			if err != nil {
				return err
			}
			if se.Event.Follow.TargetPubkey == ownPub && followerCountTx(tx) >= followerCap {
				return nil
			}
		}
		if err := authorBucket.Put(id[:], eventBytes); err != nil {
			return err
		}
		// Follower-map maintenance: a crawled Follow targeting this zen
		// counts as an observation, deferred the same way as the sync
		// path while a rebuild is pending.
		if se.Event.Log == core.ProfileLog && !s.followersNeedsRebuild.Load() {
			if err := applyFollowMapTx(tx, se); err != nil {
				return fmt.Errorf("follower map: %w", err)
			}
		}
		return nil
	})
}

// CrawledProfiles returns all stored Profile-log events for a crawled
// identity.
func (s *Store) CrawledProfiles(author core.Identity) ([]core.SignedEvent, error) {
	var out []core.SignedEvent
	err := s.db.View(func(tx *bolt.Tx) error {
		authorBucket := tx.Bucket(bucketRemote).Bucket([]byte(author))
		if authorBucket == nil {
			return nil
		}
		return authorBucket.ForEach(func(k, v []byte) error {
			if !isEventKey(k) {
				return nil
			}
			var se core.SignedEvent
			if err := core.CanonicalDecode(v, &se); err != nil {
				return nil
			}
			if se.Event.Log == core.ProfileLog {
				out = append(out, se)
			}
			return nil
		})
	})
	return out, err
}

// DisplayName resolves the zen name (Profile.DisplayName) for an identity,
// projected last-write-wins over its Profile-log events. For the local
// identity it reads the own ProfileLog; for others it reads the remote cache.
// Returns an empty string if no Profile event is known.
func (s *Store) DisplayName(id core.Identity) (string, error) {
	ownID, ok, err := s.Identity()
	if err != nil {
		return "", err
	}
	var events []core.SignedEvent
	if ok && id == ownID {
		events, err = s.OwnEvents(core.ProfileLog)
	} else {
		events, err = s.CrawledProfiles(id)
	}
	if err != nil {
		return "", err
	}
	if prof := core.NewLog(events).Profile(); prof != nil {
		return prof.DisplayName, nil
	}
	return "", nil
}

// CrawledIdentities returns the set of identities whose events are cached in
// the remote bucket. This is the set of zens we have synced with or crawled,
// not the declared follow graph; use FollowGraph for the latter.
func (s *Store) CrawledIdentities() ([]core.Identity, error) {
	var out []core.Identity
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRemote).ForEach(func(k, v []byte) error {
			out = append(out, core.Identity(k))
			return nil
		})
	})
	return out, err
}

// putFollowState writes the cached follow+pin+flag flags for an identity.
func (s *Store) putFollowState(id core.Identity, st followState) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFollowState).Put([]byte(id), st.encode())
	})
}

// flagAuthor marks an identity as flagged for a protocol violation. A flag
// means the author's key signed the offending event, so the author is
// compromised or misbehaving, never framed.
func (s *Store) flagAuthor(id core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return flagAuthorTx(tx, id)
	})
}

// flagAuthorTx marks an identity as flagged within an existing transaction.
func flagAuthorTx(tx *bolt.Tx, id core.Identity) error {
	b := tx.Bucket(bucketFollowState)
	st := decodeFollowState(b.Get([]byte(id)))
	st.flagged = true
	return b.Put([]byte(id), st.encode())
}

// remoteFollowCount projects the author's held ProfileLog plus the candidate
// event and returns the net follow count. Used by the receiver-side follow
// cap check: an author whose net follows exceed MaxFollows has signed a
// ProfileLog their own client would have blocked, so the receiver flags them
// and skips the store. The candidate is applied in timestamp+sequence order
// alongside the held events so the projection matches the post-insert state.
func remoteFollowCount(tx *bolt.Tx, author core.Identity, candidate *core.SignedEvent) (int, error) {
	var events []core.SignedEvent
	if ab := tx.Bucket(bucketRemote).Bucket([]byte(author)); ab != nil {
		if err := ab.ForEach(func(k, v []byte) error {
			if !isEventKey(k) {
				return nil
			}
			var se core.SignedEvent
			if err := core.CanonicalDecode(v, &se); err != nil {
				return nil
			}
			if se.Event.Log == core.ProfileLog {
				events = append(events, se)
			}
			return nil
		}); err != nil {
			return 0, err
		}
	}
	events = append(events, *candidate)
	return core.NewLog(events).FollowSet().Len(), nil
}

// deleteFollowState removes the cached follow+pin+flag flags for an identity,
// clearing both the follow edge and any pin on it.
func (s *Store) deleteFollowState(id core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFollowState).Delete([]byte(id))
	})
}

// getFollowState reads the cached follow+pin+flag flags for an identity.
func (s *Store) getFollowState(id core.Identity) (followState, error) {
	var st followState
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketFollowState).Get([]byte(id))
		st = decodeFollowState(v)
		return nil
	})
	return st, err
}

// IsFollowed reports whether an identity is in the current follow graph,
// read from the local follow-state cache. The cache is kept in sync with the
// signed, synced ProfileLog by store.Follow and store.Unfollow, and rebuilt
// from the log on backup restore.
func (s *Store) IsFollowed(id core.Identity) (bool, error) {
	st, err := s.getFollowState(id)
	return st.followed, err
}

// IsFlagged reports whether an identity has been flagged for a protocol
// violation (oversized event, rate-limit breach, etc). A flag means the
// author's key signed the offending event.
func (s *Store) IsFlagged(id core.Identity) (bool, error) {
	st, err := s.getFollowState(id)
	return st.flagged, err
}

// PinIdentity marks a follow as pinned for top sync priority. A pin is a
// property of the follow edge, so the identity must be followed: pinning a
// non-follow is rejected. A pin is local state, never synced or signed, and
// is cleared on unfollow. Backup-critical, since losing the pin set would
// silently downgrade high-priority follows on restore.
func (s *Store) PinIdentity(id core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFollowState)
		v := b.Get([]byte(id))
		st := decodeFollowState(v)
		if !st.followed {
			return ErrPinRequiresFollow
		}
		st.pinned = true
		return b.Put([]byte(id), st.encode())
	})
}

// UnpinIdentity removes a sync-priority pin from a follow.
func (s *Store) UnpinIdentity(id core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFollowState)
		v := b.Get([]byte(id))
		st := decodeFollowState(v)
		st.pinned = false
		if !st.followed && !st.pinned {
			return b.Delete([]byte(id))
		}
		return b.Put([]byte(id), st.encode())
	})
}

// IsPinned reports whether a follow is pinned for sync priority.
func (s *Store) IsPinned(id core.Identity) (bool, error) {
	st, err := s.getFollowState(id)
	return st.pinned, err
}

// PinnedIdentities returns every followed identity pinned for sync priority,
// used for backup export, the follows view, and the weight formula's
// pin(author) metric.
func (s *Store) PinnedIdentities() ([]core.Identity, error) {
	var out []core.Identity
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFollowState).ForEach(func(k, v []byte) error {
			if decodeFollowState(v).pinned {
				out = append(out, core.Identity(k))
			}
			return nil
		})
	})
	return out, err
}

// PutPinnedIdentities applies a set of pins onto existing follow edges, used
// by backup import after the follow-state cache has been rebuilt from the
// restored ProfileLog. Identities not in the follow graph are skipped, since
// a pin requires a follow.
func (s *Store) PutPinnedIdentities(ids []core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFollowState)
		for _, id := range ids {
			v := b.Get([]byte(id))
			st := decodeFollowState(v)
			if !st.followed {
				continue
			}
			st.pinned = true
			if err := b.Put([]byte(id), st.encode()); err != nil {
				return err
			}
		}
		return nil
	})
}
