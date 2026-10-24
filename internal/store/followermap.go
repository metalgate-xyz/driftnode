package store

import (
	"encoding/binary"
	"fmt"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// The follower map is one record per follower author, stored inside the
// follower's author bucket in bucketRemote and keyed by the followed target
// pubkey. Reads use the map instead of scanning every stored event, so the
// cost of a follower lookup grows with the number of tracked authors, not
// with all stored events. The record lives in the same bucket as the Follow
// evidence bytes, so deleting the author bucket (crawl LRU eviction)
// removes map record and evidence together: the map can never claim a
// follower whose proof was deleted.

// followMapPrefix distinguishes a follower-map record key from an event ID
// key inside the same author bucket. Event ID keys are 32 bytes; the
// prefixed map key is 33 bytes, so the two key spaces cannot collide, and
// scanners that decode event values skip keys that are not event IDs.
var followMapPrefix = []byte{0x01}

// metaKeyFollowerCount stores the number of follower-map records targeting
// the local identity, as a big-endian uint64. Incrementally maintained by
// every map write and recomputed from zero by the rebuild.
var metaKeyFollowerCount = []byte("followercount")

// followerCap is the follower map cap. The production value, MaxFollowers,
// is the smallest power of two above world population: no legitimate node
// reaches it, so the map is unbounded in practice. A Follow that would push
// the count past the cap is dropped at ingest without flagging: the author
// side cannot enforce in-degree, so exceeding it is not a signed protocol
// violation. A package variable so tests can exercise the boundary.
var followerCap = uint64(core.MaxFollowers)

// followMapKey returns the author-bucket key for the follower-map record
// targeting the given pubkey.
func followMapKey(target [32]byte) []byte {
	k := make([]byte, 0, 1+32)
	k = append(k, followMapPrefix...)
	k = append(k, target[:]...)
	return k
}

// isFollowMapKey reports whether k is a follower-map record key rather than
// an event ID key.
func isFollowMapKey(k []byte) bool {
	return len(k) == 33 && len(k) > 0 && k[0] == followMapPrefix[0]
}

// isEventKey reports whether a remote author-bucket key addresses stored
// event bytes (a 32-byte event ID) rather than a follower-map record. Every
// author-bucket scanner must skip non-event keys before decoding, since a
// map record's value is a raw event ID, not CBOR.
func isEventKey(k []byte) bool { return len(k) == 32 }

// ownPubkeyInTx returns the local identity's raw pubkey within tx, or the
// zero array when no identity is set.
func ownPubkeyInTx(tx *bolt.Tx) ([32]byte, error) {
	var out [32]byte
	raw := tx.Bucket(bucketMeta).Get(metaKeyIdentity)
	if raw == nil {
		return out, nil
	}
	pub, err := core.Identity(raw).PubkeyBytes()
	if err != nil {
		return out, fmt.Errorf("own identity: %w", err)
	}
	copy(out[:], pub)
	return out, nil
}

// rebuildFollowerMapTx recomputes every follower-map record and the follower
// count from zero by replaying all stored ProfileLog Follow and Unfollow
// events in timestamp + sequence order, like FollowSet.Apply: a follower
// who later unfollows leaves the set. Only events targeting the local
// identity leave records, since only those are ever read. Within tx.
func rebuildFollowerMapTx(tx *bolt.Tx, ownPub [32]byte) (uint64, error) {
	if ownPub == [32]byte{} {
		return 0, nil
	}
	remote := tx.Bucket(bucketRemote)
	var events []core.SignedEvent
	err := remote.ForEach(func(authorKey, _ []byte) error {
		ab := remote.Bucket(authorKey)
		if ab == nil {
			return nil
		}
		return ab.ForEach(func(k, v []byte) error {
			if isFollowMapKey(k) {
				// Clear stale records before re-deriving.
				return ab.Delete(k)
			}
			var se core.SignedEvent
			if err := core.CanonicalDecode(v, &se); err != nil {
				return nil
			}
			if se.Event.Log == core.ProfileLog && se.Event.Follow != nil {
				events = append(events, se)
			}
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	sorted := core.NewLog(events).SortedByTime()
	type edge struct {
		id core.EventID
	}
	winning := make(map[core.Identity]edge)
	for i := range sorted {
		se := &sorted[i]
		if se.Event.Follow.TargetPubkey != ownPub {
			continue
		}
		switch se.Event.Kind {
		case core.KindFollow:
			id, err := se.ID()
			if err != nil {
				continue
			}
			winning[se.Author] = edge{id: id}
		case core.KindUnfollow:
			delete(winning, se.Author)
		}
	}
	var count uint64
	for author, e := range winning {
		ab := remote.Bucket([]byte(author))
		if ab == nil {
			continue
		}
		if err := ab.Put(followMapKey(ownPub), e.id[:]); err != nil {
			return 0, err
		}
		count++
	}
	if err := putFollowerCountTx(tx, count); err != nil {
		return 0, err
	}
	return count, nil
}

// rebuildFollowersTx recomputes the follower map inside the caller's
// transaction and clears the pending-rebuild flag, so the flag decision and
// the rebuild serialize with every other write. Called from ingest paths
// (before applying a ProfileLog event) and from the follower reads.
func (s *Store) rebuildFollowersTx(tx *bolt.Tx) error {
	ownPub, err := ownPubkeyInTx(tx)
	if err != nil {
		return err
	}
	if _, err := rebuildFollowerMapTx(tx, ownPub); err != nil {
		return err
	}
	s.followersNeedsRebuild.Store(false)
	return nil
}

// ensureFollowersReady rebuilds the follower map when a rebuild is pending.
// The lazy rebuild runs on the first follower read after open (or after a
// crawl eviction), so a session that never reads followers never pays the
// scan.
func (s *Store) ensureFollowersReady() error {
	if !s.followersNeedsRebuild.Load() {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		// Re-check under the write lock: a concurrent reader may have
		// rebuilt between our check and this transaction.
		if !s.followersNeedsRebuild.Load() {
			return nil
		}
		return s.rebuildFollowersTx(tx)
	})
}

// putFollowerCountTx persists the follower count within tx.
func putFollowerCountTx(tx *bolt.Tx, count uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], count)
	return tx.Bucket(bucketMeta).Put(metaKeyFollowerCount, buf[:])
}

// followerCountTx reads the persisted follower count within tx.
func followerCountTx(tx *bolt.Tx) uint64 {
	raw := tx.Bucket(bucketMeta).Get(metaKeyFollowerCount)
	if raw == nil {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

// applyFollowMapTx maintains the map for one newly stored ProfileLog Follow
// or Unfollow targeting the local identity, within the caller's ingest
// transaction. Events arrive in any order, so each write compares
// (timestamp, sequence) with the recorded evidence, matching the
// FollowSet.Apply replay: the newest Follow wins, and a newer Unfollow
// removes the record. kind must be KindFollow or KindUnfollow. The caller
// has already confirmed the event is not a duplicate.
func applyFollowMapTx(tx *bolt.Tx, se *core.SignedEvent) error {
	ownPub, err := ownPubkeyInTx(tx)
	if err != nil {
		return err
	}
	if ownPub == [32]byte{} || se.Event.Follow == nil {
		return nil
	}
	if se.Event.Follow.TargetPubkey != ownPub {
		return nil
	}
	fb := tx.Bucket(bucketRemote)
	ab, err := fb.CreateBucketIfNotExists([]byte(se.Author))
	if err != nil {
		return fmt.Errorf("create author bucket: %w", err)
	}
	key := followMapKey(ownPub)
	id, err := se.ID()
	if err != nil {
		return fmt.Errorf("event id: %w", err)
	}
	raw := ab.Get(key)
	havePrev := false
	var prev core.SignedEvent
	if raw != nil {
		var prevID [32]byte
		copy(prevID[:], raw)
		if evBytes := ab.Get(prevID[:]); evBytes != nil {
			if err := core.CanonicalDecode(evBytes, &prev); err == nil {
				havePrev = true
			}
		}
	}
	newer := !havePrev ||
		se.Event.Timestamp > prev.Event.Timestamp ||
		(se.Event.Timestamp == prev.Event.Timestamp && se.Event.Sequence > prev.Event.Sequence)
	switch se.Event.Kind {
	case core.KindFollow:
		if raw != nil && !newer {
			return nil
		}
		if raw == nil && followerCountTx(tx) >= followerCap {
			// New follower over the cap: drop without flagging. The
			// author side cannot enforce in-degree, so exceeding the cap
			// is not a signed protocol violation.
			return nil
		}
		if err := ab.Put(key, id[:]); err != nil {
			return err
		}
		if raw == nil {
			if err := putFollowerCountTx(tx, followerCountTx(tx)+1); err != nil {
				return err
			}
		}
	case core.KindUnfollow:
		if raw == nil || !newer {
			return nil
		}
		if err := ab.Delete(key); err != nil {
			return err
		}
		if n := followerCountTx(tx); n > 0 {
			if err := putFollowerCountTx(tx, n-1); err != nil {
				return err
			}
		}
	}
	return nil
}
