package store

import (
	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// eventRefKey builds the composite key for bucketEventRef: TargetEventID (32
// bytes) followed by SourceEventID (32 bytes). A range scan by the TargetEventID
// prefix finds every event referencing a target in O(k).
func eventRefKey(targetID, sourceID core.EventID) []byte {
	k := make([]byte, 0, 64)
	k = append(k, targetID[:]...)
	k = append(k, sourceID[:]...)
	return k
}

// eventRefPrefix returns the 32-byte prefix for a range scan of all events
// referencing targetID.
func eventRefPrefix(targetID core.EventID) []byte {
	return targetID[:]
}

// indexEventAdd adds an event to the existence index. Called on ingest (own
// and remote) for every stored event.
func indexEventAdd(tx *bolt.Tx, id core.EventID, author core.Identity, kind core.Kind) error {
	b := tx.Bucket(bucketEventIndex)
	return b.Put(id[:], authorKindKey(author, kind))
}

// indexEventRemove removes an event from the existence index. Called on
// delete (cascade) and prune.
func indexEventRemove(tx *bolt.Tx, id core.EventID) error {
	b := tx.Bucket(bucketEventIndex)
	return b.Delete(id[:])
}

// eventExists looks up an event ID in the existence index and returns the
// author and kind if present.
func eventExists(tx *bolt.Tx, id core.EventID) (core.Identity, core.Kind, bool) {
	v := tx.Bucket(bucketEventIndex).Get(id[:])
	return decodeAuthorKind(v)
}

// indexRefAdd adds a reference entry: source event references target event.
// Called on ingest of a referencing event (Like, Reply, Delete) after the
// existence index confirms the target exists.
func indexRefAdd(tx *bolt.Tx, targetID, sourceID core.EventID, author core.Identity, kind core.Kind) error {
	b := tx.Bucket(bucketEventRef)
	return b.Put(eventRefKey(targetID, sourceID), authorKindKey(author, kind))
}

// indexRefRemove removes a single reference entry.
func indexRefRemove(tx *bolt.Tx, targetID, sourceID core.EventID) error {
	b := tx.Bucket(bucketEventRef)
	return b.Delete(eventRefKey(targetID, sourceID))
}

// refSource is a referencing event found by a range scan of the reference
// index.
type refSource struct {
	sourceID core.EventID
	author   core.Identity
	kind     core.Kind
}

// refsByTarget returns every event that references targetID, by range-scanning
// the reference index with the target ID prefix.
func refsByTarget(tx *bolt.Tx, targetID core.EventID) ([]refSource, error) {
	var out []refSource
	b := tx.Bucket(bucketEventRef)
	prefix := eventRefPrefix(targetID)
	c := b.Cursor()
	for k, v := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = c.Next() {
		if len(k) < 32 {
			continue
		}
		var sourceID core.EventID
		copy(sourceID[:], k[32:])
		author, kind, ok := decodeAuthorKind(v)
		if !ok {
			continue
		}
		out = append(out, refSource{sourceID: sourceID, author: author, kind: kind})
	}
	return out, nil
}

// hasPrefix is a local bytes.HasPrefix to avoid importing bytes in the hot
// path. Equivalent to bytes.HasPrefix(k, prefix).
func hasPrefix(k, prefix []byte) bool {
	if len(k) < len(prefix) {
		return false
	}
	for i, b := range prefix {
		if k[i] != b {
			return false
		}
	}
	return true
}

// indexRefRemoveSource removes all reference entries where sourceID is the
// referencing event (the source), by scanning the entire ref index. Used when
// a source event is dropped from the store (cascade or prune) to clean up its
// outbound references.
func indexRefRemoveSource(tx *bolt.Tx, sourceID core.EventID) error {
	b := tx.Bucket(bucketEventRef)
	var keys [][]byte
	if err := b.ForEach(func(k, v []byte) error {
		if len(k) < 64 {
			return nil
		}
		var sid core.EventID
		copy(sid[:], k[32:])
		if sid == sourceID {
			keys = append(keys, append([]byte(nil), k...))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// cleanEventIndexesForBucket removes the existence and reference index
// entries of every event held in a remote author bucket, before the bucket
// itself is deleted. Used by crawl eviction, which drops a whole author at
// once, unlike prune's per-event removal. The bucket's non-event keys
// (follower-map records) are skipped: they have no index entries.
func cleanEventIndexesForBucket(tx *bolt.Tx, ab *bolt.Bucket) {
	_ = ab.ForEach(func(k, _ []byte) error {
		if !isEventKey(k) {
			return nil
		}
		var evID core.EventID
		copy(evID[:], k)
		_ = indexEventRemove(tx, evID)
		_ = indexRefRemoveSource(tx, evID)
		return nil
	})
}

// cascadeDelete removes the target from the existence index and drops every
// event referencing it from the store, cleaning up their index entries. The
// target's bytes stay in the bucket so a re-receive is skipped by the
// set-union dedup check. Called from PutRemoteEvent when a Delete event is
// stored.
func cascadeDelete(tx *bolt.Tx, targetID core.EventID) error {
	// Drop every event referencing the target.
	refs, err := refsByTarget(tx, targetID)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		if err := deleteEventFromStore(tx, ref.sourceID, ref.author); err != nil {
			return err
		}
		if err := indexEventRemove(tx, ref.sourceID); err != nil {
			return err
		}
		if err := indexRefRemoveSource(tx, ref.sourceID); err != nil {
			return err
		}
		if err := indexRefRemove(tx, targetID, ref.sourceID); err != nil {
			return err
		}
	}
	// Remove the target's own outbound references (entries where the target
	// is the source referencing something else).
	if err := indexRefRemoveSource(tx, targetID); err != nil {
		return err
	}
	// Tombstone the target: remove from the existence index so future
	// references to it are dropped. The bytes stay for dedup.
	return indexEventRemove(tx, targetID)
}

// deleteEventFromStore removes an event's bytes from the store bucket that
// holds it (own or remote). Used by the cascade on Delete. The cascade only
// runs for PostLog events, so only the own PostLog and the remote per-author
// bucket need to be checked.
func deleteEventFromStore(tx *bolt.Tx, id core.EventID, author core.Identity) error {
	// Own PostLog.
	if lb := tx.Bucket(bucketOwn).Bucket([]byte(core.PostLog)); lb != nil && lb.Get(id[:]) != nil {
		return lb.Delete(id[:])
	}
	// Remote bucket: per-author sub-bucket.
	remote := tx.Bucket(bucketRemote)
	if ab := remote.Bucket([]byte(author)); ab != nil && ab.Get(id[:]) != nil {
		return ab.Delete(id[:])
	}
	return nil
}

// EventExists reports whether an event with the given ID is in the local
// existence index (own or remote). Used by the author side to confirm a post
// is held locally before signing a Like or Reply.
func (s *Store) EventExists(id core.EventID) (bool, error) {
	var exists bool
	err := s.db.View(func(tx *bolt.Tx) error {
		_, _, exists = eventExists(tx, id)
		return nil
	})
	return exists, err
}

// referenceTarget returns the EventID referenced by a referencing event, or
// the zero EventID if the event does not reference anything. A Post with a
// non-zero ParentID references its parent; a Like references its target; a
// Delete references its target.
func referenceTarget(se *core.SignedEvent) core.EventID {
	switch se.Event.Kind {
	case core.KindLike:
		if se.Event.Like != nil {
			return se.Event.Like.TargetID
		}
	case core.KindDelete:
		if se.Event.Delete != nil {
			return se.Event.Delete.TargetID
		}
	case core.KindPost:
		if se.Event.Post != nil && !se.Event.Post.ParentID.IsZero() {
			return se.Event.Post.ParentID
		}
	}
	return core.EventID{}
}
