package store

import (
	"fmt"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// AppendOwnEvent appends a signed event to the user's own log of the given
// name. Events are keyed by their event ID (the BLAKE3 of the canonical signed
// bytes), so importing a backup merges as a set union of immutable events
// rather than resequencing (§7, §8). A duplicate Like (same author, same
// target) is silently dropped: one Like per author per post. For PostLog
// events, the existence and reference indexes are maintained (an own event is
// added to bucketEventIndex, a referencing own event to bucketEventRef) but
// the target is not looked up: the own path is a pure set-union write and does
// not gate. Indexes cover PostLog events only; ProfileLog compaction is
// handled at write time by tidyProfileLogTx in the SignAndAppend path.
func (s *Store) AppendOwnEvent(log core.LogName, se *core.SignedEvent) error {
	id, err := se.ID()
	if err != nil {
		return fmt.Errorf("event id: %w", err)
	}
	eventBytes, err := core.CanonicalEncode(se)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return appendOwnEventTx(tx, log, se, id, eventBytes)
	})
}

// appendOwnEventTx writes a signed event to the own log within an existing
// transaction. Set-union dedup by event ID; one Like per author per target.
// PostLog events maintain the existence and reference indexes. The caller
// has already computed the event ID and canonical bytes.
func appendOwnEventTx(tx *bolt.Tx, log core.LogName, se *core.SignedEvent, id core.EventID, eventBytes []byte) error {
	logBucket, err := tx.Bucket(bucketOwn).CreateBucketIfNotExists([]byte(log))
	if err != nil {
		return fmt.Errorf("create log bucket: %w", err)
	}
	// Set union: skip if already present.
	if logBucket.Get(id[:]) != nil {
		return nil
	}
	if se.Event.Kind == core.KindLike && se.Event.Like != nil {
		if _, dup := existingLikeID(se.Author, se.Event.Like.TargetID, logBucket); dup {
			return nil
		}
	}
	if err := logBucket.Put(id[:], eventBytes); err != nil {
		return err
	}
	if log == core.PostLog {
		if err := indexEventAdd(tx, id, se.Author, se.Event.Kind); err != nil {
			return err
		}
		if target := referenceTarget(se); !target.IsZero() {
			if err := indexRefAdd(tx, target, id, se.Author, se.Event.Kind); err != nil {
				return err
			}
		}
	}
	return nil
}

// OwnEvents returns all signed events from the user's own log of the given
// name, in sequence order.
func (s *Store) OwnEvents(log core.LogName) ([]core.SignedEvent, error) {
	var out []core.SignedEvent
	err := s.db.View(func(tx *bolt.Tx) error {
		logBucket := tx.Bucket(bucketOwn).Bucket([]byte(log))
		if logBucket == nil {
			return nil
		}
		return logBucket.ForEach(func(k, v []byte) error {
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

// AllOwnEvents returns all signed events from all own logs (Profile, Detail,
// Post), in that order. The DetailLog is included because it is the user's
// own personal metadata and is backup-critical.
func (s *Store) AllOwnEvents() ([]core.SignedEvent, error) {
	prof, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		return nil, err
	}
	detail, err := s.OwnEvents(core.DetailLog)
	if err != nil {
		return nil, err
	}
	posts, err := s.OwnEvents(core.PostLog)
	if err != nil {
		return nil, err
	}
	return append(append(prof, detail...), posts...), err
}

// OwnEventCount returns the number of events in the user's own log of the
// given name. Used by the CLI to assign the next sequence number.
func (s *Store) OwnEventCount(log core.LogName) (uint64, error) {
	var count uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		logBucket := tx.Bucket(bucketOwn).Bucket([]byte(log))
		if logBucket == nil {
			return nil
		}
		count = uint64(logBucket.Stats().KeyN)
		return nil
	})
	return count, err
}
