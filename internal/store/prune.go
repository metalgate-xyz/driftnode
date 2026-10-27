package store

import (
	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// PruneRemotePosts removes remote PostLog events older than the retention
// window from the remote bucket. It scans each per-author sub-bucket and
// deletes any PostLog event whose timestamp is older than now-retention.
// Own PostLog events are never pruned: own logs are backup-critical and
// retained in full. Returns the number of events removed. A zero retention
// window is a no-op (retain everything).
func (s *Store) PruneRemotePosts() (int, error) {
	if s.retention <= 0 {
		return 0, nil
	}
	cutoff := s.now64() - s.retention.Nanoseconds()
	pruned := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		remote := tx.Bucket(bucketRemote)
		return remote.ForEach(func(authorKey, _ []byte) error {
			authorBucket := remote.Bucket(authorKey)
			if authorBucket == nil {
				return nil
			}
			var stale [][]byte
			if err := authorBucket.ForEach(func(k, v []byte) error {
				if !isEventKey(k) {
					return nil
				}
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return nil
				}
				if se.Event.Log == core.PostLog && se.Event.Timestamp < cutoff {
					stale = append(stale, append([]byte(nil), k...))
				}
				return nil
			}); err != nil {
				return err
			}
			for _, k := range stale {
				var evID core.EventID
				copy(evID[:], k)
				if err := authorBucket.Delete(k); err != nil {
					return err
				}
				if err := indexEventRemove(tx, evID); err != nil {
					return err
				}
				if err := indexRefRemoveSource(tx, evID); err != nil {
					return err
				}
				pruned++
			}
			return nil
		})
	})
	return pruned, err
}

// RetentionCutoff returns the nanosecond timestamp below which a PostLog
// event is stale and dropped at ingest, or 0 when retention is disabled.
// The sync layer uses this to raise its pull cursor so peers do not send
// events the receiver would drop.
func (s *Store) RetentionCutoff() int64 {
	if s.retention <= 0 {
		return 0
	}
	return s.now64() - s.retention.Nanoseconds()
}
