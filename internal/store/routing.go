package store

import (
	"encoding/binary"
	"time"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// PutRouting records the tailcat token used to reach a followed identity.
// The binding is updated whenever a session with that identity completes.
func (s *Store) PutRouting(id core.Identity, token string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRouting).Put([]byte(id), []byte(token))
	})
}

// Routing returns the token bound to an identity, or false if no binding
// exists (the identity is followed but its token is not yet known).
func (s *Store) Routing(id core.Identity) (string, bool, error) {
	var token []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		token = tx.Bucket(bucketRouting).Get([]byte(id))
		return nil
	})
	if err != nil {
		return "", false, err
	}
	if token == nil {
		return "", false, nil
	}
	return string(token), true, nil
}

// DeleteRouting removes the routing binding for an identity, used on unfollow.
func (s *Store) DeleteRouting(id core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRouting).Delete([]byte(id))
	})
}

// RoutingByToken returns the identity bound to the given token, by scanning
// the routing table. The routing table holds only followed identities, so
// this is a small scan. Used to enrich zen display with the connected
// identity's name once a session has bound its token.
func (s *Store) RoutingByToken(token string) (core.Identity, bool, error) {
	var found core.Identity
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRouting).ForEach(func(k, v []byte) error {
			if string(v) == token {
				found = core.Identity(k)
			}
			return nil
		})
	})
	return found, found != "", err
}

// AllRouting returns every (identity, token) binding in the routing table.
// Used by the daemon to rehydrate the in-memory zens map on restart, so the
// known network is visible before the next sync round re-dials each token.
func (s *Store) AllRouting() (map[core.Identity]string, error) {
	out := make(map[core.Identity]string)
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRouting).ForEach(func(k, v []byte) error {
			out[core.Identity(k)] = string(v)
			return nil
		})
	})
	return out, err
}

// SyncCursor returns the high-water timestamp of the last successful pull
// of (author, log). Returns 0 when no cursor is recorded, so a first pull
// requests the full log. The cursor is advanced after events are merged,
// so a re-pull after a dropped connection re-sends and dedups any events
// already stored (see PutRemoteEvent).
func (s *Store) SyncCursor(author core.Identity, log core.LogName) (int64, error) {
	var raw []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		raw = tx.Bucket(bucketCursor).Get(cursorKey(author, log))
		return nil
	})
	if err != nil {
		return 0, err
	}
	if raw == nil {
		return 0, nil
	}
	return int64(binary.BigEndian.Uint64(raw)), nil
}

// PutSyncCursor records the high-water timestamp for (author, log).
func (s *Store) PutSyncCursor(author core.Identity, log core.LogName, ts int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(ts))
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketCursor).Put(cursorKey(author, log), buf[:])
	})
}

func cursorKey(author core.Identity, log core.LogName) []byte {
	k := make([]byte, 0, len(author)+len(log))
	k = append(k, []byte(author)...)
	k = append(k, []byte(log)...)
	return k
}

// secondOf returns the unix-second floor of a nanosecond timestamp.
func secondOf(ns int64) int64 { return ns / int64(time.Second) }
