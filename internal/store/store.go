// Package store is the bbolt-backed local store for driftnode. It holds
// the account's encrypted keypair, its own Profile log and PostLog, followed
// accounts' synced PostLogs (re-fetchable cache), and the media cache.
// Everything is accessed offline, with no daemon or network (§13 step 2).
package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// Bucket names. Own events are keyed by event ID (BLAKE3-256 of the canonical
// signed bytes) within per-log buckets, so backup import merges as a set
// union of immutable events. Followed events are keyed by event ID within
// per-author buckets for the same dedup property.
var (
	bucketMeta    = []byte("meta")
	bucketKey     = []byte("key")     // single EncryptedKey value under key "key"
	bucketOwn     = []byte("own")     // own logs: sub-buckets per log name
	bucketFollows = []byte("follows") // synced PostLogs, keyed by author+eventID
	bucketMedia   = []byte("media")   // content-addressed blobs
)

// meta keys
var metaKeyIdentity = []byte("identity") // own identity string

// Store is the local persistent store. It wraps a bbolt DB.
type Store struct {
	db   *bolt.DB
	path string
}

// Open opens or creates the store at path. The parent directory is created if
// missing.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bbolt: %w", err)
	}
	s := &Store{db: db, path: path}
	if err := s.initBuckets(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// Path returns the filesystem path of the store.
func (s *Store) Path() string { return s.path }

func (s *Store) initBuckets() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketMeta, bucketKey, bucketOwn, bucketFollows, bucketMedia} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return fmt.Errorf("create bucket %q: %w", b, err)
			}
		}
		return nil
	})
}

// InitIdentity stores the encrypted keypair and the derived identity string.
// This is what `driftnode init` calls after generating a keypair.
func (s *Store) InitIdentity(kp *core.KeyPair, enc *core.EncryptedKey) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		keyBucket := tx.Bucket(bucketKey)
		if existing := keyBucket.Get([]byte("key")); existing != nil {
			return errors.New("identity already initialized; use `key import` to replace")
		}
		encBytes, err := core.CanonicalEncode(enc)
		if err != nil {
			return fmt.Errorf("encode key: %w", err)
		}
		if err := keyBucket.Put([]byte("key"), encBytes); err != nil {
			return fmt.Errorf("put key: %w", err)
		}
		return tx.Bucket(bucketMeta).Put(metaKeyIdentity, []byte(kp.Identity().String()))
	})
}

// Identity returns the stored identity string, or false if not initialized.
func (s *Store) Identity() (core.Identity, bool, error) {
	var id []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		id = tx.Bucket(bucketMeta).Get(metaKeyIdentity)
		return nil
	})
	if err != nil {
		return "", false, err
	}
	if id == nil {
		return "", false, nil
	}
	return core.Identity(id), true, nil
}

// EncryptedKey returns the stored encrypted keypair.
func (s *Store) EncryptedKey() (*core.EncryptedKey, error) {
	var raw []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		raw = tx.Bucket(bucketKey).Get([]byte("key"))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, errors.New("no keypair stored; run `driftnode init`")
	}
	var ek core.EncryptedKey
	if err := core.CanonicalDecode(raw, &ek); err != nil {
		return nil, fmt.Errorf("decode key: %w", err)
	}
	return &ek, nil
}

// PutKey replaces the stored encrypted keypair (used by `key import`).
func (s *Store) PutKey(kp *core.KeyPair, enc *core.EncryptedKey) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		encBytes, err := core.CanonicalEncode(enc)
		if err != nil {
			return fmt.Errorf("encode key: %w", err)
		}
		if err := tx.Bucket(bucketKey).Put([]byte("key"), encBytes); err != nil {
			return err
		}
		return tx.Bucket(bucketMeta).Put(metaKeyIdentity, []byte(kp.Identity().String()))
	})
}

// AppendOwnEvent appends a signed event to the user's own log of the given
// name. Events are keyed by their event ID (the BLAKE3 of the canonical signed
// bytes), so importing a backup merges as a set union of immutable events
// rather than resequencing (§7, §8).
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
		logBucket, err := tx.Bucket(bucketOwn).CreateBucketIfNotExists([]byte(log))
		if err != nil {
			return fmt.Errorf("create log bucket: %w", err)
		}
		// Set union: skip if already present.
		if logBucket.Get(id[:]) != nil {
			return nil
		}
		return logBucket.Put(id[:], eventBytes)
	})
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

// AllOwnEvents returns all signed events from both own logs.
func (s *Store) AllOwnEvents() ([]core.SignedEvent, error) {
	prof, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		return nil, err
	}
	posts, err := s.OwnEvents(core.PostLog)
	if err != nil {
		return nil, err
	}
	return append(prof, posts...), err
}

// PutFollowedEvent stores an event from a followed identity's PostLog,
// keyed by event ID for set-union deduplication (an event already present is
// not overwritten). Returns true if the event was newly inserted.
func (s *Store) PutFollowedEvent(se *core.SignedEvent, seq uint64) (bool, error) {
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
		fb := tx.Bucket(bucketFollows)
		authorBucket, err := fb.CreateBucketIfNotExists([]byte(se.Author))
		if err != nil {
			return fmt.Errorf("create author bucket: %w", err)
		}
		if authorBucket.Get(id[:]) != nil {
			return nil
		}
		inserted = true
		return authorBucket.Put(id[:], eventBytes)
	})
	return inserted, err
}

// FollowedEvents returns all synced events from a followed identity.
func (s *Store) FollowedEvents(author core.Identity) ([]core.SignedEvent, error) {
	var out []core.SignedEvent
	err := s.db.View(func(tx *bolt.Tx) error {
		authorBucket := tx.Bucket(bucketFollows).Bucket([]byte(author))
		if authorBucket == nil {
			return nil
		}
		return authorBucket.ForEach(func(k, v []byte) error {
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

// PutMedia stores a content-addressed media blob.
func (s *Store) PutMedia(hash core.ContentHash, data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMedia).Put(hash[:], data)
	})
}

// GetMedia retrieves a content-addressed media blob.
func (s *Store) GetMedia(hash core.ContentHash) ([]byte, error) {
	var out []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		out = tx.Bucket(bucketMedia).Get(hash[:])
		return nil
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, errors.New("media not found")
	}
	// bbolt reuses its buffer; copy.
	c := make([]byte, len(out))
	copy(c, out)
	return c, nil
}

// ExportBackup returns the account-critical state for a single-file backup
// (§8): the encrypted keypair and the user's own logs.
func (s *Store) ExportBackup() (*core.BackupData, error) {
	ek, err := s.EncryptedKey()
	if err != nil {
		return nil, err
	}
	events, err := s.AllOwnEvents()
	if err != nil {
		return nil, err
	}
	return &core.BackupData{Key: ek, OwnLogs: events}, nil
}

// ImportBackup merges a backup into the store: replaces the keypair (via the
// CLI's separate PutKey call after decryption) and merges own logs as a set
// union of signed events (§8). AppendOwnEvent dedups by event ID, so this is
// idempotent.
func (s *Store) ImportBackup(bd *core.BackupData) error {
	if bd.Key == nil {
		return errors.New("backup missing key")
	}
	for _, se := range bd.OwnLogs {
		if err := s.AppendOwnEvent(se.Event.Log, &se); err != nil {
			return err
		}
	}
	return nil
}

// PutEncryptedKey stores an encrypted keypair directly (used by `key import`),
// without the identity string (which can only be derived after decryption).
func (s *Store) PutEncryptedKey(enc *core.EncryptedKey) error {
	encBytes, err := core.CanonicalEncode(enc)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketKey).Put([]byte("key"), encBytes); err != nil {
			return err
		}
		// Clear the cached identity; it will be set when the key is next
		// unlocked and the keypair is known.
		return tx.Bucket(bucketMeta).Delete(metaKeyIdentity)
	})
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

// SetIdentity stores the identity string for the current keypair. Called by
// the CLI after unlocking an imported key.
func (s *Store) SetIdentity(id core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put(metaKeyIdentity, []byte(id.String()))
	})
}

// PutCrawledProfile stores a Profile-log event from a crawled (not followed)
// identity. These form a bounded LRU cache (section 7.4), keyed by author
// identity and event ID.
func (s *Store) PutCrawledProfile(author core.Identity, se *core.SignedEvent) error {
	id, err := se.ID()
	if err != nil {
		return fmt.Errorf("event id: %w", err)
	}
	eventBytes, err := core.CanonicalEncode(se)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		authorBucket, err := tx.Bucket(bucketFollows).CreateBucketIfNotExists([]byte(author))
		if err != nil {
			return err
		}
		return authorBucket.Put(id[:], eventBytes)
	})
}

// CrawledProfiles returns all stored Profile-log events for a crawled
// identity.
func (s *Store) CrawledProfiles(author core.Identity) ([]core.SignedEvent, error) {
	var out []core.SignedEvent
	err := s.db.View(func(tx *bolt.Tx) error {
		authorBucket := tx.Bucket(bucketFollows).Bucket([]byte(author))
		if authorBucket == nil {
			return nil
		}
		return authorBucket.ForEach(func(k, v []byte) error {
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

// FollowedIdentities returns the set of identities whose events are cached in
// the follows bucket.
func (s *Store) FollowedIdentities() ([]core.Identity, error) {
	var out []core.Identity
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFollows).ForEach(func(k, v []byte) error {
			out = append(out, core.Identity(k))
			return nil
		})
	})
	return out, err
}

// AllPosts returns own PostLog events plus all synced PostLog events from
// followed identities, for merged timeline construction (section 9.1).
func (s *Store) AllPosts() ([]core.SignedEvent, error) {
	own, err := s.OwnEvents(core.PostLog)
	if err != nil {
		return nil, err
	}
	identities, err := s.FollowedIdentities()
	if err != nil {
		return nil, err
	}
	for _, id := range identities {
		events, err := s.FollowedEvents(id)
		if err != nil {
			return nil, err
		}
		for _, se := range events {
			if se.Event.Log == core.PostLog {
				own = append(own, se)
			}
		}
	}
	return own, nil
}
