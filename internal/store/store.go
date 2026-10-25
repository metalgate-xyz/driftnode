// Package store is the bbolt-backed local store for driftnode. It holds
// the account's encrypted keypair, its own Profile log and PostLog, followed
// accounts' synced PostLogs (re-fetchable cache), and the media cache.
// Everything is accessed offline, with no daemon or network (§13 step 2).
package store

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"driftnode/internal/core"
	"driftnode/internal/weight"

	bolt "go.etcd.io/bbolt"
)

// Bucket names. Own events are keyed by event ID (BLAKE3-256 of the canonical
// signed bytes) within per-log buckets, so backup import merges as a set
// union of immutable events. Followed events are keyed by event ID within
// per-author buckets for the same dedup property. Routing maps a followed
// identity to the tailcat token currently used to reach it. Verified holds
// identities the user has confirmed out-of-band (§7), so a later connection
// under a different identity can be flagged as a possible MITM. FollowState
// is a local identity-keyed cache of the follow graph plus per-follow pin
// flags: followed=1 when the identity is in the follow graph (derived from
// the signed, synced ProfileLog), pinned=1 when the user has marked the
// follow for top sync priority. It is rebuilt from the log on backup restore.
var (
	bucketMeta        = []byte("meta")
	bucketKey         = []byte("key")         // single EncryptedKey value under key "key"
	bucketOwn         = []byte("own")         // own logs: sub-buckets per log name
	bucketRemote      = []byte("remote")      // remote events (synced PostLogs + crawled Profiles), keyed by author+eventID
	bucketMedia       = []byte("media")       // content-addressed blobs
	bucketRouting     = []byte("routing")     // identity -> tailcat token
	bucketVerified    = []byte("verified")    // identity -> presence (out-of-band confirmed)
	bucketFollowState = []byte("followstate") // identity -> [3]byte{followed, pinned, flagged}
	bucketTransport   = []byte("transport")   // tailcat private key, so the node's address token stays stable across restarts
	bucketCursor      = []byte("cursor")      // per-(author,log) high-water timestamp of last successful pull
	bucketWeight      = []byte("weight")      // identity -> float64 sync-priority weight, recomputed each sync round
	bucketRateSecond  = []byte("ratesecond")  // per-(author,kind) last event second, for same-second rate check
	bucketEventIndex  = []byte("eventindex")  // EventID -> author Identity + kind byte; covers every stored event
	bucketEventRef    = []byte("eventref")    // composite TargetEventID+SourceEventID -> source author + kind byte
	bucketCrawlLRU    = []byte("crawllru")    // composite int64BE(touchedNs)+Identity -> presence; crawl cache LRU
	bucketCrawlTS     = []byte("crawlts")     // Identity -> int64BE last-touched ns; reverse index for crawl LRU touch
	bucketTokenLRU    = []byte("tokenlru")    // composite int64BE(touchedNs)+token -> presence; discovered-token LRU
	bucketTokenTS     = []byte("tokents")     // token -> int64BE last-touched ns; reverse index for token LRU touch
	bucketBootstrapTk = []byte("bootstraptk") // discovered tokens learned from the bootstrap file; never evicted
	bucketSeq         = []byte("seq")         // per-log monotonic sequence counter (uint64 BE), never decreases
)

// meta keys
var metaKeyIdentity = []byte("identity") // own identity string

// ErrPinRequiresFollow is returned by PinIdentity when the identity is not in
// the current follow graph. A pin is a property of the follow edge.
var ErrPinRequiresFollow = errors.New("can only pin a followed identity")

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

// Store is the local persistent store. It wraps a bbolt DB.
type Store struct {
	db    *bolt.DB
	path  string
	posts *postQueue

	// followersNeedsRebuild flags that the follower map and its count may
	// be stale: set at open (the map is derived, not trusted from disk)
	// and whenever an author bucket is deleted wholesale by crawl eviction.
	// Only cleared inside the write transaction that recomputed the map,
	// so the flag decision serializes with every ingest.
	followersNeedsRebuild atomic.Bool

	// retention is the PostLog retention window. A non-zero value drops
	// remote PostLog events older than now-retention at ingest and prunes
	// aged events from the remote bucket. Zero retains everything (the
	// default).
	retention time.Duration
	// now is the wall-clock source as unix nanoseconds. Tests override it
	// to control retention and future-skew checks. nil means core.Now64.
	now func() int64
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
	s := &Store{db: db, path: path, retention: retentionFromEnv()}
	s.followersNeedsRebuild.Store(true)
	if err := s.initBuckets(); err != nil {
		db.Close()
		return nil, err
	}
	s.posts = newPostQueue(s)
	return s, nil
}

// retentionFromEnv reads the PostLog retention window from the
// DRIFTNODE_RETENTION env var as a Go duration (e.g. "720h" for 30 days).
// An empty or unparsable value disables retention (retain everything).
func retentionFromEnv() time.Duration {
	v := os.Getenv("DRIFTNODE_RETENTION")
	if v == "" {
		return 0
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}

// SetRetention sets the PostLog retention window, overriding the env var.
// A non-zero value drops and prunes remote PostLog events older than the
// window; zero retains everything.
func (s *Store) SetRetention(d time.Duration) { s.retention = d }

// Retention returns the configured PostLog retention window.
func (s *Store) Retention() time.Duration { return s.retention }

// SetNow sets the wall-clock source for tests. The function must return
// unix nanoseconds.
func (s *Store) SetNow(fn func() int64) { s.now = fn }

// now64 returns the current wall clock as unix nanoseconds.
func (s *Store) now64() int64 {
	if s.now != nil {
		return s.now()
	}
	return core.Now64()
}

// Close closes the underlying database.
func (s *Store) Close() error {
	if s.posts != nil {
		s.posts.Stop()
	}
	return s.db.Close()
}

// DB returns the underlying bbolt database, for batch writes that the
// per-event store methods do not support.
func (s *Store) DB() *bolt.DB { return s.db }

// Path returns the filesystem path of the store.
func (s *Store) Path() string { return s.path }

func (s *Store) initBuckets() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketMeta, bucketKey, bucketOwn, bucketRemote, bucketMedia, bucketRouting, bucketVerified, bucketFollowState, bucketTransport, bucketCursor, bucketWeight, bucketRateSecond, bucketEventIndex, bucketEventRef, bucketCrawlLRU, bucketCrawlTS, bucketTokenLRU, bucketTokenTS, bucketBootstrapTk, bucketSeq} {
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
	verified, err := s.VerifiedIdentities()
	if err != nil {
		return nil, err
	}
	pinned, err := s.PinnedIdentities()
	if err != nil {
		return nil, err
	}
	return &core.BackupData{Key: ek, OwnLogs: events, Verified: verified, Pinned: pinned}, nil
}

// ImportBackup merges a backup into the store: replaces the keypair (via the
// CLI's separate PutKey call after decryption) and merges own logs as a set
// union of signed events (§8). AppendOwnEvent dedups by event ID, so this is
// idempotent. Verified identities replace the local set. The follow-state
// cache is rebuilt from the restored ProfileLog, then the backup's pins are
// applied onto the rebuilt follow edges (a pin requires a follow).
func (s *Store) ImportBackup(bd *core.BackupData) error {
	if bd.Key == nil {
		return errors.New("backup missing key")
	}
	for _, se := range bd.OwnLogs {
		if err := s.AppendOwnEvent(se.Event.Log, &se); err != nil {
			return err
		}
	}
	if bd.Verified != nil {
		if err := s.PutVerifiedIdentities(bd.Verified); err != nil {
			return fmt.Errorf("restore verified: %w", err)
		}
	}
	if err := s.rebuildFollowState(); err != nil {
		return fmt.Errorf("rebuild follow state: %w", err)
	}
	if bd.Pinned != nil {
		if err := s.PutPinnedIdentities(bd.Pinned); err != nil {
			return fmt.Errorf("restore pinned: %w", err)
		}
	}
	return nil
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

// FollowGraph returns the identities this node currently follows, derived by
// replaying its own ProfileLog Follow/Unfollow events (section 7.1). This is
// the authoritative auto-dial set: a zen is dialed while it is followed and
// its routing token is bound.
func (s *Store) FollowGraph() ([]core.Identity, error) {
	events, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		return nil, err
	}
	fs := core.NewLog(events).FollowSet()
	var out []core.Identity
	fs.Each(func(target [32]byte) {
		out = append(out, core.IdentityFromPubkey(ed25519.PublicKey(target[:])))
	})
	return out, nil
}

// RecomputeWeights regenerates the per-follow sync-priority weight for every
// identity in the current follow graph and persists it. It gathers all four
// interaction metrics in a single read transaction, then runs the weight
// formula and writes the result. Weights are recomputed each sync round so
// the graph reflects current interaction history; sync priority itself is
// applied in phase 6.
func (s *Store) RecomputeWeights() (map[core.Identity]float64, error) {
	in, err := s.gatherWeightInputs()
	if err != nil {
		return nil, fmt.Errorf("gather weight inputs: %w", err)
	}
	weights := weight.Weights(*in)
	if err := s.persistWeights(weights); err != nil {
		return nil, fmt.Errorf("persist weights: %w", err)
	}
	return weights, nil
}

// Weight returns the persisted sync-priority weight for an identity, or 0 if
// no weight is recorded (a not-followed identity, or a follow whose weight
// has not yet been recomputed this round).
func (s *Store) Weight(id core.Identity) (float64, error) {
	var raw []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		raw = tx.Bucket(bucketWeight).Get([]byte(id))
		return nil
	})
	if err != nil {
		return 0, err
	}
	return decodeWeight(raw), nil
}

// WeightedGraph returns the persisted weights for every followed identity,
// the weighted form of FollowGraph used by the phase-6 scheduler. Identities
// that somehow lack a persisted weight are absent; RecomputeWeights is the
// writer that keeps this set complete.
func (s *Store) WeightedGraph() (map[core.Identity]float64, error) {
	out := make(map[core.Identity]float64)
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketWeight).ForEach(func(k, v []byte) error {
			out[core.Identity(k)] = decodeWeight(v)
			return nil
		})
	})
	return out, err
}

// gatherWeightInputs builds the weight.Inputs for the current follow graph
// in a single read transaction: the follow set, outbound like/reply counts
// per followed author, the reciprocal (they-follow-me) set, and the pin set.
// Likes and replies are scoped to followed authors by resolving own Like
// target IDs and reply parent IDs against an event-ID-to-author index built
// over all held PostLogs, matching OutboundLikes and OutboundReplies.
func (s *Store) gatherWeightInputs() (*weight.Inputs, error) {
	ownID, ok, err := s.Identity()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("no local identity set")
	}
	ownPub, err := ownID.PubkeyBytes()
	if err != nil {
		return nil, fmt.Errorf("own identity: %w", err)
	}
	var ownArr [32]byte
	copy(ownArr[:], ownPub)

	var follows []core.Identity
	ownLikes := []core.EventID{}   // target IDs of own Like events
	ownReplies := []core.EventID{} // parent IDs of own reply Posts
	authors := make(map[core.EventID]core.Identity)
	recip := make(map[core.Identity]bool)
	pins := make(map[core.Identity]bool)

	err = s.db.View(func(tx *bolt.Tx) error {
		own := tx.Bucket(bucketOwn)
		if own == nil {
			return errors.New("own bucket missing")
		}

		// Follow set and pins from the own ProfileLog and follow-state cache.
		follows, err = followGraphIdentities(tx)
		if err != nil {
			return err
		}
		followSet := make(map[string]bool)
		for _, id := range follows {
			followSet[id.String()] = true
		}
		if fb := tx.Bucket(bucketFollowState); fb != nil {
			_ = fb.ForEach(func(k, v []byte) error {
				if decodeFollowState(v).pinned {
					pins[core.Identity(k)] = true
				}
				return nil
			})
		}

		// Own PostLog: collect like targets and reply parents, and seed the
		// event-ID-to-author index with own posts.
		if ob := own.Bucket([]byte(core.PostLog)); ob != nil {
			if err := ob.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return nil
				}
				var id core.EventID
				copy(id[:], k)
				authors[id] = se.Author
				if se.Event.Kind == core.KindLike && se.Event.Like != nil {
					ownLikes = append(ownLikes, se.Event.Like.TargetID)
				}
				if se.Event.Post != nil && !se.Event.Post.ParentID.IsZero() {
					ownReplies = append(ownReplies, se.Event.Post.ParentID)
				}
				return nil
			}); err != nil {
				return err
			}
		}

		// Remote bucket: extend the author index with followed authors'
		// posts, and record reciprocal follows (Follow events targeting
		// the local identity).
		remote := tx.Bucket(bucketRemote)
		if remote == nil {
			return nil
		}
		return remote.ForEach(func(authorKey, _ []byte) error {
			authorID := core.Identity(authorKey)
			isFollowed := followSet[authorID.String()]
			ab := remote.Bucket(authorKey)
			if ab == nil {
				return nil
			}
			return ab.ForEach(func(k, v []byte) error {
				if !isEventKey(k) {
					return nil
				}
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return nil
				}
				if se.Event.Log == core.PostLog && isFollowed {
					var id core.EventID
					copy(id[:], k)
					authors[id] = se.Author
				}
				if se.Event.Kind == core.KindFollow && se.Event.Follow != nil &&
					se.Event.Follow.TargetPubkey == ownArr {
					recip[authorID] = true
				}
				return nil
			})
		})
	})
	if err != nil {
		return nil, err
	}

	// Resolve own likes and replies to per-author counts.
	likeCounts := make(map[core.Identity]int)
	for _, tid := range ownLikes {
		if author, ok := authors[tid]; ok {
			likeCounts[author]++
		}
	}
	replyCounts := make(map[core.Identity]int)
	for _, pid := range ownReplies {
		if author, ok := authors[pid]; ok {
			replyCounts[author]++
		}
	}

	return &weight.Inputs{
		Follows: follows,
		Likes:   likeCounts,
		Replies: replyCounts,
		Recip:   recip,
		Pins:    pins,
	}, nil
}

// persistWeights writes the weight map to the weight bucket, replacing the
// previous round's values. A followed identity with no weight entry is not
// pruned here; the bucket is a cache rebuilt wholesale each round.
func (s *Store) persistWeights(weights map[core.Identity]float64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketWeight)
		if err := b.ForEach(func(k, v []byte) error { return b.Delete(k) }); err != nil {
			return err
		}
		for id, w := range weights {
			if err := b.Put([]byte(id), encodeWeight(w)); err != nil {
				return err
			}
		}
		return nil
	})
}

// encodeWeight stores a float64 weight as its IEEE-754 big-endian bytes.
func encodeWeight(w float64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(w))
	return buf[:]
}

// decodeWeight reads a persisted float64 weight. A missing or short value is
// 0, so a never-recomputed follow reads as the lowest priority.
func decodeWeight(raw []byte) float64 {
	if len(raw) < 8 {
		return 0
	}
	return math.Float64frombits(binary.BigEndian.Uint64(raw))
}

// identity that this zen has received (section 9.3). A follower's Follow
// event lives in the follower's own Profile log and arrives at the followed
// zen during a normal bidirectional sync, so the followed zen derives its
// follower set from events it has personally received rather than from a
// crawl of the network. The result is this zen's own observed follower set,
// not a global truth (the same caveat as Like counts, section 7.2).
func (s *Store) ReceivedFollowers() ([]core.Identity, error) {
	ownID, ok, err := s.Identity()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("no local identity set")
	}
	ownPub, err := ownID.PubkeyBytes()
	if err != nil {
		return nil, fmt.Errorf("own identity: %w", err)
	}
	var ownArr [32]byte
	copy(ownArr[:], ownPub)
	if err := s.ensureFollowersReady(); err != nil {
		return nil, err
	}
	// Follower-map read: one point lookup per tracked author bucket, so
	// the cost grows with the number of authors, not with stored events.
	key := followMapKey(ownArr)
	var out []core.Identity
	err = s.db.View(func(tx *bolt.Tx) error {
		fb := tx.Bucket(bucketRemote)
		return fb.ForEach(func(authorKey, _ []byte) error {
			authorBucket := fb.Bucket(authorKey)
			if authorBucket == nil {
				return nil
			}
			if authorBucket.Get(key) != nil {
				out = append(out, core.Identity(authorKey))
			}
			return nil
		})
	})
	return out, err
}

// ReceivedFollowEvents returns the Follow events targeting the local
// identity that this zen has received, keyed by event ID for the wire
// response. Each event is self-certifying: the follower's signature is
// verifiable by the receiver without trusting this zen, so a relay cannot
// forge a follower.
func (s *Store) ReceivedFollowEvents() ([]core.SignedEvent, error) {
	ownID, ok, err := s.Identity()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("no local identity set")
	}
	ownPub, err := ownID.PubkeyBytes()
	if err != nil {
		return nil, fmt.Errorf("own identity: %w", err)
	}
	var ownArr [32]byte
	copy(ownArr[:], ownPub)
	if err := s.ensureFollowersReady(); err != nil {
		return nil, err
	}
	// Follower-map read: the map record holds the winning Follow event's
	// ID, so the evidence bytes are fetched by point lookup per follower.
	key := followMapKey(ownArr)
	var out []core.SignedEvent
	err = s.db.View(func(tx *bolt.Tx) error {
		fb := tx.Bucket(bucketRemote)
		return fb.ForEach(func(authorKey, _ []byte) error {
			authorBucket := fb.Bucket(authorKey)
			if authorBucket == nil {
				return nil
			}
			raw := authorBucket.Get(key)
			if raw == nil {
				return nil
			}
			var evID [32]byte
			copy(evID[:], raw)
			evBytes := authorBucket.Get(evID[:])
			if evBytes == nil {
				return nil
			}
			var se core.SignedEvent
			if err := core.CanonicalDecode(evBytes, &se); err != nil {
				return nil
			}
			out = append(out, se)
			return nil
		})
	})
	return out, err
}

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

// PutTransportKey stores the tailcat private key bytes, so the node's address
// token stays stable across restarts (section 5.2). The bytes are the JSON
// serialization of tailcat.PrivateKey.
func (s *Store) PutTransportKey(keyBytes []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTransport).Put([]byte("tailcat"), keyBytes)
	})
}

// TransportKey returns the stored tailcat private key bytes, or false if no
// key has been persisted yet.
func (s *Store) TransportKey() ([]byte, bool, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		data = tx.Bucket(bucketTransport).Get([]byte("tailcat"))
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if data == nil {
		return nil, false, nil
	}
	return append([]byte(nil), data...), true, nil
}

// DeleteTransportKey removes the stored tailcat private key.
func (s *Store) DeleteTransportKey() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTransport).Delete([]byte("tailcat"))
	})
}

// AllPosts returns own PostLog events plus all synced PostLog events from
// followed identities, for merged timeline construction (section 9.1). Only
// identities in the declared follow graph (FollowGraph) contribute; events
// cached in the remote bucket from non-followed zens (crawled profiles,
// probed-but-not-followed tokens) are excluded so the feed matches who the
// user actually follows.
func (s *Store) AllPosts() ([]core.SignedEvent, error) {
	own, err := s.OwnEvents(core.PostLog)
	if err != nil {
		return nil, err
	}
	identities, err := s.FollowGraph()
	if err != nil {
		return nil, err
	}
	for _, id := range identities {
		events, err := s.CrawledEvents(id)
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

// AllPostsOneTx is AllPosts but reads own + all crawled PostLog events in a
// single bbolt transaction. AllPosts opens one transaction per followed
// identity (thousands for a large follow graph), which dominates feed-build
// time; this collapses them into one read transaction.
func (s *Store) AllPostsOneTx() ([]core.SignedEvent, error) {
	var out []core.SignedEvent
	err := s.db.View(func(tx *bolt.Tx) error {
		// Own PostLog.
		if ownBucket := tx.Bucket(bucketOwn).Bucket([]byte(core.PostLog)); ownBucket != nil {
			if err := ownBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return fmt.Errorf("decode own event at %x: %w", k, err)
				}
				out = append(out, se)
				return nil
			}); err != nil {
				return err
			}
		}
		// Follow graph from own ProfileLog, then each followed identity's
		// crawled PostLog events, all within this transaction.
		follows, err := followGraphIdentities(tx)
		if err != nil {
			return err
		}
		remote := tx.Bucket(bucketRemote)
		for _, id := range follows {
			authorBucket := remote.Bucket([]byte(id))
			if authorBucket == nil {
				continue
			}
			if err := authorBucket.ForEach(func(k, v []byte) error {
				if !isEventKey(k) {
					return nil
				}
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return fmt.Errorf("decode remote event at %x: %w", k, err)
				}
				if se.Event.Log == core.PostLog {
					out = append(out, se)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

// OutboundReplies counts, per author, how many of the user's own posts are
// replies to that author's posts. A reply is a Post event with a non-zero
// ParentID; the parent's author is resolved by looking up the parent event
// ID across all held PostLogs (own plus followed). Posts whose parent is not
// held locally are not counted: the parent author is unknown until the
// referenced PostLog has been synced. This is an interaction metric feeding
// the weighted follow graph.
func (s *Store) OutboundReplies() (map[core.Identity]int, error) {
	out := make(map[core.Identity]int)
	err := s.db.View(func(tx *bolt.Tx) error {
		// Build an event ID -> author index over all held PostLogs so a
		// reply's parent can be resolved to an author in one pass.
		follows, err := followGraphIdentities(tx)
		if err != nil {
			return err
		}
		authors := postAuthorIndex(tx, follows)
		// Count the user's own replies grouped by the parent's author.
		if ownBucket := tx.Bucket(bucketOwn).Bucket([]byte(core.PostLog)); ownBucket != nil {
			if err := ownBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return fmt.Errorf("decode own event at %x: %w", k, err)
				}
				if se.Event.Post == nil || se.Event.Post.ParentID.IsZero() {
					return nil
				}
				parent := se.Event.Post.ParentID
				author, ok := authors[parent]
				if !ok {
					return nil
				}
				out[author]++
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

// OutboundLikes computes how many Like events the user has signed targeting
// each followed author's posts, by resolving Like.target_id to the liked
// post's author across held PostLogs. Counts only authors in the current
// follow graph. An interaction metric feeding the weighted follow graph.
func (s *Store) OutboundLikes() (map[core.Identity]int, error) {
	out := make(map[core.Identity]int)
	err := s.db.View(func(tx *bolt.Tx) error {
		// Build an event ID -> author index over all held PostLogs so a
		// like's target can be resolved to an author in one pass.
		follows, err := followGraphIdentities(tx)
		if err != nil {
			return err
		}
		authors := postAuthorIndex(tx, follows)
		// Count the user's own likes grouped by the liked post's author.
		if ownBucket := tx.Bucket(bucketOwn).Bucket([]byte(core.PostLog)); ownBucket != nil {
			if err := ownBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return fmt.Errorf("decode own event at %x: %w", k, err)
				}
				if se.Event.Kind != core.KindLike || se.Event.Like == nil {
					return nil
				}
				author, ok := authors[se.Event.Like.TargetID]
				if !ok {
					return nil
				}
				out[author]++
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

// LikesForPosts returns Like events held locally whose target_id matches any
// of the given event IDs. Scans own PostLog and all crawled PostLogs. Used by
// the on-demand like fetch: a zen asks "do you have likes for these posts?"
// and this returns whatever it holds. Results are best-effort, not
// authoritative.
func (s *Store) LikesForPosts(targetIDs []core.EventID) ([]core.SignedEvent, error) {
	wanted := make(map[[32]byte]bool, len(targetIDs))
	for _, id := range targetIDs {
		wanted[id] = true
	}
	var out []core.SignedEvent
	err := s.db.View(func(tx *bolt.Tx) error {
		// Own PostLog.
		if ownBucket := tx.Bucket(bucketOwn).Bucket([]byte(core.PostLog)); ownBucket != nil {
			if err := ownBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return fmt.Errorf("decode own event at %x: %w", k, err)
				}
				if se.Event.Kind == core.KindLike && se.Event.Like != nil && wanted[se.Event.Like.TargetID] {
					var evID core.EventID
					copy(evID[:], k)
					if _, _, exists := eventExists(tx, evID); exists {
						out = append(out, se)
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		// All remote PostLogs.
		remote := tx.Bucket(bucketRemote)
		if remote == nil {
			return nil
		}
		return remote.ForEach(func(author, _ []byte) error {
			authorBucket := remote.Bucket(author)
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
				if se.Event.Kind == core.KindLike && se.Event.Like != nil && wanted[se.Event.Like.TargetID] {
					var evID core.EventID
					copy(evID[:], k)
					if _, _, exists := eventExists(tx, evID); exists {
						out = append(out, se)
					}
				}
				return nil
			})
		})
	})
	return out, err
}

// VerifyIdentity records that the user has confirmed an identity
// out-of-band (compared the full driftnode:<pubkey> string through a trusted
// channel). Verified identities are backup-critical trust state: losing them
// would silently downgrade a confirmed peer to unverified on restore.
func (s *Store) VerifyIdentity(id core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketVerified).Put([]byte(id), []byte{1})
	})
}

// UnverifyIdentity removes an out-of-band confirmation.
func (s *Store) UnverifyIdentity(id core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketVerified).Delete([]byte(id))
	})
}

// IsVerified reports whether an identity was confirmed out-of-band.
func (s *Store) IsVerified(id core.Identity) (bool, error) {
	var present bool
	err := s.db.View(func(tx *bolt.Tx) error {
		present = tx.Bucket(bucketVerified).Get([]byte(id)) != nil
		return nil
	})
	return present, err
}

// VerifiedIdentities returns every identity the user has confirmed out-of-band,
// used for backup export and for the zens view.
func (s *Store) VerifiedIdentities() ([]core.Identity, error) {
	var out []core.Identity
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketVerified).ForEach(func(k, v []byte) error {
			out = append(out, core.Identity(k))
			return nil
		})
	})
	return out, err
}

// PutVerifiedIdentities replaces the verified set, used by backup import.
func (s *Store) PutVerifiedIdentities(ids []core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketVerified)
		if err := b.ForEach(func(k, v []byte) error { return b.Delete(k) }); err != nil {
			return err
		}
		for _, id := range ids {
			if err := b.Put([]byte(id), []byte{1}); err != nil {
				return err
			}
		}
		return nil
	})
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
