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
	"sync/atomic"
	"time"

	"driftnode/internal/core"

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
