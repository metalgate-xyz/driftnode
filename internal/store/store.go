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
	"os"
	"path/filepath"
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
	bucketMeta       = []byte("meta")
	bucketKey        = []byte("key")        // single EncryptedKey value under key "key"
	bucketOwn        = []byte("own")        // own logs: sub-buckets per log name
	bucketCrawl      = []byte("crawl")      // remote events (synced PostLogs + crawled Profiles), keyed by author+eventID
	bucketMedia      = []byte("media")      // content-addressed blobs
	bucketRouting    = []byte("routing")    // identity -> tailcat token
	bucketVerified   = []byte("verified")   // identity -> presence (out-of-band confirmed)
	bucketFollowState = []byte("followstate") // identity -> [2]byte{followed, pinned}
	bucketTransport  = []byte("transport")  // tailcat private key, so the node's address token stays stable across restarts
	bucketCursor     = []byte("cursor")     // per-(author,log) high-water timestamp of last successful pull
)

// meta keys
var metaKeyIdentity = []byte("identity") // own identity string

// ErrPinRequiresFollow is returned by PinIdentity when the identity is not in
// the current follow graph. A pin is a property of the follow edge.
var ErrPinRequiresFollow = errors.New("can only pin a followed identity")

// followState is the two local flags cached per identity: whether it is
// followed and whether the follow is pinned. Stored as a fixed 2-byte value
// so reads and writes are single-record and the flags can't drift apart.
type followState struct {
	followed bool
	pinned   bool
}

func (f followState) encode() []byte {
	out := make([]byte, 2)
	if f.followed {
		out[0] = 1
	}
	if f.pinned {
		out[1] = 1
	}
	return out
}

func decodeFollowState(v []byte) followState {
	var f followState
	if len(v) >= 2 && v[0] != 0 {
		f.followed = true
	}
	if len(v) >= 2 && v[1] != 0 {
		f.pinned = true
	}
	return f
}

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

// DB returns the underlying bbolt database, for batch writes that the
// per-event store methods do not support.
func (s *Store) DB() *bolt.DB { return s.db }

// Path returns the filesystem path of the store.
func (s *Store) Path() string { return s.path }

func (s *Store) initBuckets() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketMeta, bucketKey, bucketOwn, bucketCrawl, bucketMedia, bucketRouting, bucketVerified, bucketFollowState, bucketTransport, bucketCursor} {
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
// target) is silently dropped: one Like per author per post.
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
		if se.Event.Kind == core.KindLike && se.Event.Like != nil {
			if _, dup := existingLikeID(se.Author, se.Event.Like.TargetID, logBucket); dup {
				return nil
			}
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

// PutCrawledEvent stores a remote event (synced PostLog or crawled Profile
// log), keyed by event ID for set-union deduplication (an event already
// present is not overwritten). Returns true if the event was newly inserted.
func (s *Store) PutCrawledEvent(se *core.SignedEvent, seq uint64) (bool, error) {
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
		fb := tx.Bucket(bucketCrawl)
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
		inserted = true
		return authorBucket.Put(id[:], eventBytes)
	})
	return inserted, err
}

// CrawledEvents returns all cached events from a remote identity.
func (s *Store) CrawledEvents(author core.Identity) ([]core.SignedEvent, error) {
	var out []core.SignedEvent
	err := s.db.View(func(tx *bolt.Tx) error {
		authorBucket := tx.Bucket(bucketCrawl).Bucket([]byte(author))
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
// identity in the shared crawl bucket. The crawler stores only Profile logs;
// PostLog events arrive via PutCrawledEvent from sync. Both share the same
// per-author dedup by event ID.
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
		authorBucket, err := tx.Bucket(bucketCrawl).CreateBucketIfNotExists([]byte(author))
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
		authorBucket := tx.Bucket(bucketCrawl).Bucket([]byte(author))
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

// DisplayName resolves the zen name (Profile.DisplayName) for an identity,
// projected last-write-wins over its Profile-log events. For the local
// identity it reads the own ProfileLog; for others it reads the crawl cache.
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
// the crawl bucket. This is the set of zens we have synced with or crawled,
// not the declared follow graph; use FollowGraph for the latter.
func (s *Store) CrawledIdentities() ([]core.Identity, error) {
	var out []core.Identity
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketCrawl).ForEach(func(k, v []byte) error {
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

// ReceivedFollowers returns the authors of Follow events targeting the local
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
	seen := make(map[core.Identity]bool)
	err = s.db.View(func(tx *bolt.Tx) error {
		fb := tx.Bucket(bucketCrawl)
		return fb.ForEach(func(authorKey, _ []byte) error {
			authorBucket := fb.Bucket(authorKey)
			if authorBucket == nil {
				return nil
			}
			return authorBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return nil
				}
				if se.Event.Kind != core.KindFollow || se.Event.Follow == nil {
					return nil
				}
				if se.Event.Follow.TargetPubkey == ownArr {
					seen[se.Author] = true
				}
				return nil
			})
		})
	})
	if err != nil {
		return nil, err
	}
	out := make([]core.Identity, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	return out, nil
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
	var out []core.SignedEvent
	err = s.db.View(func(tx *bolt.Tx) error {
		fb := tx.Bucket(bucketCrawl)
		return fb.ForEach(func(authorKey, _ []byte) error {
			authorBucket := fb.Bucket(authorKey)
			if authorBucket == nil {
				return nil
			}
			return authorBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return nil
				}
				if se.Event.Kind == core.KindFollow && se.Event.Follow != nil &&
					se.Event.Follow.TargetPubkey == ownArr {
					out = append(out, se)
				}
				return nil
			})
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
// already stored (see PutCrawledEvent).
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
// cached in the crawl bucket from non-followed zens (crawled profiles,
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
		var follows []core.Identity
		if profBucket := tx.Bucket(bucketOwn).Bucket([]byte(core.ProfileLog)); profBucket != nil {
			var profEvents []core.SignedEvent
			if err := profBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return fmt.Errorf("decode profile event at %x: %w", k, err)
				}
				profEvents = append(profEvents, se)
				return nil
			}); err != nil {
				return err
			}
			fs := core.NewLog(profEvents).FollowSet()
			fs.Each(func(target [32]byte) {
				follows = append(follows, core.IdentityFromPubkey(ed25519.PublicKey(target[:])))
			})
		}
		crawl := tx.Bucket(bucketCrawl)
		for _, id := range follows {
			authorBucket := crawl.Bucket([]byte(id))
			if authorBucket == nil {
				continue
			}
			if err := authorBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return fmt.Errorf("decode crawled event at %x: %w", k, err)
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
		authors := make(map[[32]byte]core.Identity)
		// Own PostLog.
		if ownBucket := tx.Bucket(bucketOwn).Bucket([]byte(core.PostLog)); ownBucket != nil {
			if err := ownBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return fmt.Errorf("decode own event at %x: %w", k, err)
				}
				var id [32]byte
				copy(id[:], k)
				authors[id] = se.Author
				return nil
			}); err != nil {
				return err
			}
		}
		// Followed identities' PostLogs from the crawl bucket.
		// First resolve the follow graph from the own ProfileLog.
		var profEvents []core.SignedEvent
		if profBucket := tx.Bucket(bucketOwn).Bucket([]byte(core.ProfileLog)); profBucket != nil {
			if err := profBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return fmt.Errorf("decode profile event at %x: %w", k, err)
				}
				profEvents = append(profEvents, se)
				return nil
			}); err != nil {
				return err
			}
		}
		fs := core.NewLog(profEvents).FollowSet()
		fs.Each(func(target [32]byte) {
			id := core.IdentityFromPubkey(ed25519.PublicKey(target[:]))
			authorBucket := tx.Bucket(bucketCrawl).Bucket([]byte(id))
			if authorBucket == nil {
				return
			}
			_ = authorBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return nil
				}
				if se.Event.Log != core.PostLog {
					return nil
				}
				var evID [32]byte
				copy(evID[:], k)
				authors[evID] = se.Author
				return nil
			})
		})
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
		authors := make(map[[32]byte]core.Identity)
		if ownBucket := tx.Bucket(bucketOwn).Bucket([]byte(core.PostLog)); ownBucket != nil {
			if err := ownBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return fmt.Errorf("decode own event at %x: %w", k, err)
				}
				var id [32]byte
				copy(id[:], k)
				authors[id] = se.Author
				return nil
			}); err != nil {
				return err
			}
		}
		var profEvents []core.SignedEvent
		if profBucket := tx.Bucket(bucketOwn).Bucket([]byte(core.ProfileLog)); profBucket != nil {
			if err := profBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return fmt.Errorf("decode profile event at %x: %w", k, err)
				}
				profEvents = append(profEvents, se)
				return nil
			}); err != nil {
				return err
			}
		}
		fs := core.NewLog(profEvents).FollowSet()
		fs.Each(func(target [32]byte) {
			id := core.IdentityFromPubkey(ed25519.PublicKey(target[:]))
			authorBucket := tx.Bucket(bucketCrawl).Bucket([]byte(id))
			if authorBucket == nil {
				return
			}
			_ = authorBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return nil
				}
				if se.Event.Log != core.PostLog {
					return nil
				}
				var evID [32]byte
				copy(evID[:], k)
				authors[evID] = se.Author
				return nil
			})
		})
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
					out = append(out, se)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		// All crawled PostLogs.
		crawl := tx.Bucket(bucketCrawl)
		if crawl == nil {
			return nil
		}
		return crawl.ForEach(func(author, _ []byte) error {
			authorBucket := crawl.Bucket(author)
			if authorBucket == nil {
				return nil
			}
			return authorBucket.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return nil
				}
				if se.Event.Kind == core.KindLike && se.Event.Like != nil && wanted[se.Event.Like.TargetID] {
					out = append(out, se)
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

// putFollowState writes the cached follow+pin flags for an identity.
func (s *Store) putFollowState(id core.Identity, st followState) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFollowState).Put([]byte(id), st.encode())
	})
}

// deleteFollowState removes the cached follow+pin flags for an identity,
// clearing both the follow edge and any pin on it.
func (s *Store) deleteFollowState(id core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFollowState).Delete([]byte(id))
	})
}

// getFollowState reads the cached follow+pin flags for an identity.
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
