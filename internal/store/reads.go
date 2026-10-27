package store

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// AllPostsOneTx returns own PostLog events plus all synced PostLog events
// from followed identities, read in a single bbolt transaction, for merged
// timeline construction (section 9.1). Only identities in the declared
// follow graph (FollowGraph) contribute; events cached in the remote bucket
// from non-followed zens (crawled profiles, probed-but-not-followed tokens)
// are excluded so the feed matches who the user actually follows. The one
// transaction matters: one transaction per followed identity dominates
// feed-build time on a large follow graph.
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

// LikesForPosts returns Like events held locally whose target_id matches any
// of the given event IDs. The reference index (bucketEventRef), maintained at
// ingest and through cascade delete and prune, maps each target to the
// events referencing it, so the lookup touches only the requested targets
// and their referencing events. Used by the on-demand like fetch: a zen asks
// "do you have likes for these posts?" and this returns whatever it holds.
// Results are best-effort, not authoritative; their order is unspecified.
func (s *Store) LikesForPosts(targetIDs []core.EventID) ([]core.SignedEvent, error) {
	wanted := make(map[core.EventID]bool, len(targetIDs))
	for _, id := range targetIDs {
		wanted[id] = true
	}
	var out []core.SignedEvent
	err := s.db.View(func(tx *bolt.Tx) error {
		ownPub, err := ownPubkeyInTx(tx)
		if err != nil {
			return err
		}
		ownID := core.IdentityFromPubkey(ed25519.PublicKey(ownPub[:]))
		remote := tx.Bucket(bucketRemote)
		for target := range wanted {
			refs, err := refsByTarget(tx, target)
			if err != nil {
				return err
			}
			for _, ref := range refs {
				if ref.kind != core.KindLike {
					continue
				}
				var evBytes []byte
				if ref.author == ownID {
					if lb := tx.Bucket(bucketOwn).Bucket([]byte(core.PostLog)); lb != nil {
						evBytes = lb.Get(ref.sourceID[:])
					}
				} else if ab := remote.Bucket([]byte(ref.author)); ab != nil {
					evBytes = ab.Get(ref.sourceID[:])
				}
				if evBytes == nil {
					continue
				}
				var se core.SignedEvent
				if err := core.CanonicalDecode(evBytes, &se); err != nil {
					continue
				}
				if se.Event.Kind != core.KindLike || se.Event.Like == nil || se.Event.Like.TargetID != target {
					continue
				}
				out = append(out, se)
			}
		}
		return nil
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

// ReceivedFollowers returns the identities that follow this zen, from Follow events
// that this zen has received (section 9.3). A follower's Follow
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
