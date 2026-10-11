package store

import (
	"crypto/ed25519"
	"fmt"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// authorKindKey packs the author identity and event kind into one byte
// sequence: identity bytes followed by the kind byte. Used as a composite
// bucket key (bucketRateSecond) and as an index value (bucketEventIndex,
// bucketEventRef).
func authorKindKey(author core.Identity, kind core.Kind) []byte {
	v := make([]byte, 0, len(author)+1)
	v = append(v, []byte(author)...)
	v = append(v, byte(kind))
	return v
}

// decodeAuthorKind unpacks an author identity and kind from a value produced
// by authorKindKey.
func decodeAuthorKind(v []byte) (core.Identity, core.Kind, bool) {
	if len(v) < 1 {
		return "", 0, false
	}
	author := core.Identity(v[:len(v)-1])
	kind := core.Kind(v[len(v)-1])
	return author, kind, true
}

// followGraphIdentities returns the identities this node currently follows,
// decoded from its own ProfileLog within tx. Equivalent to Store.FollowGraph
// but usable inside an existing bbolt transaction.
func followGraphIdentities(tx *bolt.Tx) ([]core.Identity, error) {
	var profEvents []core.SignedEvent
	if pb := tx.Bucket(bucketOwn).Bucket([]byte(core.ProfileLog)); pb != nil {
		if err := pb.ForEach(func(k, v []byte) error {
			var se core.SignedEvent
			if err := core.CanonicalDecode(v, &se); err != nil {
				return fmt.Errorf("decode profile event at %x: %w", k, err)
			}
			profEvents = append(profEvents, se)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	fs := core.NewLog(profEvents).FollowSet()
	var out []core.Identity
	fs.Each(func(target [32]byte) {
		out = append(out, core.IdentityFromPubkey(ed25519.PublicKey(target[:])))
	})
	return out, nil
}

// postAuthorIndex maps every held PostLog event ID (own plus followed
// identities' remote posts) to its author, within tx. Used to resolve a
// reply parent or like target to an author in one pass.
func postAuthorIndex(tx *bolt.Tx, follows []core.Identity) map[core.EventID]core.Identity {
	authors := make(map[core.EventID]core.Identity)
	if ownBucket := tx.Bucket(bucketOwn).Bucket([]byte(core.PostLog)); ownBucket != nil {
		_ = ownBucket.ForEach(func(k, v []byte) error {
			var se core.SignedEvent
			if err := core.CanonicalDecode(v, &se); err != nil {
				return fmt.Errorf("decode own event at %x: %w", k, err)
			}
			var id core.EventID
			copy(id[:], k)
			authors[id] = se.Author
			return nil
		})
	}
	remote := tx.Bucket(bucketRemote)
	for _, id := range follows {
		authorBucket := remote.Bucket([]byte(id))
		if authorBucket == nil {
			continue
		}
		_ = authorBucket.ForEach(func(k, v []byte) error {
			var se core.SignedEvent
			if err := core.CanonicalDecode(v, &se); err != nil {
				return nil
			}
			if se.Event.Log != core.PostLog {
				return nil
			}
			var evID core.EventID
			copy(evID[:], k)
			authors[evID] = se.Author
			return nil
		})
	}
	return authors
}
