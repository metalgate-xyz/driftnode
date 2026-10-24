package store

import (
	"crypto/ed25519"
	"encoding/binary"
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
			if len(k) != 32 {
				return nil
			}
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

// nextSequence returns the next value for the per-log monotonic sequence
// counter, advancing it in place. The counter never decreases, even when
// events are removed from the own log by tidy-at-write: deriving Sequence
// from key count would produce a lower or colliding number after a removal.
// On first use, the counter is seeded from the current key count so existing
// logs continue from the same numbering.
func nextSequence(tx *bolt.Tx, log core.LogName) (uint64, error) {
	b := tx.Bucket(bucketSeq)
	key := []byte(log)
	raw := b.Get(key)
	var cur uint64
	if raw != nil {
		cur = binary.BigEndian.Uint64(raw)
	} else {
		// Seed from the current own-log key count so a pre-existing log
		// continues from its highest sequence, not from 1.
		if lb := tx.Bucket(bucketOwn).Bucket(key); lb != nil {
			cur = uint64(lb.Stats().KeyN)
		}
	}
	cur++
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], cur)
	if err := b.Put(key, buf[:]); err != nil {
		return 0, fmt.Errorf("put sequence: %w", err)
	}
	return cur, nil
}
