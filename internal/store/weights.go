package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"driftnode/internal/core"
	"driftnode/internal/weight"

	bolt "go.etcd.io/bbolt"
)

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
// over all held PostLogs.
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
