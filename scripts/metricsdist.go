//go:build ignore

// metricsdist computes and reports the distribution of the weighted-follow
// interaction metrics (outbound likes, outbound replies, reciprocal follow,
// pins) for a generated driftnode store. It reads the store in a SINGLE pass:
// one bbolt read transaction builds the event-ID->author index once and
// derives all four metrics from it, instead of calling the store's
// OutboundLikes/OutboundReplies/ReceivedFollowers methods, which each
// independently re-scan the crawl bucket and rebuild that index.
//
// Usage:
//
//	go run scripts/metricsdist.go -db data/metrics.db
package main

import (
	"crypto/ed25519"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

func main() {
	dbPath := flag.String("db", "data/test.db", "path to the bbolt store")
	flag.Parse()

	db, err := bolt.Open(*dbPath, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		failf("open db: %v", err)
	}
	defer db.Close()

	// Single read transaction: derive follow set, own events, crawl index,
	// and all four metrics without re-scanning.
	var follows []core.Identity
	var ownLikes []core.EventID  // target IDs of own Like events
	var ownReplies []core.EventID // parent IDs of own reply Posts
	authors := make(map[core.EventID]core.Identity) // event ID -> author, over all held PostLogs
	recip := make(map[string]int)                    // followed author -> 1 if they follow back
	pinSet := make(map[string]bool)

	ownID, ok, err := readMetaIdentity(db)
	if err != nil {
		failf("read meta identity: %v", err)
	}
	if !ok {
		fail("no local identity in meta bucket")
	}
	ownPub, err := ownID.PubkeyBytes()
	if err != nil {
		failf("own pubkey: %v", err)
	}
	var ownArr [32]byte
	copy(ownArr[:], ownPub)

	err = db.View(func(tx *bolt.Tx) error {
		own := tx.Bucket([]byte("own"))
		if own == nil {
			return fmt.Errorf("own bucket missing")
		}

		// Follow set from own ProfileLog.
		var profEvents []core.SignedEvent
		if pb := own.Bucket([]byte(core.ProfileLog)); pb != nil {
			if err := pb.ForEach(func(k, v []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(v, &se); err != nil {
					return nil
				}
				profEvents = append(profEvents, se)
				return nil
			}); err != nil {
				return err
			}
		}
		fs := core.NewLog(profEvents).FollowSet()
		followSet := make(map[string]bool)
		fs.Each(func(target [32]byte) {
			id := core.IdentityFromPubkey(ed25519.PublicKey(target[:]))
			follows = append(follows, id)
			followSet[id.String()] = true
		})

		// Pin set from the follow-state bucket. Each value is 2 raw
		// bytes: [followed, pinned], keyed by the identity string.
		if fb := tx.Bucket([]byte("followstate")); fb != nil {
			_ = fb.ForEach(func(k, v []byte) error {
				if len(v) >= 2 && v[1] == 1 {
					pinSet[string(k)] = true
				}
				return nil
			})
		}

		// Own PostLog: collect like target IDs and reply parent IDs, and
		// add own posts to the event-ID->author index.
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

		// Crawl bucket: single pass. For each author sub-bucket, decode
		// every event once. PostLog events extend the event-ID->author
		// index (only for followed authors, since that is all the metrics
		// resolve). Follow events targeting own pubkey mark reciprocal.
		crawl := tx.Bucket([]byte("crawl"))
		if crawl == nil {
			return nil
		}
		return crawl.ForEach(func(authorKey, _ []byte) error {
			authorID := core.Identity(authorKey)
			isFollowed := followSet[authorID.String()]
			ab := crawl.Bucket(authorKey)
			if ab == nil {
				return nil
			}
			return ab.ForEach(func(k, v []byte) error {
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
					recip[authorID.String()] = 1
				}
				return nil
			})
		})
	})
	if err != nil {
		failf("read: %v", err)
	}

	// Resolve own likes and replies to per-author counts using the index.
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

	fmt.Printf("follow graph size: %d\n\n", len(follows))

	likeVals := identityMetricValues(likeCounts)
	replyVals := identityMetricValues(replyCounts)
	recipVals := stringMetricValues(recip)
	pinVals := make([]float64, 0, len(follows))
	for _, f := range follows {
		if pinSet[f.String()] {
			pinVals = append(pinVals, 1)
		}
	}

	report("outbound likes/author", likeVals)
	report("outbound replies/author", replyVals)
	report("reciprocal follow", recipVals)
	report("pin", pinVals)

	// Combined nonzero coverage: how many followed authors have any signal.
	nonzero := 0
	for _, f := range follows {
		if likeCounts[f] > 0 || replyCounts[f] > 0 || recip[f.String()] > 0 || pinSet[f.String()] {
			nonzero++
		}
	}
	fmt.Printf("\nauthors with any signal: %d of %d (%.1f%%)\n",
		nonzero, len(follows), pct(nonzero, len(follows)))
}

func readMetaIdentity(db *bolt.DB) (core.Identity, bool, error) {
	var id []byte
	err := db.View(func(tx *bolt.Tx) error {
		mb := tx.Bucket([]byte("meta"))
		if mb == nil {
			return nil
		}
		id = mb.Get([]byte("identity"))
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

// identityMetricValues extracts the integer counts from a core.Identity-keyed
// metric map into a sorted float64 slice for percentile and histogram
// reporting.
func identityMetricValues(m map[core.Identity]int) []float64 {
	out := make([]float64, 0, len(m))
	for _, v := range m {
		out = append(out, float64(v))
	}
	sort.Float64s(out)
	return out
}

// stringMetricValues extracts the integer counts from a string-keyed metric
// map into a sorted float64 slice.
func stringMetricValues(m map[string]int) []float64 {
	out := make([]float64, 0, len(m))
	for _, v := range m {
		out = append(out, float64(v))
	}
	sort.Float64s(out)
	return out
}

// report prints summary statistics and a histogram for one metric.
func report(name string, vals []float64) {
	fmt.Printf("== %s ==\n", name)
	if len(vals) == 0 {
		fmt.Println("  (no data)")
		fmt.Println()
		return
	}
	n := len(vals)
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	fmt.Printf("  authors with value: %d\n", n)
	fmt.Printf("  min: %.0f  max: %.0f  mean: %.2f\n", vals[0], vals[n-1], sum/float64(n))
	fmt.Printf("  p50: %.0f  p90: %.0f  p99: %.0f\n",
		percentile(vals, 50), percentile(vals, 90), percentile(vals, 99))
	histogram(vals)
	fmt.Println()
}

// percentile returns the p-th percentile of a sorted slice.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// histogram prints a coarse ASCII histogram bucketing values by powers of 2.
func histogram(sorted []float64) {
	if len(sorted) == 0 {
		return
	}
	max := sorted[len(sorted)-1]
	if max <= 0 {
		return
	}
	// Buckets: [0], [1], [2-3], [4-7], [8-15], [16-31], [32+].
	bucketOf := func(v float64) int {
		if v <= 0 {
			return 0
		}
		b := 1
		for v > 1 {
			v /= 2
			b++
		}
		return b
	}
	maxBucket := bucketOf(max)
	counts := make(map[int]int)
	for _, v := range sorted {
		counts[bucketOf(v)]++
	}
	for b := 0; b <= maxBucket; b++ {
		c := counts[b]
		if c == 0 && b > 0 && b < maxBucket {
			continue
		}
		label := bucketLabel(b)
		bar := strings.Repeat("#", int(math.Ceil(float64(c)*50/float64(len(sorted)))))
		fmt.Printf("  %-8s %4d  %s\n", label, c, bar)
	}
}

func bucketLabel(b int) string {
	switch b {
	case 0:
		return "0"
	case 1:
		return "1"
	default:
		lo := 1 << (b - 1)
		hi := (1 << b) - 1
		return fmt.Sprintf("%d-%d", lo, hi)
	}
}

func pct(n, total int) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(n) / float64(total)
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "metricsdist:", msg)
	os.Exit(1)
}

func failf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "metricsdist: "+format+"\n", args...)
	os.Exit(1)
}
