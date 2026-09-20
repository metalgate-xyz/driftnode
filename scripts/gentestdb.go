//go:build ignore

// gentestdb generates a synthetic driftnode store for exercising the TUI at
// scale without a real distributed network. It creates a local identity that
// follows a configurable number of remote zens, each with its own posts and
// follow-back edges, all written into the crawl cache as if they had been
// synced. The resulting db opens in the daemon and TUI like any real store:
// the feed, follows, followers, and zens tabs all have content to render.
//
// Only event kinds with a real creation path are generated: Post, Profile,
// and Follow. Reply, Like, and Delete have no command or RPC to produce
// them, so they are omitted.
//
// Usage:
//
//	go run scripts/gentestdb.go -db data/test.db [flags]
//
// Flags:
//
//	-db          path to the bbolt store (default data/test.db)
//	-zens        number of remote zens to generate (default 100)
//	-posts       posts per zen (default 20)
//	-followers   fraction of zens that follow the local identity back, 0..1 (default 0.5)
//	-own-posts   posts to write to the local identity's own PostLog (default 5)
//	-name        display name for the local identity (default "me")
//	-passphrase  passphrase to encrypt the local private key (default "passphrase")
//	-seed        deterministic PRNG seed so two runs with the same flags match (default 1)
//	-overwrite   replace the db if it already exists (default false)
//
// The generator uses math/rand seeded from -seed for reproducible content.
// It does not touch the network and does not write routing tokens, so the
// daemon's sync loop has nothing to dial: the zens are present in the crawl
// cache and follow graph only.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"driftnode/internal/core"
	"driftnode/internal/store"

	bolt "go.etcd.io/bbolt"
)

func main() {
	dbPath := flag.String("db", "data/test.db", "path to the bbolt store")
	nZens := flag.Int("zens", 100, "number of remote zens to generate")
	postsPer := flag.Int("posts", 20, "posts per zen")
	followBack := flag.Float64("followers", 0.5, "fraction of zens that follow the local identity back (0..1)")
	ownPosts := flag.Int("own-posts", 5, "posts to write to the local identity's own PostLog")
	ownName := flag.String("name", "me", "display name for the local identity")
	passphrase := flag.String("passphrase", "passphrase", "passphrase to encrypt the local private key")
	seed := flag.Int64("seed", 1, "deterministic PRNG seed")
	overwrite := flag.Bool("overwrite", false, "replace the db if it already exists")
	flag.Parse()

	if *nZens < 0 || *postsPer < 0 || *ownPosts < 0 {
		fail("counts cannot be negative")
	}
	if *followBack < 0 || *followBack > 1 {
		fail("-followers must be between 0 and 1")
	}

	if *overwrite {
		if err := os.Remove(*dbPath); err != nil && !os.IsNotExist(err) {
			failf("remove existing db: %v", err)
		}
	} else if exists(*dbPath) {
		failf("db already exists at %s; pass -overwrite to replace it", *dbPath)
	}

	rnd := rand.New(rand.NewSource(*seed))

	s, err := store.Open(*dbPath)
	if err != nil {
		failf("open store: %v", err)
	}
	defer s.Close()

	// Local identity: the account whose TUI will display this db. Its own
	// logs hold the follow edges that make the remote zens appear in the
	// feed (AllPosts pulls crawled events for identities in the follow
	// graph), and its profile supplies the name shown in whoami.
	own, err := core.NewKeyPair()
	if err != nil {
		failf("generate own keypair: %v", err)
	}
	enc := core.DefaultKeyEncryption()
	ek, err := enc.Encrypt(own.Private, []byte(*passphrase))
	if err != nil {
		failf("encrypt own key: %v", err)
	}
	if err := s.InitIdentity(own, ek); err != nil {
		failf("init own identity: %v", err)
	}
	ownPub, err := own.Identity().PubkeyBytes()
	if err != nil {
		failf("own pubkey: %v", err)
	}
	var ownArr [32]byte
	copy(ownArr[:], ownPub)

	if err := appendOwnProfile(s, own, *ownName); err != nil {
		failf("own profile: %v", err)
	}

	// Remote zens are written in a single bbolt batch at the end, since
	// store.PutCrawledEvent opens one transaction per event and would make
	// thousands of events slow. The buffer holds (author, eventID, bytes)
	// for the final flush into the crawl bucket with the same layout the
	// store uses.
	var batch []crawlRow
	putCrawled := func(se *core.SignedEvent) {
		id, err := se.ID()
		if err != nil {
			failf("event id: %v", err)
		}
		b, err := core.CanonicalEncode(se)
		if err != nil {
			failf("encode event: %v", err)
		}
		batch = append(batch, crawlRow{author: se.Author, id: id, bytes: b})
	}

	// Generate the remote zens. Each gets a keypair, a profile name, and a
	// set of posts, buffered for the final batch write.
	type remote struct {
		kp *core.KeyPair
	}
	remotes := make([]remote, 0, *nZens)
	base := time.Now().Add(-time.Hour * 24).UnixNano()

	for i := 0; i < *nZens; i++ {
		kp, err := core.NewKeyPair()
		if err != nil {
			failf("generate zen %d keypair: %v", i, err)
		}
		name := zenName(rnd, i)
		prof, err := kp.Sign(core.Event{
			Kind:      core.KindProfile,
			Log:       core.ProfileLog,
			Timestamp: base + int64(i),
			Sequence:  1,
			Profile:   &core.Profile{DisplayName: name},
		})
		if err != nil {
			failf("sign profile zen %d: %v", i, err)
		}
		putCrawled(prof)

		// The local identity follows this zen, so its posts show in the
		// feed. One Follow event in the local ProfileLog per zen.
		if err := followLocal(s, own, kp.Identity()); err != nil {
			failf("follow zen %d: %v", i, err)
		}

		var seq uint64
		for p := 0; p < *postsPer; p++ {
			seq++
			ts := base + int64(i)*int64(time.Second) + int64(p)*int64(time.Minute)
			post, err := kp.Sign(core.Event{
				Kind:      core.KindPost,
				Log:       core.PostLog,
				Timestamp: ts,
				Sequence:  seq,
				Post:      &core.Post{Text: postText(rnd, name, p)},
			})
			if err != nil {
				failf("sign post zen %d post %d: %v", i, p, err)
			}
			putCrawled(post)
		}

		remotes = append(remotes, remote{kp: kp})
	}

	// A fraction of zens follow the local identity back. Their Follow
	// events target the local pubkey and live in their own ProfileLogs;
	// ReceivedFollowers scans the crawl cache for them.
	followers := 0
	for _, r := range remotes {
		if rnd.Float64() < *followBack {
			se, err := r.kp.Sign(core.Event{
				Kind:      core.KindFollow,
				Log:       core.ProfileLog,
				Timestamp: core.Now64(),
				Sequence:  2,
				Follow:    &core.Follow{TargetPubkey: ownArr},
			})
			if err != nil {
				failf("sign follow-back: %v", err)
			}
			putCrawled(se)
			followers++
		}
	}

	// Flush all crawled events in one transaction. The crawl bucket uses
	// per-author sub-buckets keyed by event ID, matching the store's
	// PutCrawledEvent layout.
	if err := flushCrawled(s, batch); err != nil {
		failf("flush crawled events: %v", err)
	}

	// A handful of the local identity's own posts, so the feed shows its
	// author too, not only remote zens.
	ownSeq := uint64(0)
	for p := 0; p < *ownPosts; p++ {
		ownSeq++
		ts := base + int64(p)*int64(time.Hour)
		post, err := own.Sign(core.Event{
			Kind:      core.KindPost,
			Log:       core.PostLog,
			Timestamp: ts,
			Sequence:  ownSeq,
			Post:      &core.Post{Text: postText(rnd, *ownName, p)},
		})
		if err != nil {
			failf("sign own post %d: %v", p, err)
		}
		if err := s.AppendOwnEvent(core.PostLog, post); err != nil {
			failf("append own post %d: %v", p, err)
		}
	}

	summary(*dbPath, *nZens, *postsPer, followers, *ownPosts)
}

// flushCrawled writes all buffered crawled events into the crawl bucket in a
// single bbolt transaction, using the same per-author sub-bucket layout the
// store package uses. Dedup by event ID is preserved (PutCrawledEvent skips
// existing keys).
func flushCrawled(s *store.Store, rows []crawlRow) error {
	db := s.DB()
	return db.Update(func(tx *bolt.Tx) error {
		crawl := tx.Bucket([]byte("crawl"))
		if crawl == nil {
			return fmt.Errorf("crawl bucket missing")
		}
		for _, r := range rows {
			authorBucket, err := crawl.CreateBucketIfNotExists([]byte(r.author))
			if err != nil {
				return err
			}
			if authorBucket.Get(r.id[:]) != nil {
				continue
			}
			if err := authorBucket.Put(r.id[:], r.bytes); err != nil {
				return err
			}
		}
		return nil
	})
}

// appendOwnProfile writes a single Profile event to the local identity's own
// ProfileLog so whoami and the TUI show a display name.
func appendOwnProfile(s *store.Store, kp *core.KeyPair, name string) error {
	se, err := kp.Sign(core.Event{
		Kind:      core.KindProfile,
		Log:       core.ProfileLog,
		Timestamp: core.Now64(),
		Sequence:  1,
		Profile:   &core.Profile{DisplayName: name},
	})
	if err != nil {
		return err
	}
	return s.AppendOwnEvent(core.ProfileLog, se)
}

// followLocal appends a Follow event in the local identity's own ProfileLog
// targeting the remote identity, adding it to the follow graph that
// AllPosts and the Follows tab read.
func followLocal(s *store.Store, own *core.KeyPair, target core.Identity) error {
	seq, err := s.OwnEventCount(core.ProfileLog)
	if err != nil {
		return err
	}
	pub, err := target.PubkeyBytes()
	if err != nil {
		return err
	}
	var arr [32]byte
	copy(arr[:], pub)
	se, err := own.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: core.Now64(),
		Sequence:  seq + 1,
		Follow:    &core.Follow{TargetPubkey: arr},
	})
	if err != nil {
		return err
	}
	return s.AppendOwnEvent(core.ProfileLog, se)
}

// zenName builds a readable, stable display name from the index, so the
// Zens and Follows lists have something scannable.
func zenName(rnd *rand.Rand, i int) string {
	adjs := []string{"quiet", "bright", "drift", "iron", "solar", "lunar", "mist", "amber", "verdant", "cobalt"}
	nouns := []string{"fox", "lark", "cedar", "tide", "spark", "raven", "fern", "hawk", "stone", "wave"}
	return fmt.Sprintf("%s-%s-%d", adjs[i%len(adjs)], nouns[(i*7)%len(nouns)], i)
}

func postText(rnd *rand.Rand, author string, n int) string {
	verbs := []string{"thinking about", "watching", "waking up to", "missing", "working on", "remembering", "looking for"}
	objects := []string{"the rain", "old maps", "a slow train", "the harbor", "a long read", "the morning", "a cold coffee"}
	return fmt.Sprintf("%s: %s %s (#%d)", author, verbs[rnd.Intn(len(verbs))], objects[rnd.Intn(len(objects))], n)
}

func summary(dbPath string, zens, posts, followers, ownPosts int) {
	totalEvents := zens * (1 + posts)
	var b strings.Builder
	fmt.Fprintf(&b, "generated %s\n", dbPath)
	fmt.Fprintf(&b, "  zens:          %d\n", zens)
	fmt.Fprintf(&b, "  posts/zen:     %d (total %d)\n", posts, zens*posts)
	fmt.Fprintf(&b, "  followers:     %d of %d zens\n", followers, zens)
	fmt.Fprintf(&b, "  own posts:     %d\n", ownPosts)
	fmt.Fprintf(&b, "  remote events: ~%d (plus local profile/posts)\n", totalEvents)
	fmt.Fprintf(&b, "\nopen with:\n  driftnode --db %s daemon unlock  # then run the TUI\n", dbPath)
	fmt.Fprint(os.Stdout, b.String())
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "gentestdb:", msg)
	os.Exit(1)
}

func failf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "gentestdb: "+format+"\n", args...)
	os.Exit(1)
}

// crawlRow is one buffered crawled event pending a batch flush into the crawl
// bucket, mirroring store.PutCrawledEvent's per-author, per-eventID layout.
type crawlRow struct {
	author core.Identity
	id     core.EventID
	bytes  []byte
}
