//go:build ignore

// gentestdb generates a synthetic driftnode store for exercising the TUI at
// scale without a real distributed network. It creates a local identity that
// follows a configurable number of remote zens, each with its own posts and
// follow-back edges, all written into the crawl cache as if they had been
// synced. The resulting db opens in the daemon and TUI like any real store:
// the feed, follows, followers, and zens tabs all have content to render.
//
// Only event kinds with a real creation path are generated: Post, Profile,
// Follow, and Like. A reply is a Post with parent_id set, so replies reuse
// the Post kind. Delete has no command or RPC to produce it, so it is omitted.
//
// Distribution mode (-dist):
//
//	powerlaw (default): posts per zen, the local user's outbound likes, and
//	  outbound replies follow heavy-tailed distributions matched to observed
//	  social-network sizing. Most zens have few posts; most followed authors
//	  get zero or one local like; a few get many. This is the shape the
//	  weighted-follow-graph normalization (plan phase 4) must be tuned against.
//	uniform: legacy flat generation. Every zen gets exactly -posts posts;
//	  likes are spread evenly across all posts by a uniformly random liker.
//	  Useful only as a baseline to contrast against the power-law shape.
//
// Usage:
//
//	go run scripts/gentestdb.go -db data/test.db [flags]
//
// Flags:
//
//	-db            path to the bbolt store (default data/test.db)
//	-zens          number of remote zens to generate (default 100)
//	-posts         mean posts per zen (default 20). In powerlaw mode the
//	              per-zen count is heavy-tailed around this mean; in uniform
//	              mode every zen gets exactly this many.
//	-followers     fraction of zens that follow the local identity back, 0..1 (default 0.5)
//	-likes         fraction of posts that receive a background like from a remote zen, 0..1 (default 0.1)
//	-pins          fraction of followed zens the local identity pins, 0..1 (default 0.1)
//	-own-posts     posts to write to the local identity's own PostLog (default 5)
//	-own-like-max  powerlaw only: max likes the local identity gives a single followed author (default 30)
//	-own-reply-max powerlaw only: max replies the local identity sends to a single followed author (default 10)
//	-skew          powerlaw only: Zipf exponent for likes per author (default 1.5); replies use a steeper exponent
//	-dist          distribution mode: powerlaw or uniform (default powerlaw)
//	-name          display name for the local identity (default "me")
//	-passphrase    passphrase to encrypt the local private key (default "passphrase")
//	-seed          deterministic PRNG seed so two runs with the same flags match (default 1)
//	-overwrite     replace the db if it already exists (default false)
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

// Distribution modes.
const (
	distPowerlaw = "powerlaw"
	distUniform  = "uniform"
)

// Power-law tuning defaults. The local user's outbound likes and replies are
// the metrics that feed the weighted follow graph; their per-author counts
// are drawn from a Zipf distribution so a few authors receive most of the
// user's engagement and a long tail receives zero or one, matching observed
// social-network interaction skew.
const (
	defaultSkew      = 1.5 // Zipf exponent for likes per author.
	replySkewDelta   = 0.8 // replies are more concentrated than likes.
	postTailMultiple = 10  // max posts per zen is this times the mean.
)

// remote is a generated zen's keypair, kept so later phases (follow-back,
// likes, pins) can sign events in its log.
type remote struct {
	kp *core.KeyPair
}

// postRef pairs a generated post with the keypair that authored it, so a
// like or reply can target the post by event ID and pick a liker that is not
// the author.
type postRef struct {
	author *core.KeyPair
	id     core.EventID
}

func main() {
	dbPath := flag.String("db", "data/test.db", "path to the bbolt store")
	nZens := flag.Int("zens", 100, "number of remote zens to generate")
	postsPer := flag.Int("posts", 20, "mean posts per zen")
	followBack := flag.Float64("followers", 0.5, "fraction of zens that follow the local identity back (0..1)")
	likesFrac := flag.Float64("likes", 0.1, "fraction of posts that receive a background like from a remote zen (0..1)")
	pinsFrac := flag.Float64("pins", 0.1, "fraction of followed zens the local identity pins (0..1)")
	ownPosts := flag.Int("own-posts", 5, "posts to write to the local identity's own PostLog")
	ownLikeMax := flag.Int("own-like-max", 30, "powerlaw: max likes the local identity gives a single followed author")
	ownReplyMax := flag.Int("own-reply-max", 10, "powerlaw: max replies the local identity sends to a single followed author")
	skew := flag.Float64("skew", defaultSkew, "powerlaw: Zipf exponent for likes per author (replies use a steeper exponent)")
	dist := flag.String("dist", distPowerlaw, "distribution mode: powerlaw or uniform")
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
	if *likesFrac < 0 || *likesFrac > 1 {
		fail("-likes must be between 0 and 1")
	}
	if *pinsFrac < 0 || *pinsFrac > 1 {
		fail("-pins must be between 0 and 1")
	}
	if *ownLikeMax < 0 || *ownReplyMax < 0 {
		fail("-own-like-max and -own-reply-max cannot be negative")
	}
	if *dist != distPowerlaw && *dist != distUniform {
		failf("-dist must be %q or %q", distPowerlaw, distUniform)
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

	// Own ProfileLog events (the local profile and Follow events) are
	// buffered separately and flushed in one transaction with the
	// follow-state cache, since per-follow transactions dominate
	// generation time at scale.
	var profileBatch []crawlRow
	putProfile := func(se *core.SignedEvent) {
		id, err := se.ID()
		if err != nil {
			failf("event id: %v", err)
		}
		b, err := core.CanonicalEncode(se)
		if err != nil {
			failf("encode event: %v", err)
		}
		profileBatch = append(profileBatch, crawlRow{id: id, bytes: b})
	}

	// Follow-state cache rows: each followed identity gets a 2-byte entry
	// {followed=1, pinned=0}, written in the same transaction as the Follow
	// events so the cache and the signed log stay in sync.
	var followStateRows []followStateRow
	followSeq := uint64(1) // profile event is seq 1; follows continue from 2

	// Own profile event: buffered into the profile batch for a single
	// transactional flush with the Follow events.
	if err := appendOwnProfileEvent(own, *ownName, 1, putProfile); err != nil {
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

	// Own PostLog events (the local identity's posts, likes, replies) are
	// buffered separately and flushed in one transaction. Batching avoids
	// one write transaction per event and skips AppendOwnEvent's per-like
	// dedup scan, which is O(n^2) over the growing own PostLog.
	var ownBatch []crawlRow
	putOwn := func(se *core.SignedEvent) {
		id, err := se.ID()
		if err != nil {
			failf("event id: %v", err)
		}
		b, err := core.CanonicalEncode(se)
		if err != nil {
			failf("encode event: %v", err)
		}
		ownBatch = append(ownBatch, crawlRow{id: id, bytes: b})
	}

	// Generate the remote zens. Each gets a keypair, a profile name, and a
	// set of posts, buffered for the final batch write.
	remotes := make([]remote, 0, *nZens)
	base := time.Now().Add(-time.Hour * 24).UnixNano()

	// postsByAuthor[i] holds author i's posts, grouped so the local user's
	// outbound likes and replies can target a specific followed author.
	postsByAuthor := make([][]postRef, *nZens)

	// Collect every generated post (own and remote) with its author and
	// event ID so background likes can reference them by ID after signing.
	var allPosts []postRef

	// Per-zen post count distribution. In powerlaw mode the count is drawn
	// from a Zipf distribution with a bounded tail; in uniform mode every
	// zen gets exactly -posts. The Zipf generator emits rank-based counts
	// that decay as 1/rank^s, which approximates the heavy-tailed posts-
	// per-user distribution observed on social networks (most users post
	// little, a few post a lot).
	postCounts := make([]int, *nZens)
	switch *dist {
	case distPowerlaw:
		maxPosts := *postsPer * postTailMultiple
		if maxPosts < 1 {
			maxPosts = 1
		}
		zipf := rand.NewZipf(rnd, defaultSkew, 1, uint64(maxPosts))
		for i := range postCounts {
			postCounts[i] = int(zipf.Uint64()) + 1
		}
	case distUniform:
		for i := range postCounts {
			postCounts[i] = *postsPer
		}
	}

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
		// feed. Follow events are buffered and flushed in one transaction
		// after all zens are generated, since per-follow transactions
		// dominate generation time at scale.
		followSeq++
		followEvent, err := own.Sign(core.Event{
			Kind:      core.KindFollow,
			Log:       core.ProfileLog,
			Timestamp: core.Now64(),
			Sequence:  followSeq,
			Follow:    &core.Follow{TargetPubkey: pubKeyArr(kp)},
		})
		if err != nil {
			failf("sign follow zen %d: %v", i, err)
		}
		putProfile(followEvent)
		followStateRows = append(followStateRows, followStateRow{
			id:    kp.Identity(),
			state: []byte{1, 0},
		})

		n := postCounts[i]
		var seq uint64
		for p := 0; p < n; p++ {
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
			ref := postRef{author: kp, id: postID(post)}
			allPosts = append(allPosts, ref)
			postsByAuthor[i] = append(postsByAuthor[i], ref)
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

	// Flush the local identity's ProfileLog events (own profile + Follow
	// events) and the follow-state cache in one transaction.
	if err := flushProfile(s, profileBatch, followStateRows); err != nil {
		failf("flush profile/follow events: %v", err)
	}

	// A handful of the local identity's own posts, so the feed shows its
	// author too, not only remote zens. ownPostSeq is the shared sequence
	// counter for all own PostLog events (posts, likes, replies).
	ownPostSeq := uint64(0)
	for p := 0; p < *ownPosts; p++ {
		ownPostSeq++
		ts := base + int64(p)*int64(time.Hour)
		post, err := own.Sign(core.Event{
			Kind:      core.KindPost,
			Log:       core.PostLog,
			Timestamp: ts,
			Sequence:  ownPostSeq,
			Post:      &core.Post{Text: postText(rnd, *ownName, p)},
		})
		if err != nil {
			failf("sign own post %d: %v", p, err)
		}
		putOwn(post)
		ref := postRef{author: own, id: postID(post)}
		allPosts = append(allPosts, ref)
		postsByAuthor = append(postsByAuthor, []postRef{ref})
	}

	// Background likes from remote zens on a fraction of all posts. These
	// populate LikesForPosts for the local user's own posts but are not the
	// outbound metric; they exist so the feed shows like counts.
	bgLikes := generateBackgroundLikes(rnd, own, remotes, allPosts, *likesFrac, putCrawled, putOwn, &ownPostSeq)
	if err := flushCrawled(s, batch); err != nil {
		failf("flush background like events: %v", err)
	}

	// Outbound likes and replies by the local identity. These are the
	// interaction metrics that feed the weighted follow graph. In powerlaw
	// mode the per-author counts are drawn from a Zipf distribution so a
	// few followed authors receive most of the user's engagement and a long
	// tail receives zero or one. Own events are buffered and flushed in one
	// transaction after generation.
	var ownLikes, ownReplies int
	switch *dist {
	case distPowerlaw:
		ownLikes, ownReplies = generateOwnInteractionsPowerlaw(
			rnd, own, remotes, postsByAuthor, *skew,
			*ownLikeMax, *ownReplyMax, putOwn, &ownPostSeq)
	case distUniform:
		ownLikes, ownReplies = generateOwnInteractionsUniform(
			rnd, own, remotes, postsByAuthor, *likesFrac, putOwn, &ownPostSeq)
	}
	if err := flushOwn(s, ownBatch); err != nil {
		failf("flush own events: %v", err)
	}

	// Pins: a fraction of followed zens are pinned by the local identity.
	// A pin is local state in a bbolt bucket, never synced and never
	// signed. The local identity can only pin identities it follows.
	pins := generatePins(rnd, remotes, *pinsFrac, s)

	summary(*dbPath, *nZens, *postsPer, followers, bgLikes, pins, *ownPosts,
		ownLikes, ownReplies, *dist)
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

// flushOwn writes all buffered own-PostLog events into the own/post bucket in
// a single bbolt transaction. Own events are produced at high volume (the
// local identity's posts, likes, and replies), so batching them avoids one
// write transaction per event and skips AppendOwnEvent's per-like dedup scan,
// which is O(n^2) over the growing PostLog. The generator already ensures
// distinct like targets per author via a seen-set, so dedup by event ID is
// the only guard needed here.
func flushOwn(s *store.Store, rows []crawlRow) error {
	db := s.DB()
	return db.Update(func(tx *bolt.Tx) error {
		logBucket, err := tx.Bucket([]byte("own")).CreateBucketIfNotExists([]byte(core.PostLog))
		if err != nil {
			return fmt.Errorf("create own/post bucket: %w", err)
		}
		for _, r := range rows {
			if logBucket.Get(r.id[:]) != nil {
				continue
			}
			if err := logBucket.Put(r.id[:], r.bytes); err != nil {
				return err
			}
		}
		return nil
	})
}

// appendOwnProfileEvent signs a Profile event for the local identity and
// buffers it into the profile batch.
func appendOwnProfileEvent(kp *core.KeyPair, name string, seq uint64, put func(*core.SignedEvent)) error {
	se, err := kp.Sign(core.Event{
		Kind:      core.KindProfile,
		Log:       core.ProfileLog,
		Timestamp: core.Now64(),
		Sequence:  seq,
		Profile:   &core.Profile{DisplayName: name},
	})
	if err != nil {
		return err
	}
	put(se)
	return nil
}

// pubKeyArr extracts the 32-byte public key from a keypair as a fixed array,
// for use in Follow.TargetPubkey.
func pubKeyArr(kp *core.KeyPair) [32]byte {
	pub, err := kp.Identity().PubkeyBytes()
	if err != nil {
		failf("pubkey bytes: %v", err)
	}
	var arr [32]byte
	copy(arr[:], pub)
	return arr
}

// flushProfile writes buffered own ProfileLog events into the own/profile
// bucket and the follow-state cache in a single bbolt transaction.
func flushProfile(s *store.Store, rows []crawlRow, followStates []followStateRow) error {
	db := s.DB()
	return db.Update(func(tx *bolt.Tx) error {
		logBucket, err := tx.Bucket([]byte("own")).CreateBucketIfNotExists([]byte(core.ProfileLog))
		if err != nil {
			return fmt.Errorf("create own/profile bucket: %w", err)
		}
		for _, r := range rows {
			if logBucket.Get(r.id[:]) != nil {
				continue
			}
			if err := logBucket.Put(r.id[:], r.bytes); err != nil {
				return err
			}
		}
		fs := tx.Bucket([]byte("followstate"))
		if fs == nil {
			return fmt.Errorf("followstate bucket missing")
		}
		for _, f := range followStates {
			if err := fs.Put([]byte(f.id), f.state); err != nil {
				return err
			}
		}
		return nil
	})
}

// followStateRow is one follow-state cache entry to write in the profile
// flush transaction: the identity string and its 2-byte {followed, pinned}
// encoding.
type followStateRow struct {
	id    core.Identity
	state []byte
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

// postID returns the event ID of a signed post, exiting on a malformed event
// since a freshly signed post always has one.
func postID(se *core.SignedEvent) core.EventID {
	id, err := se.ID()
	if err != nil {
		failf("post event id: %v", err)
	}
	return id
}

// generateBackgroundLikes signs Like events by remote zens targeting a
// fraction of all posts. The liker is drawn from all identities except the
// post's own author. Remote likes are buffered into the crawl cache; the
// local identity's likes are buffered into the own PostLog batch. The shared
// ownPostSeq counter is incremented for each local identity like so its
// PostLog sequence numbers stay unique across posts, background likes, and
// later outbound interactions. These populate per-post like counts in the
// feed; the local identity's own outbound likes are produced separately.
// Returns the like count.
func generateBackgroundLikes(rnd *rand.Rand, own *core.KeyPair, remotes []remote, posts []postRef, frac float64, putCrawled, putOwn func(*core.SignedEvent), ownPostSeq *uint64) int {
	if frac <= 0 || len(posts) == 0 {
		return 0
	}
	// All possible likers: the local identity plus every remote zen.
	likers := make([]*core.KeyPair, 0, 1+len(remotes))
	likers = append(likers, own)
	for _, r := range remotes {
		likers = append(likers, r.kp)
	}
	// Per-liker sequence counter for remote likers. The local identity
	// uses the shared ownPostSeq counter instead.
	likeSeq := make(map[core.Identity]uint64)
	// Dedup: a given liker liking the same post twice is a no-op. Track
	// (liker, target) pairs so the count is not inflated by re-likes.
	seen := make(map[likerTarget]bool)
	var count int
	for _, p := range posts {
		if rnd.Float64() >= frac {
			continue
		}
		// Pick a liker that is not the post's author.
		var liker *core.KeyPair
		for {
			cand := likers[rnd.Intn(len(likers))]
			if cand.Identity() != p.author.Identity() {
				liker = cand
				break
			}
		}
		key := likerTarget{liker: liker.Identity(), target: p.id}
		if seen[key] {
			continue
		}
		seen[key] = true
		ts := core.Now64()
		var seq uint64
		if liker.Identity() == own.Identity() {
			*ownPostSeq++
			seq = *ownPostSeq
		} else {
			seq = likeSeq[liker.Identity()] + 1
			likeSeq[liker.Identity()] = seq
		}
		se, err := liker.Sign(core.Event{
			Kind:      core.KindLike,
			Log:       core.PostLog,
			Timestamp: ts,
			Sequence:  seq,
			Like:      &core.Like{TargetID: p.id},
		})
		if err != nil {
			failf("sign like: %v", err)
		}
		if liker.Identity() == own.Identity() {
			putOwn(se)
		} else {
			putCrawled(se)
		}
		count++
	}
	return count
}

// likerTarget is a dedup key for background likes: one like per liker per
// target post.
type likerTarget struct {
	liker  core.Identity
	target core.EventID
}

// generateOwnInteractionsPowerlaw produces the local identity's outbound
// likes and replies with per-author counts drawn from Zipf distributions.
// Likes use the supplied skew; replies use a steeper skew so they are more
// concentrated. Each like targets a uniformly random post by the chosen
// author; each reply is a Post with parent_id set to a random post by that
// author, appended to the local PostLog so OutboundReplies can resolve it.
// The shared ownSeq counter is incremented for every event so PostLog
// sequence numbers stay unique. Returns (ownLikes, ownReplies) counts.
func generateOwnInteractionsPowerlaw(rnd *rand.Rand, own *core.KeyPair, remotes []remote, postsByAuthor [][]postRef, skew float64, likeMax, replyMax int, putOwn func(*core.SignedEvent), ownSeq *uint64) (int, int) {
	n := len(remotes)
	if n == 0 {
		return 0, 0
	}
	// Per-author like counts: Zipf over the author rank, bounded by likeMax.
	// A few authors get near likeMax; the long tail gets 0-1.
	likeZipf := rand.NewZipf(rnd, skew, 1, uint64(likeMax))
	replySkew := skew + replySkewDelta
	replyZipf := rand.NewZipf(rnd, replySkew, 1, uint64(replyMax))

	var likes, replies int
	for i, r := range remotes {
		authorPosts := postsByAuthor[i]
		if len(authorPosts) == 0 {
			continue
		}
		// Likes: target distinct posts by this author.
		want := int(likeZipf.Uint64())
		seen := make(map[core.EventID]bool, want)
		for l := 0; l < want; l++ {
			target := authorPosts[rnd.Intn(len(authorPosts))]
			if seen[target.id] {
				continue
			}
			seen[target.id] = true
			*ownSeq++
			ts := core.Now64()
			se, err := own.Sign(core.Event{
				Kind:      core.KindLike,
				Log:       core.PostLog,
				Timestamp: ts,
				Sequence:  *ownSeq,
				Like:      &core.Like{TargetID: target.id},
			})
			if err != nil {
				failf("sign own like: %v", err)
			}
			putOwn(se)
			likes++
		}
		// Replies: a Post with parent_id set to a random post by this
		// author, appended to the local PostLog so OutboundReplies can
		// resolve the parent's author.
		wantReplies := int(replyZipf.Uint64())
		for r2 := 0; r2 < wantReplies; r2++ {
			parent := authorPosts[rnd.Intn(len(authorPosts))]
			*ownSeq++
			ts := core.Now64()
			se, err := own.Sign(core.Event{
				Kind:      core.KindPost,
				Log:       core.PostLog,
				Timestamp: ts,
				Sequence:  *ownSeq,
				Post: &core.Post{
					Text:     replyText(rnd, r.kp.Identity().String()),
					ParentID: parent.id,
				},
			})
			if err != nil {
				failf("sign own reply: %v", err)
			}
			putOwn(se)
			replies++
		}
	}
	return likes, replies
}

// generateOwnInteractionsUniform spreads the local identity's outbound likes
// evenly: each followed author's posts each receive a like with probability
// frac. No replies are produced in uniform mode (the legacy generator did not
// produce them). Returns (ownLikes, 0).
func generateOwnInteractionsUniform(rnd *rand.Rand, own *core.KeyPair, remotes []remote, postsByAuthor [][]postRef, frac float64, putOwn func(*core.SignedEvent), ownSeq *uint64) (int, int) {
	var likes int
	for i := range remotes {
		authorPosts := postsByAuthor[i]
		for _, target := range authorPosts {
			if rnd.Float64() >= frac {
				continue
			}
			*ownSeq++
			ts := core.Now64()
			se, err := own.Sign(core.Event{
				Kind:      core.KindLike,
				Log:       core.PostLog,
				Timestamp: ts,
				Sequence:  *ownSeq,
				Like:      &core.Like{TargetID: target.id},
			})
			if err != nil {
				failf("sign own like: %v", err)
			}
			putOwn(se)
			likes++
		}
	}
	return likes, 0
}

func replyText(rnd *rand.Rand, parentAuthor string) string {
	openers := []string{"yes,", "true,", "agreed:", "hmm,", "wait,", "also,", "exactly,", "no,", "so true,"}
	return fmt.Sprintf("%s %s", openers[rnd.Intn(len(openers))], parentAuthor)
}

// generatePins pins a fraction of the followed remote zens in the local
// store. A pin is local bbolt state, never signed and never synced. The
// local identity can only pin identities it follows, so the pool is the
// remote zens (all of which the local identity follows). Returns the pin
// count.
func generatePins(rnd *rand.Rand, remotes []remote, frac float64, s *store.Store) int {
	if frac <= 0 || len(remotes) == 0 {
		return 0
	}
	var count int
	for _, r := range remotes {
		if rnd.Float64() >= frac {
			continue
		}
		if err := s.PinIdentity(r.kp.Identity()); err != nil {
			failf("pin zen: %v", err)
		}
		count++
	}
	return count
}

func summary(dbPath string, zens, posts, followers, bgLikes, pins, ownPosts, ownLikes, ownReplies int, dist string) {
	var b strings.Builder
	fmt.Fprintf(&b, "generated %s\n", dbPath)
	fmt.Fprintf(&b, "  distribution:  %s\n", dist)
	fmt.Fprintf(&b, "  zens:          %d\n", zens)
	fmt.Fprintf(&b, "  posts/zen:     mean %d\n", posts)
	fmt.Fprintf(&b, "  followers:     %d of %d zens\n", followers, zens)
	fmt.Fprintf(&b, "  bg likes:      %d events (remote likers)\n", bgLikes)
	fmt.Fprintf(&b, "  own likes:     %d (outbound metric)\n", ownLikes)
	fmt.Fprintf(&b, "  own replies:   %d (outbound metric)\n", ownReplies)
	fmt.Fprintf(&b, "  pinned zens:   %d of %d\n", pins, zens)
	fmt.Fprintf(&b, "  own posts:     %d\n", ownPosts)
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
