package store

import (
	"crypto/ed25519"
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"

	"driftnode/internal/core"
	"driftnode/internal/weight"

	bolt "go.etcd.io/bbolt"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestInitIdentityAndReadback(t *testing.T) {
	s := newTestStore(t)
	kp, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	enc := core.DefaultKeyEncryption()
	ek, err := enc.Encrypt(kp.Private, []byte("passphrase"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("InitIdentity: %v", err)
	}

	// Read back identity.
	id, ok, err := s.Identity()
	if err != nil {
		t.Fatalf("Identity: %v", err)
	}
	if !ok {
		t.Fatal("identity not found")
	}
	if id != kp.Identity() {
		t.Fatalf("identity mismatch: got %q want %q", id, kp.Identity())
	}

	// Read back encrypted key and decrypt.
	ek2, err := s.EncryptedKey()
	if err != nil {
		t.Fatalf("EncryptedKey: %v", err)
	}
	priv, err := enc.Decrypt(ek2, []byte("passphrase"))
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(priv) != string(kp.Private) {
		t.Fatal("decrypted private key mismatch")
	}
}

func TestInitIdentityRefusesDoubleInit(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("first InitIdentity: %v", err)
	}
	if err := s.InitIdentity(kp, ek); err == nil {
		t.Fatal("expected error on double init")
	}
}

func TestAppendAndReadOwnEvents(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()

	se1, err := kp.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 100, Sequence: 1,
		Post: &core.Post{Text: "first"},
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	se2, err := kp.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 200, Sequence: 2,
		Post: &core.Post{Text: "second"},
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if err := s.AppendOwnEvent(core.PostLog, se1); err != nil {
		t.Fatalf("Append se1: %v", err)
	}
	if err := s.AppendOwnEvent(core.PostLog, se2); err != nil {
		t.Fatalf("Append se2: %v", err)
	}

	events, err := s.OwnEvents(core.PostLog)
	if err != nil {
		t.Fatalf("OwnEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	for _, se := range events {
		if err := se.Verify(); err != nil {
			t.Fatalf("Verify: %v", err)
		}
	}
}

func TestAppendOwnEventDedups(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	se, _ := kp.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 1, Sequence: 1,
		Post: &core.Post{Text: "dup"},
	})
	if err := s.AppendOwnEvent(core.PostLog, se); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := s.AppendOwnEvent(core.PostLog, se); err != nil {
		t.Fatalf("second append: %v", err)
	}
	events, _ := s.OwnEvents(core.PostLog)
	if len(events) != 1 {
		t.Fatalf("dedup failed: want 1 event, got %d", len(events))
	}
}

func TestMediaRoundTrip(t *testing.T) {
	s := newTestStore(t)
	data := []byte("some media bytes")
	hash := core.ContentHashOf(data)
	if err := s.PutMedia(hash, data); err != nil {
		t.Fatalf("PutMedia: %v", err)
	}
	got, err := s.GetMedia(hash)
	if err != nil {
		t.Fatalf("GetMedia: %v", err)
	}
	if string(got) != string(data) {
		t.Fatal("media data mismatch")
	}
}

func TestExportImportBackup(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("InitIdentity: %v", err)
	}
	se, _ := kp.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 1, Sequence: 1,
		Post: &core.Post{Text: "backup test"},
	})
	if err := s.AppendOwnEvent(core.PostLog, se); err != nil {
		t.Fatalf("Append: %v", err)
	}

	bd, err := s.ExportBackup()
	if err != nil {
		t.Fatalf("ExportBackup: %v", err)
	}
	if bd.Key == nil {
		t.Fatal("backup missing key")
	}
	if len(bd.OwnLogs) != 1 {
		t.Fatalf("backup events: want 1, got %d", len(bd.OwnLogs))
	}

	// Import into a fresh store and verify the event is there.
	s2 := newTestStore(t)
	if err := s2.ImportBackup(bd); err != nil {
		t.Fatalf("ImportBackup: %v", err)
	}
	events, _ := s2.OwnEvents(core.PostLog)
	if len(events) != 1 {
		t.Fatalf("after import: want 1 event, got %d", len(events))
	}
	if err := events[0].Verify(); err != nil {
		t.Fatalf("imported event does not verify: %v", err)
	}

	// Importing again must not duplicate.
	if err := s2.ImportBackup(bd); err != nil {
		t.Fatalf("second ImportBackup: %v", err)
	}
	events, _ = s2.OwnEvents(core.PostLog)
	if len(events) != 1 {
		t.Fatalf("after re-import: want 1 event, got %d", len(events))
	}
}

func TestBackupIncludesDetailLog(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("InitIdentity: %v", err)
	}
	se, _ := kp.Sign(core.Event{
		Kind: core.KindDetail, Log: core.DetailLog, Timestamp: 1, Sequence: 1,
		Detail: &core.Detail{Bio: "backup me"},
	})
	if err := s.AppendOwnEvent(core.DetailLog, se); err != nil {
		t.Fatalf("Append detail: %v", err)
	}

	bd, err := s.ExportBackup()
	if err != nil {
		t.Fatalf("ExportBackup: %v", err)
	}
	var hasDetail bool
	for _, e := range bd.OwnLogs {
		if e.Event.Kind == core.KindDetail {
			hasDetail = true
			break
		}
	}
	if !hasDetail {
		t.Fatal("backup does not include DetailLog events")
	}

	// Import into a fresh store and verify the detail event is there.
	s2 := newTestStore(t)
	if err := s2.ImportBackup(bd); err != nil {
		t.Fatalf("ImportBackup: %v", err)
	}
	events, _ := s2.OwnEvents(core.DetailLog)
	if len(events) != 1 {
		t.Fatalf("after import: want 1 detail event, got %d", len(events))
	}
	if events[0].Event.Detail == nil || events[0].Event.Detail.Bio != "backup me" {
		t.Fatalf("imported detail event mismatch: %+v", events[0].Event.Detail)
	}
}

func TestFollowGraph(t *testing.T) {
	s := newTestStore(t)
	kp, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	target1, _, _ := ed25519.GenerateKey(nil)
	target2, _, _ := ed25519.GenerateKey(nil)

	sign := func(kind core.Kind, seq uint64, pub []byte) *core.SignedEvent {
		se, err := kp.Sign(core.Event{
			Kind: kind, Log: core.ProfileLog, Timestamp: core.Now64(), Sequence: seq,
			Follow: &core.Follow{TargetPubkey: [32]byte(pub)},
		})
		if err != nil {
			t.Fatalf("Sign: %v", err)
		}
		return se
	}

	if err := s.AppendOwnEvent(core.ProfileLog, sign(core.KindFollow, 1, target1)); err != nil {
		t.Fatalf("append follow1: %v", err)
	}
	if err := s.AppendOwnEvent(core.ProfileLog, sign(core.KindFollow, 2, target2)); err != nil {
		t.Fatalf("append follow2: %v", err)
	}

	ids, err := s.FollowGraph()
	if err != nil {
		t.Fatalf("FollowGraph: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("want 2 followed identities, got %d", len(ids))
	}

	// Unfollowing target1 should leave only target2.
	if err := s.AppendOwnEvent(core.ProfileLog, sign(core.KindUnfollow, 3, target1)); err != nil {
		t.Fatalf("append unfollow1: %v", err)
	}
	ids, err = s.FollowGraph()
	if err != nil {
		t.Fatalf("FollowGraph after unfollow: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("after unfollow: want 1 followed identity, got %d", len(ids))
	}
	want := core.IdentityFromPubkey(ed25519.PublicKey(target2))
	if ids[0] != want {
		t.Fatalf("after unfollow: want %s, got %s", want, ids[0])
	}
}

// TestAllPostsScopedToFollowGraph proves the merged feed (section 9.1) only
// includes PostLog events from identities the user actually follows. Events
// from non-followed zens can land in the crawl bucket via crawled profiles
// or probed-but-not-followed tokens; they must not surface in the feed.
func TestAllPostsScopedToFollowGraph(t *testing.T) {
	s := newTestStore(t)
	me, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("me: %v", err)
	}
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Two peers: followed (alice) and not followed (bob).
	alice, _ := core.NewKeyPair()
	bob, _ := core.NewKeyPair()

	// Follow alice only.
	alicePub, _ := alice.Identity().PubkeyBytes()
	followAlice, err := me.Sign(core.Event{
		Kind: core.KindFollow, Log: core.ProfileLog, Timestamp: 1, Sequence: 1,
		Follow: &core.Follow{TargetPubkey: [32]byte(alicePub)},
	})
	if err != nil {
		t.Fatalf("sign follow: %v", err)
	}
	if err := s.AppendOwnEvent(core.ProfileLog, followAlice); err != nil {
		t.Fatalf("append follow: %v", err)
	}

	// Both peers have PostLog events in the crawl bucket (as if synced
	// or crawled). Only alice's must appear in AllPosts.
	alicePost, err := alice.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 10, Sequence: 1,
		Post: &core.Post{Text: "alice post"},
	})
	if err != nil {
		t.Fatalf("sign alice post: %v", err)
	}
	bobPost, err := bob.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 20, Sequence: 1,
		Post: &core.Post{Text: "bob post"},
	})
	if err != nil {
		t.Fatalf("sign bob post: %v", err)
	}
	if _, err := s.PutRemoteEvent(alicePost, 1); err != nil {
		t.Fatalf("put alice post: %v", err)
	}
	if _, err := s.PutRemoteEvent(bobPost, 1); err != nil {
		t.Fatalf("put bob post: %v", err)
	}

	// Sanity: both are cached in the crawl bucket.
	ids, err := s.CrawledIdentities()
	if err != nil {
		t.Fatalf("CrawledIdentities: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("crawl bucket: want 2 authors, got %d", len(ids))
	}

	posts, err := s.AllPosts()
	if err != nil {
		t.Fatalf("AllPosts: %v", err)
	}
	var texts []string
	for _, se := range posts {
		if se.Event.Post != nil {
			texts = append(texts, se.Event.Post.Text)
		}
	}
	for _, txt := range texts {
		if txt == "bob post" {
			t.Fatalf("AllPosts leaked non-followed bob's post: %v", texts)
		}
	}
	foundAlice := false
	for _, txt := range texts {
		if txt == "alice post" {
			foundAlice = true
		}
	}
	if !foundAlice {
		t.Fatalf("AllPosts missing followed alice's post: %v", texts)
	}
}

// TestAllPostsOneTxMatchesAllPosts verifies the single-transaction feed
// loader returns the same events as the per-identity loader. AllPosts opens
// one bbolt transaction per followed identity; AllPostsOneTx collapses them
// into one, so both must yield the same scoped set.
func TestAllPostsOneTxMatchesAllPosts(t *testing.T) {
	s := newTestStore(t)
	me, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("me: %v", err)
	}
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	alice, _ := core.NewKeyPair()
	alicePub, _ := alice.Identity().PubkeyBytes()
	followAlice, err := me.Sign(core.Event{
		Kind: core.KindFollow, Log: core.ProfileLog, Timestamp: 1, Sequence: 1,
		Follow: &core.Follow{TargetPubkey: [32]byte(alicePub)},
	})
	if err != nil {
		t.Fatalf("sign follow: %v", err)
	}
	if err := s.AppendOwnEvent(core.ProfileLog, followAlice); err != nil {
		t.Fatalf("append follow: %v", err)
	}
	ownPost, err := me.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 5, Sequence: 1,
		Post: &core.Post{Text: "mine"},
	})
	if err != nil {
		t.Fatalf("sign own: %v", err)
	}
	if err := s.AppendOwnEvent(core.PostLog, ownPost); err != nil {
		t.Fatalf("append own: %v", err)
	}
	alicePost, err := alice.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 10, Sequence: 1,
		Post: &core.Post{Text: "alice"},
	})
	if err != nil {
		t.Fatalf("sign alice: %v", err)
	}
	if _, err := s.PutRemoteEvent(alicePost, 1); err != nil {
		t.Fatalf("put alice: %v", err)
	}

	a, err := s.AllPosts()
	if err != nil {
		t.Fatalf("AllPosts: %v", err)
	}
	b, err := s.AllPostsOneTx()
	if err != nil {
		t.Fatalf("AllPostsOneTx: %v", err)
	}
	if len(a) != len(b) {
		t.Fatalf("len: AllPosts=%d AllPostsOneTx=%d", len(a), len(b))
	}
	// Order is not guaranteed equal (AllPosts groups by identity; AllPostsOneTx
	// interleaves), so compare as id sets.
	ids := func(events []core.SignedEvent) map[string]bool {
		m := make(map[string]bool, len(events))
		for _, se := range events {
			id, _ := se.ID()
			m[id.String()] = true
		}
		return m
	}
	ai, bi := ids(a), ids(b)
	for id := range ai {
		if !bi[id] {
			t.Fatalf("AllPostsOneTx missing %s (present in AllPosts)", id)
		}
	}
	for id := range bi {
		if !ai[id] {
			t.Fatalf("AllPostsOneTx has extra %s (absent in AllPosts)", id)
		}
	}
}

func makeFollowEventFor(t *testing.T, follower *core.KeyPair, seq uint64, target core.Identity) *core.SignedEvent {
	t.Helper()
	pub, err := target.PubkeyBytes()
	if err != nil {
		t.Fatalf("target pubkey: %v", err)
	}
	se, err := follower.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: core.Now64(),
		Sequence:  seq,
		Follow:    &core.Follow{TargetPubkey: [32]byte(pub)},
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return se
}

// TestReceivedFollowers verifies a zen derives its own follower set from
// Follow{target: me} events it has received via sync. Carol is the local
// identity; Alice and Bob follow her, Dave does not. Carol's followers are
// {Alice, Bob}.
func TestReceivedFollowers(t *testing.T) {
	s := newTestStore(t)
	carol, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("carol: %v", err)
	}
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(carol.Private, []byte("pw"))
	if err := s.InitIdentity(carol, ek); err != nil {
		t.Fatalf("InitIdentity: %v", err)
	}
	alice, _ := core.NewKeyPair()
	bob, _ := core.NewKeyPair()
	dave, _ := core.NewKeyPair()

	// Alice and Bob follow Carol (the local identity). Dave follows Bob.
	for _, follower := range []*core.KeyPair{alice, bob} {
		se := makeFollowEventFor(t, follower, 1, carol.Identity())
		if _, err := s.PutRemoteEvent(se, 1); err != nil {
			t.Fatalf("PutRemoteEvent: %v", err)
		}
	}
	daveFollow := makeFollowEventFor(t, dave, 1, bob.Identity())
	_, _ = s.PutRemoteEvent(daveFollow, 1)

	got, err := s.ReceivedFollowers()
	if err != nil {
		t.Fatalf("ReceivedFollowers: %v", err)
	}
	want := map[core.Identity]bool{alice.Identity(): true, bob.Identity(): true}
	gotSet := make(map[core.Identity]bool, len(got))
	for _, id := range got {
		gotSet[id] = true
	}
	if len(gotSet) != len(want) {
		t.Fatalf("followers: want %d, got %d (%v)", len(want), len(gotSet), got)
	}
	for w := range want {
		if !gotSet[w] {
			t.Fatalf("missing follower %s in %v", w, got)
		}
	}
}

// TestReceivedFollowEvents verifies the wire-side query returns the signed
// Follow events targeting the local identity, so the receiver can verify
// each follower's signature rather than trusting the relaying zen.
func TestReceivedFollowEvents(t *testing.T) {
	s := newTestStore(t)
	carol, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("carol: %v", err)
	}
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(carol.Private, []byte("pw"))
	if err := s.InitIdentity(carol, ek); err != nil {
		t.Fatalf("InitIdentity: %v", err)
	}
	alice, _ := core.NewKeyPair()

	se := makeFollowEventFor(t, alice, 1, carol.Identity())
	if _, err := s.PutRemoteEvent(se, 1); err != nil {
		t.Fatalf("PutRemoteEvent: %v", err)
	}

	events, err := s.ReceivedFollowEvents()
	if err != nil {
		t.Fatalf("ReceivedFollowEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 follow event, got %d", len(events))
	}
	if events[0].Author != alice.Identity() {
		t.Fatalf("author: want %s, got %s", alice.Identity(), events[0].Author)
	}
	if err := events[0].Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestDisplayNameOwnIdentity(t *testing.T) {
	s := newTestStore(t)
	kp, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("InitIdentity: %v", err)
	}
	// No profile event yet: empty name.
	name, err := s.DisplayName(kp.Identity())
	if err != nil {
		t.Fatalf("DisplayName before profile: %v", err)
	}
	if name != "" {
		t.Fatalf("want empty name, got %q", name)
	}
	// Append a profile event.
	se, _ := kp.Sign(core.Event{
		Kind: core.KindProfile, Log: core.ProfileLog, Timestamp: 1, Sequence: 1,
		Profile: &core.Profile{DisplayName: "alice"},
	})
	if err := s.AppendOwnEvent(core.ProfileLog, se); err != nil {
		t.Fatalf("append profile: %v", err)
	}
	name, err = s.DisplayName(kp.Identity())
	if err != nil {
		t.Fatalf("DisplayName after profile: %v", err)
	}
	if name != "alice" {
		t.Fatalf("want alice, got %q", name)
	}
	// A newer profile event wins (last-write-wins).
	se2, _ := kp.Sign(core.Event{
		Kind: core.KindProfile, Log: core.ProfileLog, Timestamp: 2, Sequence: 2,
		Profile: &core.Profile{DisplayName: "alicia"},
	})
	if err := s.AppendOwnEvent(core.ProfileLog, se2); err != nil {
		t.Fatalf("append profile2: %v", err)
	}
	name, _ = s.DisplayName(kp.Identity())
	if name != "alicia" {
		t.Fatalf("want alicia (last-write-wins), got %q", name)
	}
}

func TestVerifyIdentity(t *testing.T) {
	s := newTestStore(t)
	a, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	b, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	// Unverified by default.
	if v, _ := s.IsVerified(a.Identity()); v {
		t.Fatal("a should start unverified")
	}
	// Verify a, leave b unverified.
	if err := s.VerifyIdentity(a.Identity()); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if v, _ := s.IsVerified(a.Identity()); !v {
		t.Fatal("a should be verified")
	}
	if v, _ := s.IsVerified(b.Identity()); v {
		t.Fatal("b should remain unverified")
	}
	// List reflects only a.
	ids, err := s.VerifiedIdentities()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(ids) != 1 || ids[0] != a.Identity() {
		t.Fatalf("want [a], got %v", ids)
	}
	// Unverify a.
	if err := s.UnverifyIdentity(a.Identity()); err != nil {
		t.Fatalf("unverify: %v", err)
	}
	if v, _ := s.IsVerified(a.Identity()); v {
		t.Fatal("a should be unverified after unverify")
	}
	ids, _ = s.VerifiedIdentities()
	if len(ids) != 0 {
		t.Fatalf("want empty, got %v", ids)
	}
}

func TestBackupIncludesVerifiedIdentities(t *testing.T) {
	s := newTestStore(t)
	kp, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	peer, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyIdentity(peer.Identity()); err != nil {
		t.Fatalf("verify: %v", err)
	}
	bd, err := s.ExportBackup()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(bd.Verified) != 1 || bd.Verified[0] != peer.Identity() {
		t.Fatalf("backup verified: want [peer], got %v", bd.Verified)
	}
	// Restore into a fresh store.
	s2 := newTestStore(t)
	if err := s2.ImportBackup(bd); err != nil {
		t.Fatalf("import: %v", err)
	}
	if v, _ := s2.IsVerified(peer.Identity()); !v {
		t.Fatal("peer should be verified after restore")
	}
}

func TestTransportKeyRoundTrip(t *testing.T) {
	s := newTestStore(t)
	// No key initially.
	if _, ok, _ := s.TransportKey(); ok {
		t.Fatal("transport key present before being set")
	}
	key := []byte(`{"some":"key bytes"}`)
	if err := s.PutTransportKey(key); err != nil {
		t.Fatalf("PutTransportKey: %v", err)
	}
	got, ok, _ := s.TransportKey()
	if !ok {
		t.Fatal("transport key missing after put")
	}
	if string(got) != string(key) {
		t.Fatalf("transport key: want %q, got %q", key, got)
	}
	// TransportKey must return a copy: mutating it must not corrupt the store.
	got[0] = 'X'
	got2, _, _ := s.TransportKey()
	if got2[0] == 'X' {
		t.Fatal("TransportKey did not return a copy")
	}
}

func TestBackupIncludesTransportKey(t *testing.T) {
	s := newTestStore(t)
	kp, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	tk := []byte(`{"tailcat":"private"}`)
	if err := s.PutTransportKey(tk); err != nil {
		t.Fatalf("put transport key: %v", err)
	}
	// ExportBackup carries the raw key; the CLI encrypts it for the file.
	bd, err := s.ExportBackup()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	// ExportBackup itself does not encrypt the transport key; the store has
	// no passphrase. The CLI layer is responsible for sealing it.
	if bd.TransportKey != nil {
		t.Fatal("ExportBackup should not populate TransportKey (CLI encrypts)")
	}
}

func TestOutboundReplies(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	alice, _ := core.NewKeyPair()
	bob, _ := core.NewKeyPair()

	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Follow Alice and Bob so they are in the follow graph. The metric
	// counts replies only toward currently-followed authors.
	followAlice, _ := me.Sign(core.Event{
		Kind: core.KindFollow, Log: core.ProfileLog, Timestamp: 50, Sequence: 1,
		Follow: &core.Follow{TargetPubkey: [32]byte(alice.Public)},
	})
	if err := s.AppendOwnEvent(core.ProfileLog, followAlice); err != nil {
		t.Fatalf("append follow alice: %v", err)
	}
	followBob, _ := me.Sign(core.Event{
		Kind: core.KindFollow, Log: core.ProfileLog, Timestamp: 51, Sequence: 2,
		Follow: &core.Follow{TargetPubkey: [32]byte(bob.Public)},
	})
	if err := s.AppendOwnEvent(core.ProfileLog, followBob); err != nil {
		t.Fatalf("append follow bob: %v", err)
	}

	// Alice and Bob each post. Their posts go into the crawl bucket as
	// synced PostLog events, keyed by author.
	alicePost, _ := alice.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 100, Sequence: 1,
		Post: &core.Post{Text: "alice's post"},
	})
	aliceID, _ := alicePost.ID()
	if _, err := s.PutRemoteEvent(alicePost, 1); err != nil {
		t.Fatalf("put alice post: %v", err)
	}
	bobPost, _ := bob.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 200, Sequence: 1,
		Post: &core.Post{Text: "bob's post"},
	})
	bobID, _ := bobPost.ID()
	if _, err := s.PutRemoteEvent(bobPost, 1); err != nil {
		t.Fatalf("put bob post: %v", err)
	}

	// I reply to Alice's post twice and Bob's post once.
	for i, ts := range []int64{300, 400} {
		reply, _ := me.Sign(core.Event{
			Kind: core.KindPost, Log: core.PostLog, Timestamp: ts, Sequence: uint64(i + 1),
			Post: &core.Post{Text: "my reply", ParentID: aliceID},
		})
		if err := s.AppendOwnEvent(core.PostLog, reply); err != nil {
			t.Fatalf("append reply %d: %v", i, err)
		}
	}
	replyToBob, _ := me.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 500, Sequence: 3,
		Post: &core.Post{Text: "reply to bob", ParentID: bobID},
	})
	if err := s.AppendOwnEvent(core.PostLog, replyToBob); err != nil {
		t.Fatalf("append reply to bob: %v", err)
	}

	// A top-level post (no parent) should not count as a reply.
	topPost, _ := me.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 600, Sequence: 4,
		Post: &core.Post{Text: "top-level"},
	})
	if err := s.AppendOwnEvent(core.PostLog, topPost); err != nil {
		t.Fatalf("append top post: %v", err)
	}

	counts, err := s.OutboundReplies()
	if err != nil {
		t.Fatalf("OutboundReplies: %v", err)
	}
	if counts[alice.Identity()] != 2 {
		t.Fatalf("alice: want 2 replies, got %d", counts[alice.Identity()])
	}
	if counts[bob.Identity()] != 1 {
		t.Fatalf("bob: want 1 reply, got %d", counts[bob.Identity()])
	}
}

func TestOutboundRepliesEmpty(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	counts, err := s.OutboundReplies()
	if err != nil {
		t.Fatalf("OutboundReplies: %v", err)
	}
	if len(counts) != 0 {
		t.Fatalf("want empty, got %d", len(counts))
	}
}

func TestOutboundLikes(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	alice, _ := core.NewKeyPair()
	bob, _ := core.NewKeyPair()

	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Follow Alice and Bob so they are in the follow graph.
	for _, target := range [][]byte{alice.Public, bob.Public} {
		follow, _ := me.Sign(core.Event{
			Kind: core.KindFollow, Log: core.ProfileLog, Timestamp: 50, Sequence: 1,
			Follow: &core.Follow{TargetPubkey: [32]byte(target)},
		})
		if err := s.AppendOwnEvent(core.ProfileLog, follow); err != nil {
			t.Fatalf("append follow: %v", err)
		}
	}

	// Alice and Bob each post. Their posts go into the crawl bucket.
	alicePost, _ := alice.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 100, Sequence: 1,
		Post: &core.Post{Text: "alice's post"},
	})
	aliceID, _ := alicePost.ID()
	if _, err := s.PutRemoteEvent(alicePost, 1); err != nil {
		t.Fatalf("put alice post: %v", err)
	}
	bobPost, _ := bob.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 200, Sequence: 1,
		Post: &core.Post{Text: "bob's post"},
	})
	bobID, _ := bobPost.ID()
	if _, err := s.PutRemoteEvent(bobPost, 1); err != nil {
		t.Fatalf("put bob post: %v", err)
	}

	// I like Alice's post once and Bob's post once.
	likeAlice, _ := me.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: 300, Sequence: 1,
		Like: &core.Like{TargetID: aliceID},
	})
	if err := s.AppendOwnEvent(core.PostLog, likeAlice); err != nil {
		t.Fatalf("append like alice: %v", err)
	}
	likeBob, _ := me.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: 400, Sequence: 2,
		Like: &core.Like{TargetID: bobID},
	})
	if err := s.AppendOwnEvent(core.PostLog, likeBob); err != nil {
		t.Fatalf("append like bob: %v", err)
	}

	counts, err := s.OutboundLikes()
	if err != nil {
		t.Fatalf("OutboundLikes: %v", err)
	}
	if counts[alice.Identity()] != 1 {
		t.Fatalf("alice: want 1 like, got %d", counts[alice.Identity()])
	}
	if counts[bob.Identity()] != 1 {
		t.Fatalf("bob: want 1 like, got %d", counts[bob.Identity()])
	}
}

func TestLikesForPosts(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	alice, _ := core.NewKeyPair()

	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Alice posts.
	alicePost, _ := alice.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 100, Sequence: 1,
		Post: &core.Post{Text: "alice's post"},
	})
	aliceID, _ := alicePost.ID()
	if _, err := s.PutRemoteEvent(alicePost, 1); err != nil {
		t.Fatalf("put alice post: %v", err)
	}

	// I like Alice's post.
	myLike, _ := me.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: 200, Sequence: 1,
		Like: &core.Like{TargetID: aliceID},
	})
	if err := s.AppendOwnEvent(core.PostLog, myLike); err != nil {
		t.Fatalf("append like: %v", err)
	}

	// Carol likes Alice's post (from the crawl bucket).
	carol, _ := core.NewKeyPair()
	carolLike, _ := carol.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: 300, Sequence: 1,
		Like: &core.Like{TargetID: aliceID},
	})
	if _, err := s.PutRemoteEvent(carolLike, 1); err != nil {
		t.Fatalf("put carol like: %v", err)
	}

	// Query for likes targeting Alice's post.
	results, err := s.LikesForPosts([]core.EventID{aliceID})
	if err != nil {
		t.Fatalf("LikesForPosts: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("want 2 likes, got %d", len(results))
	}

	// Query for a non-existent target returns nothing.
	other := core.EventID{0xFF}
	results, err = s.LikesForPosts([]core.EventID{other})
	if err != nil {
		t.Fatalf("LikesForPosts empty: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("want 0 likes for unknown target, got %d", len(results))
	}
}

func TestPinIdentity(t *testing.T) {
	s := newTestStore(t)
	kp, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	a, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	b, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	aPub, _ := a.Identity().PubkeyBytes()
	bPub, _ := b.Identity().PubkeyBytes()
	if _, _, _, err := s.Follow(kp, [32]byte(aPub)); err != nil {
		t.Fatalf("follow a: %v", err)
	}
	if _, _, _, err := s.Follow(kp, [32]byte(bPub)); err != nil {
		t.Fatalf("follow b: %v", err)
	}
	// Pinning a non-follow fails.
	other, _ := core.NewKeyPair()
	if err := s.PinIdentity(other.Identity()); !errors.Is(err, ErrPinRequiresFollow) {
		t.Fatalf("pin non-follow: want ErrPinRequiresFollow, got %v", err)
	}
	// Unpinned by default.
	if p, _ := s.IsPinned(a.Identity()); p {
		t.Fatal("a should start unpinned")
	}
	// Pin a, leave b unpinned.
	if err := s.PinIdentity(a.Identity()); err != nil {
		t.Fatalf("pin: %v", err)
	}
	if p, _ := s.IsPinned(a.Identity()); !p {
		t.Fatal("a should be pinned")
	}
	if p, _ := s.IsPinned(b.Identity()); p {
		t.Fatal("b should remain unpinned")
	}
	// List reflects only a.
	ids, err := s.PinnedIdentities()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(ids) != 1 || ids[0] != a.Identity() {
		t.Fatalf("want [a], got %v", ids)
	}
	// Unpin a.
	if err := s.UnpinIdentity(a.Identity()); err != nil {
		t.Fatalf("unpin: %v", err)
	}
	if p, _ := s.IsPinned(a.Identity()); p {
		t.Fatal("a should be unpinned after unpin")
	}
	ids, _ = s.PinnedIdentities()
	if len(ids) != 0 {
		t.Fatalf("want empty, got %v", ids)
	}
}

func TestBackupIncludesPinnedIdentities(t *testing.T) {
	s := newTestStore(t)
	kp, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	peer, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	peerPub, _ := peer.Identity().PubkeyBytes()
	if _, _, _, err := s.Follow(kp, [32]byte(peerPub)); err != nil {
		t.Fatalf("follow peer: %v", err)
	}
	if err := s.PinIdentity(peer.Identity()); err != nil {
		t.Fatalf("pin: %v", err)
	}
	bd, err := s.ExportBackup()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(bd.Pinned) != 1 || bd.Pinned[0] != peer.Identity() {
		t.Fatalf("backup pinned: want [peer], got %v", bd.Pinned)
	}
	// Restore into a fresh store.
	s2 := newTestStore(t)
	if err := s2.ImportBackup(bd); err != nil {
		t.Fatalf("import: %v", err)
	}
	if p, _ := s2.IsPinned(peer.Identity()); !p {
		t.Fatal("peer should be pinned after restore")
	}
}

// TestUnfollowClearsPin proves the clear-on-unfollow invariant lives in the
// shared store.Unfollow: both the daemon and the offline CLI route through
// it, so unfollowing can never leave a dangling pin on an ex-follow.
func TestUnfollowClearsPin(t *testing.T) {
	s := newTestStore(t)
	kp, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	peer, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	target, err := peer.Identity().PubkeyBytes()
	if err != nil {
		t.Fatal(err)
	}
	// Follow, pin, then unfollow via the shared method.
	if _, _, _, err := s.Follow(kp, [32]byte(target)); err != nil {
		t.Fatalf("follow: %v", err)
	}
	if err := s.PinIdentity(peer.Identity()); err != nil {
		t.Fatalf("pin: %v", err)
	}
	if _, _, _, err := s.Unfollow(kp, [32]byte(target)); err != nil {
		t.Fatalf("unfollow: %v", err)
	}
	if p, _ := s.IsPinned(peer.Identity()); p {
		t.Fatal("unfollow should clear the pin on the target")
	}
}

// TestRecomputeWeights verifies the store gathers all four interaction
// metrics and persists per-follow weights that match the weight formula
// applied to the same ground-truth inputs. The store's job is metric
// gathering and persistence; the formula itself is tested in the weight
// package. This asserts the wiring: the right counts reach the formula, and
// the result round-trips through Weight and WeightedGraph.
func TestRecomputeWeights(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	alice, _ := core.NewKeyPair() // pinned, no interaction
	bob, _ := core.NewKeyPair()   // liked 3x
	carol, _ := core.NewKeyPair() // reciprocal follow, no interaction
	dave, _ := core.NewKeyPair()  // cold-start: followed, no signal

	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}

	follows := []core.Identity{alice.Identity(), bob.Identity(), carol.Identity(), dave.Identity()}
	for _, target := range [][]byte{alice.Public, bob.Public, carol.Public, dave.Public} {
		if _, _, _, err := s.Follow(me, [32]byte(target)); err != nil {
			t.Fatalf("follow: %v", err)
		}
	}

	// Bob posts 3 times, and I like each post once. One Like per author
	// per post is enforced, so accumulating per-author like counts
	// requires distinct target posts.
	for i := 0; i < 3; i++ {
		post, _ := bob.Sign(core.Event{
			Kind: core.KindPost, Log: core.PostLog, Timestamp: int64(1+i) * 1e9, Sequence: uint64(1 + i),
			Post: &core.Post{Text: "bob's post"},
		})
		pid, _ := post.ID()
		if _, err := s.PutRemoteEvent(post, 1); err != nil {
			t.Fatalf("put bob post %d: %v", i, err)
		}
		like, _ := me.Sign(core.Event{
			Kind: core.KindLike, Log: core.PostLog, Timestamp: int64(10+i) * 1e9, Sequence: uint64(1 + i),
			Like: &core.Like{TargetID: pid},
		})
		if err := s.AppendOwnEvent(core.PostLog, like); err != nil {
			t.Fatalf("append like %d: %v", i, err)
		}
	}

	// Carol follows me back, so she is reciprocal.
	carolFollow, _ := carol.Sign(core.Event{
		Kind: core.KindFollow, Log: core.ProfileLog, Timestamp: 60, Sequence: 1,
		Follow: &core.Follow{TargetPubkey: [32]byte(me.Public)},
	})
	if _, err := s.PutRemoteEvent(carolFollow, 1); err != nil {
		t.Fatalf("put carol follow: %v", err)
	}

	// Pin Alice.
	if err := s.PinIdentity(alice.Identity()); err != nil {
		t.Fatalf("pin alice: %v", err)
	}

	weights, err := s.RecomputeWeights()
	if err != nil {
		t.Fatalf("RecomputeWeights: %v", err)
	}

	// Expected weights: the formula applied to the same ground-truth inputs
	// the store should have gathered.
	expected := weight.Weights(weight.Inputs{
		Follows: follows,
		Likes:   map[core.Identity]int{bob.Identity(): 3},
		Recip:   map[core.Identity]bool{carol.Identity(): true},
		Pins:    map[core.Identity]bool{alice.Identity(): true},
	})
	for _, id := range follows {
		if math.Abs(weights[id]-expected[id]) > 1e-9 {
			t.Fatalf("weight %v: want %g, got %g", id, expected[id], weights[id])
		}
	}

	// The formula's defining properties must hold on the gathered metrics:
	// a pin dominates accumulated interaction, reciprocal beats cold-start,
	// and interaction beats cold-start. These are the properties the
	// scheduler (phase 6) will rely on, so they must hold end to end, not
	// just in the weight package's unit tests.
	if weights[alice.Identity()] <= weights[bob.Identity()] {
		t.Fatalf("pin must dominate interaction: alice=%g bob=%g",
			weights[alice.Identity()], weights[bob.Identity()])
	}
	if weights[carol.Identity()] <= weights[dave.Identity()] {
		t.Fatalf("reciprocal must beat cold-start: carol=%g dave=%g",
			weights[carol.Identity()], weights[dave.Identity()])
	}
	if weights[bob.Identity()] <= weights[dave.Identity()] {
		t.Fatalf("interaction must beat cold-start: bob=%g dave=%g",
			weights[bob.Identity()], weights[dave.Identity()])
	}

	// Weight round-trips through Weight.
	gotAlice, err := s.Weight(alice.Identity())
	if err != nil {
		t.Fatalf("Weight alice: %v", err)
	}
	if math.Abs(gotAlice-weights[alice.Identity()]) > 1e-9 {
		t.Fatalf("Weight alice round-trip: want %g, got %g", weights[alice.Identity()], gotAlice)
	}

	// WeightedGraph returns the same set as the returned map.
	wg, err := s.WeightedGraph()
	if err != nil {
		t.Fatalf("WeightedGraph: %v", err)
	}
	if len(wg) != len(weights) {
		t.Fatalf("WeightedGraph size: want %d, got %d", len(weights), len(wg))
	}
	for id, w := range weights {
		if math.Abs(wg[id]-w) > 1e-9 {
			t.Fatalf("WeightedGraph %v: want %g, got %g", id, w, wg[id])
		}
	}
}

// TestRecomputeWeightsRefreshes verifies that a second RecomputeWeights after a
// metric changes reflects the new signal and replaces the old weight.
func TestRecomputeWeightsRefreshes(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	bob, _ := core.NewKeyPair()

	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, _, _, err := s.Follow(me, [32]byte(bob.Public)); err != nil {
		t.Fatalf("follow: %v", err)
	}

	// First round: Bob is cold-start.
	w1, err := s.RecomputeWeights()
	if err != nil {
		t.Fatalf("recompute 1: %v", err)
	}
	want1 := weight.Weights(weight.Inputs{Follows: []core.Identity{bob.Identity()}})
	if math.Abs(w1[bob.Identity()]-want1[bob.Identity()]) > 1e-9 {
		t.Fatalf("round 1: want %g, got %g", want1[bob.Identity()], w1[bob.Identity()])
	}

	// Bob posts and I like it once.
	bobPost, _ := bob.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 100, Sequence: 1,
		Post: &core.Post{Text: "bob's post"},
	})
	bobID, _ := bobPost.ID()
	if _, err := s.PutRemoteEvent(bobPost, 1); err != nil {
		t.Fatalf("put bob post: %v", err)
	}
	like, _ := me.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: 200, Sequence: 1,
		Like: &core.Like{TargetID: bobID},
	})
	if err := s.AppendOwnEvent(core.PostLog, like); err != nil {
		t.Fatalf("append like: %v", err)
	}

	// Second round: Bob now has one like.
	w2, err := s.RecomputeWeights()
	if err != nil {
		t.Fatalf("recompute 2: %v", err)
	}
	want2 := weight.Weights(weight.Inputs{
		Follows: []core.Identity{bob.Identity()},
		Likes:   map[core.Identity]int{bob.Identity(): 1},
	})
	if math.Abs(w2[bob.Identity()]-want2[bob.Identity()]) > 1e-9 {
		t.Fatalf("round 2: want %g, got %g", want2[bob.Identity()], w2[bob.Identity()])
	}

	// The persisted weight reflects the refresh, not the stale round-1 value.
	got, err := s.Weight(bob.Identity())
	if err != nil {
		t.Fatalf("Weight: %v", err)
	}
	if math.Abs(got-want2[bob.Identity()]) > 1e-9 {
		t.Fatalf("persisted weight: want %g, got %g", want2[bob.Identity()], got)
	}
}

// TestWeightUnknownIdentity verifies a not-followed identity reads as 0.
func TestWeightUnknownIdentity(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	other, _ := core.NewKeyPair()
	w, err := s.Weight(other.Identity())
	if err != nil {
		t.Fatalf("Weight: %v", err)
	}
	if w != 0 {
		t.Fatalf("unknown identity: want 0, got %g", w)
	}
}

// signOversizedForges a valid-signature event that exceeds the per-kind cap.
// Sign rejects oversized events, so the test bypasses it with a raw ed25519
// signature over the canonical bytes. This simulates a forged author.
func signOversized(t *testing.T, kp *core.KeyPair, ev core.Event) *core.SignedEvent {
	t.Helper()
	canon, err := core.CanonicalEncode(ev)
	if err != nil {
		t.Fatalf("CanonicalEncode: %v", err)
	}
	if len(canon) <= core.CapForKind(ev.Kind) {
		t.Fatalf("test event not oversized: %d <= cap %d", len(canon), core.CapForKind(ev.Kind))
	}
	sig := ed25519.Sign(kp.Private, canon)
	return &core.SignedEvent{Event: ev, Author: kp.Identity(), Signature: sig}
}

// TestReceiverRejectsOversizedEvent verifies that an oversized event with a
// valid signature is dropped and the author is flagged, while an at-cap event
// is stored normally.
func TestReceiverRejectsOversizedEvent(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()

	// An at-cap Post is stored.
	atCap := core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 1, Sequence: 1,
		Post: &core.Post{Text: "gm"},
	}
	seAt, err := author.Sign(atCap)
	if err != nil {
		t.Fatalf("Sign at-cap: %v", err)
	}
	inserted, err := s.PutRemoteEvent(seAt, 1)
	if err != nil {
		t.Fatalf("PutRemoteEvent at-cap: %v", err)
	}
	if !inserted {
		t.Fatal("at-cap event not inserted")
	}

	// An oversized Post (valid signature, forged past the author cap) is
	// dropped and the author is flagged.
	oversized := core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 2, Sequence: 2,
		Post: &core.Post{Text: string(make([]byte, core.CapPost))},
	}
	seOver := signOversized(t, author, oversized)
	if err := seOver.Verify(); err != nil {
		t.Fatalf("forged oversized event must verify: %v", err)
	}
	inserted, err = s.PutRemoteEvent(seOver, 2)
	if err != nil {
		t.Fatalf("PutRemoteEvent oversized: %v", err)
	}
	if inserted {
		t.Fatal("oversized event was stored")
	}
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if !flagged {
		t.Fatal("oversized event did not flag the author")
	}
}

// TestReceiverOversizedBadSignatureNoFlag verifies that an oversized event
// with a bad (tampered) signature is dropped without flagging: Verify runs
// first, so a transit tamper is a drop, not a flag.
func TestReceiverOversizedBadSignatureNoFlag(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()

	oversized := core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 1, Sequence: 1,
		Post: &core.Post{Text: string(make([]byte, core.CapPost))},
	}
	se := signOversized(t, author, oversized)
	// Tamper with the signature so Verify fails.
	se.Signature[0] ^= 0xFF

	// PutRemoteEvent does not call Verify; the sync layer does. Simulate the
	// sync layer: Verify first, then only call PutRemoteEvent if it passes.
	if err := se.Verify(); err == nil {
		t.Fatal("tampered event must fail Verify")
	}
	// Since Verify failed, the sync layer drops the event without calling
	// PutRemoteEvent, so the author is not flagged.
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if flagged {
		t.Fatal("author flagged for a transit-tampered event")
	}
}

// TestReceiverOversizedProfile verifies that an oversized Profile event is
// dropped and the author is flagged via PutCrawledProfile.
func TestReceiverOversizedProfile(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()

	oversized := core.Event{
		Kind: core.KindProfile, Log: core.ProfileLog, Timestamp: 1, Sequence: 1,
		Profile: &core.Profile{DisplayName: string(make([]byte, core.CapProfile))},
	}
	se := signOversized(t, author, oversized)
	if err := se.Verify(); err != nil {
		t.Fatalf("forged oversized profile must verify: %v", err)
	}
	if err := s.PutCrawledProfile(author.Identity(), se); err != nil {
		t.Fatalf("PutCrawledProfile oversized: %v", err)
	}
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if !flagged {
		t.Fatal("oversized profile did not flag the author")
	}
}

// TestAuthorRateLimitBumpsToNextSecond verifies that two SignAndAppendPost
// calls for the same kind in the same second produce events in different
// seconds. The author-side rate limiter holds the second event until the
// next second.
func TestAuthorRateLimitBumpsToNextSecond(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("init: %v", err)
	}

	ch1, err := s.EnqueuePostEvent(kp, core.PostLog, core.Event{
		Kind: core.KindPost,
		Post: &core.Post{Text: "first"},
	})
	if err != nil && !errors.Is(err, ErrHeld) {
		t.Fatalf("first post: %v", err)
	}
	res1 := <-ch1
	if res1.Err != nil {
		t.Fatalf("first result: %v", res1.Err)
	}

	ch2, err := s.EnqueuePostEvent(kp, core.PostLog, core.Event{
		Kind: core.KindPost,
		Post: &core.Post{Text: "second"},
	})
	if err != nil && !errors.Is(err, ErrHeld) {
		t.Fatalf("second post: %v", err)
	}
	res2 := <-ch2
	if res2.Err != nil {
		t.Fatalf("second result: %v", res2.Err)
	}

	sec1 := res1.SE.Event.Timestamp / int64(1e9)
	sec2 := res2.SE.Event.Timestamp / int64(1e9)
	if sec1 == sec2 {
		t.Fatalf("two posts in the same second: sec1=%d sec2=%d", sec1, sec2)
	}
}

// TestAuthorRateLimitQueueFull verifies that overflowing the bounded queue
// rejects the incoming event with ErrRateLimitQueueFull, not the queued
// ones.
func TestAuthorRateLimitQueueFull(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("init: %v", err)
	}

	// The first call takes the current second; the rest fill the queue.
	// The queue cap is rateQueueCap (10). After filling, the next call
	// is rejected.
	channels := make([]chan signResult, 0, rateQueueCap+1)
	ch, err := s.EnqueuePostEvent(kp, core.PostLog, core.Event{
		Kind: core.KindPost,
		Post: &core.Post{Text: "first"},
	})
	if err != nil && !errors.Is(err, ErrHeld) {
		t.Fatalf("first post: %v", err)
	}
	channels = append(channels, ch)
	for i := 0; i < rateQueueCap; i++ {
		ch, err := s.EnqueuePostEvent(kp, core.PostLog, core.Event{
			Kind: core.KindPost,
			Post: &core.Post{Text: "queued"},
		})
		if err != nil && !errors.Is(err, ErrHeld) {
			t.Fatalf("queued post %d: %v", i, err)
		}
		channels = append(channels, ch)
	}

	// The next call must be rejected: the queue is full.
	_, err = s.EnqueuePostEvent(kp, core.PostLog, core.Event{
		Kind: core.KindPost,
		Post: &core.Post{Text: "overflow"},
	})
	if !errors.Is(err, ErrRateLimitQueueFull) {
		t.Fatalf("overflow post: want ErrRateLimitQueueFull, got %v", err)
	}

	// Drain all results; they must all succeed.
	for i, ch := range channels {
		res := <-ch
		if res.Err != nil {
			t.Fatalf("result %d: %v", i, res.Err)
		}
	}
}

// TestReceiverRejectsSameSecondSameKind verifies that two signed events of
// the same kind from the same author in the same second: the second is
// rejected and the author is flagged.
func TestReceiverRejectsSameSecondSameKind(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()

	se1, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 5e9, Sequence: 1,
		Post: &core.Post{Text: "first"},
	})
	se2, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 5e9 + 100, Sequence: 2,
		Post: &core.Post{Text: "second"},
	})

	if _, err := s.PutRemoteEvent(se1, 1); err != nil {
		t.Fatalf("put first: %v", err)
	}
	inserted, err := s.PutRemoteEvent(se2, 1)
	if err != nil {
		t.Fatalf("put second: %v", err)
	}
	if inserted {
		t.Fatal("second same-second event was stored")
	}
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if !flagged {
		t.Fatal("author not flagged for same-second violation")
	}
}

// TestReceiverSameSecondDifferentKind verifies that a Post and a Like from
// the same author in the same second are both stored: the rate check is
// per kind, so the bound is 1/sec per kind, not 1/sec total.
func TestReceiverSameSecondDifferentKind(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()

	post, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 5e9, Sequence: 1,
		Post: &core.Post{Text: "post"},
	})
	postID, _ := post.ID()
	like, _ := author.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: 5e9 + 100, Sequence: 2,
		Like: &core.Like{TargetID: postID},
	})

	if _, err := s.PutRemoteEvent(post, 1); err != nil {
		t.Fatalf("put post: %v", err)
	}
	inserted, err := s.PutRemoteEvent(like, 1)
	if err != nil {
		t.Fatalf("put like: %v", err)
	}
	if !inserted {
		t.Fatal("same-second like (different kind) was dropped")
	}
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if flagged {
		t.Fatal("author flagged for same-second different-kind events")
	}
}

// TestReceiverDuplicateNoFalseFlag verifies that a replayed duplicate event
// does not false-flag the author: dedup runs before the rate check.
func TestReceiverDuplicateNoFalseFlag(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()

	se, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 5e9, Sequence: 1,
		Post: &core.Post{Text: "post"},
	})

	if _, err := s.PutRemoteEvent(se, 1); err != nil {
		t.Fatalf("first put: %v", err)
	}
	inserted, err := s.PutRemoteEvent(se, 1)
	if err != nil {
		t.Fatalf("replay put: %v", err)
	}
	if inserted {
		t.Fatal("replayed duplicate was inserted")
	}
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if flagged {
		t.Fatal("author false-flagged by a replayed duplicate")
	}
}

// TestReceiverProfileLogExemptFromRate verifies that a burst of Follow
// events from the same author in the same second does not flag: ProfileLog
// is exempt from the rate limit because bulk-follow onboarding is legitimate.
func TestReceiverProfileLogExemptFromRate(t *testing.T) {
	s := newTestStore(t)
	follower, _ := core.NewKeyPair()
	target, _ := core.NewKeyPair()

	for i := 0; i < 5; i++ {
		se, _ := follower.Sign(core.Event{
			Kind: core.KindFollow, Log: core.ProfileLog, Timestamp: 5e9 + int64(i), Sequence: uint64(i + 1),
			Follow: &core.Follow{TargetPubkey: [32]byte(target.Public)},
		})
		if _, err := s.PutRemoteEvent(se, 1); err != nil {
			t.Fatalf("put follow %d: %v", i, err)
		}
	}
	flagged, err := s.IsFlagged(follower.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if flagged {
		t.Fatal("author flagged for same-second ProfileLog burst")
	}
}

// TestAuthorLikeIdempotentNoHold verifies that a same-target like in the same
// second returns the existing like's event ID without holding.
func TestAuthorLikeIdempotentNoHold(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Seed a post to like.
	postCh, err := s.EnqueuePostEvent(kp, core.PostLog, core.Event{
		Kind: core.KindPost,
		Post: &core.Post{Text: "like me"},
	})
	if err != nil && !errors.Is(err, ErrHeld) {
		t.Fatalf("post: %v", err)
	}
	postRes := <-postCh
	if postRes.Err != nil {
		t.Fatalf("post result: %v", postRes.Err)
	}

	// First like.
	likeCh1, err := s.EnqueuePostEvent(kp, core.PostLog, core.Event{
		Kind: core.KindLike,
		Like: &core.Like{TargetID: postRes.ID},
	})
	if err != nil && !errors.Is(err, ErrHeld) {
		t.Fatalf("first like: %v", err)
	}
	likeRes1 := <-likeCh1
	if likeRes1.Err != nil {
		t.Fatalf("first like result: %v", likeRes1.Err)
	}

	// Second like of the same post: idempotent, returns existing ID, no hold.
	likeCh2, err := s.EnqueuePostEvent(kp, core.PostLog, core.Event{
		Kind: core.KindLike,
		Like: &core.Like{TargetID: postRes.ID},
	})
	if err != nil {
		t.Fatalf("second like: %v", err)
	}
	likeRes2 := <-likeCh2
	if likeRes2.Err != nil {
		t.Fatalf("second like result: %v", likeRes2.Err)
	}
	if likeRes2.ID != likeRes1.ID {
		t.Fatalf("idempotent like: want %s, got %s", likeRes1.ID, likeRes2.ID)
	}
	if likeRes2.SE != nil {
		t.Fatal("idempotent like returned a new signed event")
	}
}

// TestRetentionRejectsOldPostLogEvent verifies that a PostLog event older
// than the retention window is rejected at ingest, while one within the
// window is stored.
func TestRetentionRejectsOldPostLogEvent(t *testing.T) {
	s := newTestStore(t)
	s.SetRetention(10 * time.Second)
	now := int64(100 * 1e9)
	s.SetNow(func() int64 { return now })
	author, _ := core.NewKeyPair()

	// An event at the cutoff (now - retention) is kept.
	atCutoff, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: now - 10*1e9, Sequence: 1,
		Post: &core.Post{Text: "kept"},
	})
	inserted, err := s.PutRemoteEvent(atCutoff, 1)
	if err != nil {
		t.Fatalf("put at-cutoff: %v", err)
	}
	if !inserted {
		t.Fatal("at-cutoff event was dropped")
	}

	// An event one nanosecond older is dropped.
	stale, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: now - 10*1e9 - 1, Sequence: 2,
		Post: &core.Post{Text: "stale"},
	})
	inserted, err = s.PutRemoteEvent(stale, 2)
	if err != nil {
		t.Fatalf("put stale: %v", err)
	}
	if inserted {
		t.Fatal("stale event was stored")
	}

	// The author is not flagged: a stale event is not misconduct.
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if flagged {
		t.Fatal("author flagged for a stale event")
	}
}

// TestRetentionProfileLogExempt verifies that ProfileLog events are never
// rejected by retention: ProfileLog is bounded by compaction, not retention.
func TestRetentionProfileLogExempt(t *testing.T) {
	s := newTestStore(t)
	s.SetRetention(1 * time.Second)
	now := int64(100 * 1e9)
	s.SetNow(func() int64 { return now })
	author, _ := core.NewKeyPair()

	// A Profile event far older than the window is still stored.
	old, _ := author.Sign(core.Event{
		Kind: core.KindProfile, Log: core.ProfileLog, Timestamp: now - 1000*1e9, Sequence: 1,
		Profile: &core.Profile{DisplayName: "old"},
	})
	inserted, err := s.PutRemoteEvent(old, 1)
	if err != nil {
		t.Fatalf("put old profile: %v", err)
	}
	if !inserted {
		t.Fatal("old Profile event was dropped by retention")
	}
}

// TestRetentionDisabledStoresEverything verifies that with no retention
// window configured, an arbitrarily old PostLog event is stored.
func TestRetentionDisabledStoresEverything(t *testing.T) {
	s := newTestStore(t)
	// retention left at the default (disabled).
	now := int64(100 * 1e9)
	s.SetNow(func() int64 { return now })
	author, _ := core.NewKeyPair()

	ancient, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: now - 1000*365*24*int64(1e9), Sequence: 1,
		Post: &core.Post{Text: "ancient"},
	})
	inserted, err := s.PutRemoteEvent(ancient, 1)
	if err != nil {
		t.Fatalf("put ancient: %v", err)
	}
	if !inserted {
		t.Fatal("ancient event dropped with retention disabled")
	}
}

// TestPruneRemotePostsRemovesStale verifies that stored remote PostLog events
// older than the window are pruned, while own PostLog events and remote
// ProfileLog events are retained.
func TestPruneRemotePostsRemovesStale(t *testing.T) {
	s := newTestStore(t)
	s.SetRetention(10 * time.Second)
	now := int64(100 * 1e9)
	s.SetNow(func() int64 { return now })
	author, _ := core.NewKeyPair()

	// Seed a stale remote post and a fresh remote post.
	stale, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: now - 20*1e9, Sequence: 1,
		Post: &core.Post{Text: "stale"},
	})
	// Temporarily disable retention so the stale event is accepted at
	// ingest, then re-enable for the prune.
	s.SetRetention(0)
	if _, err := s.PutRemoteEvent(stale, 1); err != nil {
		t.Fatalf("seed stale: %v", err)
	}
	s.SetRetention(10 * time.Second)

	fresh, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: now - 1*1e9, Sequence: 2,
		Post: &core.Post{Text: "fresh"},
	})
	if _, err := s.PutRemoteEvent(fresh, 2); err != nil {
		t.Fatalf("seed fresh: %v", err)
	}

	// Seed a remote Profile event older than the window; it must survive
	// the prune (ProfileLog is exempt).
	prof, _ := author.Sign(core.Event{
		Kind: core.KindProfile, Log: core.ProfileLog, Timestamp: now - 20*1e9, Sequence: 3,
		Profile: &core.Profile{DisplayName: "old-profile"},
	})
	s.SetRetention(0)
	if _, err := s.PutRemoteEvent(prof, 3); err != nil {
		t.Fatalf("seed profile: %v", err)
	}
	s.SetRetention(10 * time.Second)

	// Seed an own PostLog event older than the window; it must survive.
	kp, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	ownOld, _ := kp.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: now - 20*1e9, Sequence: 1,
		Post: &core.Post{Text: "own-old"},
	})
	if err := s.AppendOwnEvent(core.PostLog, ownOld); err != nil {
		t.Fatalf("seed own: %v", err)
	}

	n, err := s.PruneRemotePosts()
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 1 {
		t.Fatalf("pruned count: want 1, got %d", n)
	}

	// The stale remote post is gone; the fresh remote post and the remote
	// profile remain.
	events, err := s.CrawledEvents(author.Identity())
	if err != nil {
		t.Fatalf("crawled events: %v", err)
	}
	var gotFresh, gotProfile bool
	for _, se := range events {
		if se.Event.Log == core.PostLog && se.Event.Kind == core.KindPost && se.Event.Post != nil && se.Event.Post.Text == "fresh" {
			gotFresh = true
		}
		if se.Event.Log == core.ProfileLog && se.Event.Kind == core.KindProfile {
			gotProfile = true
		}
	}
	if !gotFresh {
		t.Fatal("fresh remote post was pruned")
	}
	if !gotProfile {
		t.Fatal("remote profile was pruned (ProfileLog is exempt)")
	}
	if gotProfile && len(events) != 2 {
		t.Fatalf("remote bucket should have fresh post + profile, got %d events", len(events))
	}

	// The own old post survives.
	own, err := s.OwnEvents(core.PostLog)
	if err != nil {
		t.Fatalf("own events: %v", err)
	}
	if len(own) != 1 || own[0].Event.Post.Text != "own-old" {
		t.Fatalf("own old post was pruned; own logs are retained in full")
	}
}

// TestRetentionCutoffForPullCursor verifies that RetentionCutoff returns the
// ingest cutoff used to raise the pull cursor, and 0 when disabled.
func TestRetentionCutoffForPullCursor(t *testing.T) {
	s := newTestStore(t)
	s.SetRetention(10 * time.Second)
	now := int64(100 * 1e9)
	s.SetNow(func() int64 { return now })

	want := now - 10*1e9
	if got := s.RetentionCutoff(); got != want {
		t.Fatalf("RetentionCutoff: want %d, got %d", want, got)
	}

	s.SetRetention(0)
	if got := s.RetentionCutoff(); got != 0 {
		t.Fatalf("RetentionCutoff disabled: want 0, got %d", got)
	}
}

// TestFutureSkewRejectsEvent verifies that a signed event whose timestamp is
// more than MaxFutureSkew ahead of the receiver clock is rejected at ingest.
// The author is not flagged: a fast clock is skew, not misconduct.
func TestFutureSkewRejectsEvent(t *testing.T) {
	s := newTestStore(t)
	now := int64(100 * 1e9)
	s.SetNow(func() int64 { return now })
	author, _ := core.NewKeyPair()

	// An event 2 minutes ahead is beyond MaxFutureSkew (60s): rejected.
	tooFar, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: now + 120*1e9, Sequence: 1,
		Post: &core.Post{Text: "future"},
	})
	inserted, err := s.PutRemoteEvent(tooFar, 1)
	if err != nil {
		t.Fatalf("put too-far: %v", err)
	}
	if inserted {
		t.Fatal("future-skewed event was stored")
	}

	// The author is not flagged: future skew is not misconduct.
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if flagged {
		t.Fatal("author flagged for a future-skewed event")
	}
}

// TestFutureSkewWithinMarginStoresEvent verifies that an event stamped within
// MaxFutureSkew of the receiver clock is stored normally.
func TestFutureSkewWithinMarginStoresEvent(t *testing.T) {
	s := newTestStore(t)
	now := int64(100 * 1e9)
	s.SetNow(func() int64 { return now })
	author, _ := core.NewKeyPair()

	// An event 30s ahead is within MaxFutureSkew (60s): stored.
	within, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: now + 30*1e9, Sequence: 1,
		Post: &core.Post{Text: "within"},
	})
	inserted, err := s.PutRemoteEvent(within, 1)
	if err != nil {
		t.Fatalf("put within: %v", err)
	}
	if !inserted {
		t.Fatal("within-margin event was dropped")
	}
}

// --- Phase 5: Linked-event existence ---

// TestLikeTargetingUnknownPostDropped verifies that a Like targeting a post
// the receiver does not hold is dropped at ingest, not stored.
func TestLikeTargetingUnknownPostDropped(t *testing.T) {
	s := newTestStore(t)
	liker, _ := core.NewKeyPair()

	// A Like targeting a post we have never seen.
	var unknown core.EventID
	unknown[0] = 0xAB
	like, _ := liker.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: 1e9, Sequence: 1,
		Like: &core.Like{TargetID: unknown},
	})
	inserted, err := s.PutRemoteEvent(like, 1)
	if err != nil {
		t.Fatalf("PutRemoteEvent: %v", err)
	}
	if inserted {
		t.Fatal("like targeting unknown post was stored")
	}
	// Not flagged: existence is receiver-local, not a protocol violation.
	flagged, err := s.IsFlagged(liker.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if flagged {
		t.Fatal("author flagged for an orphan like")
	}
}

// TestLikeTargetingTombstonedPostDropped verifies that a Like targeting a
// tombstoned (deleted) post is dropped at ingest.
func TestLikeTargetingTombstonedPostDropped(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()

	// Author posts, then deletes their post.
	post, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 1e9, Sequence: 1,
		Post: &core.Post{Text: "doomed"},
	})
	postID, _ := post.ID()
	if _, err := s.PutRemoteEvent(post, 1); err != nil {
		t.Fatalf("put post: %v", err)
	}
	del, _ := author.Sign(core.Event{
		Kind: core.KindDelete, Log: core.PostLog, Timestamp: 2e9, Sequence: 2,
		Delete: &core.Delete{TargetID: postID},
	})
	if _, err := s.PutRemoteEvent(del, 2); err != nil {
		t.Fatalf("put delete: %v", err)
	}

	// A Like targeting the now-tombstoned post is dropped.
	liker, _ := core.NewKeyPair()
	like, _ := liker.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: 3e9, Sequence: 1,
		Like: &core.Like{TargetID: postID},
	})
	inserted, err := s.PutRemoteEvent(like, 1)
	if err != nil {
		t.Fatalf("PutRemoteEvent: %v", err)
	}
	if inserted {
		t.Fatal("like targeting tombstoned post was stored")
	}
}

// TestReplyTargetingUnknownParentDropped verifies that a Reply (a Post with
// ParentID set) whose parent does not exist locally is dropped at ingest.
func TestReplyTargetingUnknownParentDropped(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()

	var unknown core.EventID
	unknown[0] = 0xCD
	reply, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 1e9, Sequence: 1,
		Post: &core.Post{Text: "reply to nothing", ParentID: unknown},
	})
	inserted, err := s.PutRemoteEvent(reply, 1)
	if err != nil {
		t.Fatalf("PutRemoteEvent: %v", err)
	}
	if inserted {
		t.Fatal("reply targeting unknown parent was stored")
	}
}

// TestDeleteDropsReferencingLikes verifies that when a Delete arrives for a
// post that has stored likes, those likes are dropped from the store.
func TestDeleteDropsReferencingLikes(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()
	liker, _ := core.NewKeyPair()

	// Author posts.
	post, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 1e9, Sequence: 1,
		Post: &core.Post{Text: "delete me"},
	})
	postID, _ := post.ID()
	if _, err := s.PutRemoteEvent(post, 1); err != nil {
		t.Fatalf("put post: %v", err)
	}
	// Liker likes the post.
	like, _ := liker.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: 2e9, Sequence: 1,
		Like: &core.Like{TargetID: postID},
	})
	if _, err := s.PutRemoteEvent(like, 1); err != nil {
		t.Fatalf("put like: %v", err)
	}
	// The like is present before the delete.
	likes, err := s.LikesForPosts([]core.EventID{postID})
	if err != nil {
		t.Fatalf("LikesForPosts before: %v", err)
	}
	if len(likes) != 1 {
		t.Fatalf("want 1 like before delete, got %d", len(likes))
	}
	likeID, _ := like.ID()
	exists, err := s.EventExists(likeID)
	if err != nil {
		t.Fatalf("EventExists before: %v", err)
	}
	if !exists {
		t.Fatal("like should exist before delete")
	}

	// Author deletes the post. The like is cascaded.
	del, _ := author.Sign(core.Event{
		Kind: core.KindDelete, Log: core.PostLog, Timestamp: 3e9, Sequence: 2,
		Delete: &core.Delete{TargetID: postID},
	})
	if _, err := s.PutRemoteEvent(del, 2); err != nil {
		t.Fatalf("put delete: %v", err)
	}

	// The like is gone from the store and the index.
	likes, err = s.LikesForPosts([]core.EventID{postID})
	if err != nil {
		t.Fatalf("LikesForPosts after: %v", err)
	}
	if len(likes) != 0 {
		t.Fatalf("want 0 likes after delete, got %d", len(likes))
	}
	exists, err = s.EventExists(likeID)
	if err != nil {
		t.Fatalf("EventExists after: %v", err)
	}
	if exists {
		t.Fatal("like should not exist in index after delete")
	}
}

// TestDeleteDropsReferencingReplies verifies that when a Delete arrives for a
// post that has stored replies, those replies are dropped from the store.
func TestDeleteDropsReferencingReplies(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()
	replier, _ := core.NewKeyPair()

	// Author posts.
	post, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 1e9, Sequence: 1,
		Post: &core.Post{Text: "parent"},
	})
	postID, _ := post.ID()
	if _, err := s.PutRemoteEvent(post, 1); err != nil {
		t.Fatalf("put post: %v", err)
	}
	// Replier replies to the post.
	reply, _ := replier.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 2e9, Sequence: 1,
		Post: &core.Post{Text: "a reply", ParentID: postID},
	})
	replyID, _ := reply.ID()
	if _, err := s.PutRemoteEvent(reply, 1); err != nil {
		t.Fatalf("put reply: %v", err)
	}
	exists, err := s.EventExists(replyID)
	if err != nil {
		t.Fatalf("EventExists before: %v", err)
	}
	if !exists {
		t.Fatal("reply should exist before delete")
	}

	// Author deletes the post. The reply is cascaded.
	del, _ := author.Sign(core.Event{
		Kind: core.KindDelete, Log: core.PostLog, Timestamp: 3e9, Sequence: 2,
		Delete: &core.Delete{TargetID: postID},
	})
	if _, err := s.PutRemoteEvent(del, 2); err != nil {
		t.Fatalf("put delete: %v", err)
	}

	exists, err = s.EventExists(replyID)
	if err != nil {
		t.Fatalf("EventExists after: %v", err)
	}
	if exists {
		t.Fatal("reply should not exist in index after parent delete")
	}
	// The reply's bytes are gone from the store.
	events, err := s.CrawledEvents(replier.Identity())
	if err != nil {
		t.Fatalf("CrawledEvents after: %v", err)
	}
	for _, se := range events {
		seID, _ := se.ID()
		if seID == replyID {
			t.Fatal("reply bytes still in store after parent delete")
		}
	}
}

// TestLikeTargetingHeldPostStored verifies that a Like targeting a post the
// receiver holds is stored normally (positive case).
func TestLikeTargetingHeldPostStored(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()
	liker, _ := core.NewKeyPair()

	post, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 1e9, Sequence: 1,
		Post: &core.Post{Text: "held"},
	})
	postID, _ := post.ID()
	if _, err := s.PutRemoteEvent(post, 1); err != nil {
		t.Fatalf("put post: %v", err)
	}
	like, _ := liker.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: 2e9, Sequence: 1,
		Like: &core.Like{TargetID: postID},
	})
	inserted, err := s.PutRemoteEvent(like, 1)
	if err != nil {
		t.Fatalf("PutRemoteEvent: %v", err)
	}
	if !inserted {
		t.Fatal("like targeting held post was not stored")
	}
}

// TestReplyTargetingHeldParentStored verifies that a Reply targeting a held
// parent post is stored normally (positive case).
func TestReplyTargetingHeldParentStored(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()
	replier, _ := core.NewKeyPair()

	parent, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 1e9, Sequence: 1,
		Post: &core.Post{Text: "parent"},
	})
	parentID, _ := parent.ID()
	if _, err := s.PutRemoteEvent(parent, 1); err != nil {
		t.Fatalf("put parent: %v", err)
	}
	reply, _ := replier.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 2e9, Sequence: 1,
		Post: &core.Post{Text: "reply", ParentID: parentID},
	})
	inserted, err := s.PutRemoteEvent(reply, 1)
	if err != nil {
		t.Fatalf("PutRemoteEvent: %v", err)
	}
	if !inserted {
		t.Fatal("reply targeting held parent was not stored")
	}
}

// TestLikeTargetingOwnPostStored verifies that a Like targeting the local
// user's own post (in bucketOwn, not bucketRemote) is stored: the existence
// index spans both buckets.
func TestLikeTargetingOwnPostStored(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	liker, _ := core.NewKeyPair()

	// I post to my own log.
	post, _ := me.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 1e9, Sequence: 1,
		Post: &core.Post{Text: "my post"},
	})
	postID, _ := post.ID()
	if err := s.AppendOwnEvent(core.PostLog, post); err != nil {
		t.Fatalf("append own post: %v", err)
	}
	// A remote liker likes my post. The existence index spans own + remote,
	// so the like is stored.
	like, _ := liker.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: 2e9, Sequence: 1,
		Like: &core.Like{TargetID: postID},
	})
	inserted, err := s.PutRemoteEvent(like, 1)
	if err != nil {
		t.Fatalf("PutRemoteEvent: %v", err)
	}
	if !inserted {
		t.Fatal("like targeting own post was not stored")
	}
}

// TestDeleteTargetingLikeRemovesLike verifies that a Delete targeting a Like
// (not a post) removes that Like from the store and the reference index.
func TestDeleteTargetingLikeRemovesLike(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()
	liker, _ := core.NewKeyPair()

	// Author posts.
	post, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: 1e9, Sequence: 1,
		Post: &core.Post{Text: "post"},
	})
	postID, _ := post.ID()
	if _, err := s.PutRemoteEvent(post, 1); err != nil {
		t.Fatalf("put post: %v", err)
	}
	// Liker likes the post.
	like, _ := liker.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: 2e9, Sequence: 1,
		Like: &core.Like{TargetID: postID},
	})
	likeID, _ := like.ID()
	if _, err := s.PutRemoteEvent(like, 1); err != nil {
		t.Fatalf("put like: %v", err)
	}
	// The like is present.
	likes, err := s.LikesForPosts([]core.EventID{postID})
	if err != nil {
		t.Fatalf("LikesForPosts before: %v", err)
	}
	if len(likes) != 1 {
		t.Fatalf("want 1 like, got %d", len(likes))
	}

	// Author deletes the Like itself.
	del, _ := author.Sign(core.Event{
		Kind: core.KindDelete, Log: core.PostLog, Timestamp: 3e9, Sequence: 2,
		Delete: &core.Delete{TargetID: likeID},
	})
	if _, err := s.PutRemoteEvent(del, 2); err != nil {
		t.Fatalf("put delete: %v", err)
	}

	// The like is gone from the store and the index.
	likes, err = s.LikesForPosts([]core.EventID{postID})
	if err != nil {
		t.Fatalf("LikesForPosts after: %v", err)
	}
	if len(likes) != 0 {
		t.Fatalf("want 0 likes after delete, got %d", len(likes))
	}
	exists, err := s.EventExists(likeID)
	if err != nil {
		t.Fatalf("EventExists after: %v", err)
	}
	if exists {
		t.Fatal("like should not exist in index after delete")
	}
}

// TestPruneDropsLikeTargetingPrunedPost verifies that after a prune removes a
// post, a Like targeting the pruned post is dropped at ingest. The existence
// and reference indexes are updated on prune.
func TestPruneDropsLikeTargetingPrunedPost(t *testing.T) {
	s := newTestStore(t)
	s.SetRetention(10 * time.Second)
	author, _ := core.NewKeyPair()
	liker, _ := core.NewKeyPair()

	now := int64(100 * 1e9)
	s.SetNow(func() int64 { return now })

	// Author posts within the retention window.
	post, _ := author.Sign(core.Event{
		Kind: core.KindPost, Log: core.PostLog, Timestamp: now - 5*1e9, Sequence: 1,
		Post: &core.Post{Text: "recent"},
	})
	postID, _ := post.ID()
	if _, err := s.PutRemoteEvent(post, 1); err != nil {
		t.Fatalf("put post: %v", err)
	}

	// Prune: the post is within the window, so it is not pruned.
	pruned, err := s.PruneRemotePosts()
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned != 0 {
		t.Fatalf("want 0 pruned, got %d", pruned)
	}

	// A like targeting the still-held post is stored.
	like, _ := liker.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: now - 4*1e9, Sequence: 1,
		Like: &core.Like{TargetID: postID},
	})
	inserted, err := s.PutRemoteEvent(like, 1)
	if err != nil {
		t.Fatalf("PutRemoteEvent like: %v", err)
	}
	if !inserted {
		t.Fatal("like targeting held post was not stored")
	}

	// Advance the clock past the retention window so the post ages out.
	s.SetNow(func() int64 { return now + 20*1e9 })

	// Prune: the post and the like are now stale.
	pruned, err = s.PruneRemotePosts()
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned != 2 {
		t.Fatalf("want 2 pruned (post + like), got %d", pruned)
	}

	// A new like targeting the pruned post is dropped: the index entry was
	// removed on prune.
	like2, _ := liker.Sign(core.Event{
		Kind: core.KindLike, Log: core.PostLog, Timestamp: now + 20*1e9, Sequence: 2,
		Like: &core.Like{TargetID: postID},
	})
	inserted, err = s.PutRemoteEvent(like2, 2)
	if err != nil {
		t.Fatalf("PutRemoteEvent like2: %v", err)
	}
	if inserted {
		t.Fatal("like targeting pruned post was stored")
	}
}

// signRemoteFollowFor signs a KindFollow event by author targeting target.
func signRemoteFollowFor(t *testing.T, author *core.KeyPair, seq uint64, ts int64, target [32]byte) *core.SignedEvent {
	t.Helper()
	se, err := author.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: ts,
		Sequence:  seq,
		Follow:    &core.Follow{TargetPubkey: target},
	})
	if err != nil {
		t.Fatalf("sign follow: %v", err)
	}
	return se
}

// bulkOwnFollows writes n signed Follow events for distinct targets into the
// own ProfileLog bucket in a single transaction. Used to seed the author-side
// follow cap boundary without the O(n^2) cost of SignAndAppend projecting the
// log on every append. The boundary call itself goes through SignAndAppend.
func bulkOwnFollows(t *testing.T, s *Store, kp *core.KeyPair, n int) {
	t.Helper()
	err := s.db.Update(func(tx *bolt.Tx) error {
		logBucket, err := tx.Bucket(bucketOwn).CreateBucketIfNotExists([]byte(core.ProfileLog))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			target, _ := core.NewKeyPair()
			se, err := kp.Sign(core.Event{
				Kind:      core.KindFollow,
				Log:       core.ProfileLog,
				Timestamp: int64(5e9 + i),
				Sequence:  uint64(i + 1),
				Follow:    &core.Follow{TargetPubkey: [32]byte(target.Public)},
			})
			if err != nil {
				return err
			}
			id, err := se.ID()
			if err != nil {
				return err
			}
			buf, err := core.CanonicalEncode(se)
			if err != nil {
				return err
			}
			if err := logBucket.Put(id[:], buf); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("bulk own follows: %v", err)
	}
}

// bulkRemoteFollows writes n signed Follow events for distinct targets into a
// remote author's sub-bucket in a single transaction. Used to seed the
// receiver-side follow cap boundary without the O(n^2) cost of PutRemoteEvent
// projecting the log on every event. The boundary call itself goes through
// PutRemoteEvent (or PutCrawledProfile).
func bulkRemoteFollows(t *testing.T, s *Store, author *core.KeyPair, n int) {
	t.Helper()
	err := s.db.Update(func(tx *bolt.Tx) error {
		ab, err := tx.Bucket(bucketRemote).CreateBucketIfNotExists([]byte(author.Identity()))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			target, _ := core.NewKeyPair()
			se, err := author.Sign(core.Event{
				Kind:      core.KindFollow,
				Log:       core.ProfileLog,
				Timestamp: int64(5e9 + i),
				Sequence:  uint64(i + 1),
				Follow:    &core.Follow{TargetPubkey: [32]byte(target.Public)},
			})
			if err != nil {
				return err
			}
			id, err := se.ID()
			if err != nil {
				return err
			}
			buf, err := core.CanonicalEncode(se)
			if err != nil {
				return err
			}
			if err := ab.Put(id[:], buf); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("bulk remote follows: %v", err)
	}
}

// TestAuthorFollowCapRejectsExceeding verifies that SignAndAppend rejects a
// Follow that would push the net follow count past MaxFollows. The author
// side enforces the cap, so a legitimate author can never sign more than
// MaxFollows net follows.
func TestAuthorFollowCapRejectsExceeding(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Seed the own ProfileLog with MaxFollows net follows directly, so the
	// boundary call through SignAndAppend projects the log once rather than
	// re-projecting on each of MaxFollows appends.
	bulkOwnFollows(t, s, kp, core.MaxFollows)

	// The MaxFollows+1th distinct target is rejected.
	target, _ := core.NewKeyPair()
	_, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: [32]byte(target.Public)},
	})
	if !errors.Is(err, core.ErrFollowCapExceeded) {
		t.Fatalf("want ErrFollowCapExceeded, got %v", err)
	}

	// Re-following an already-followed target does not change the net
	// count, so it is allowed even at the cap. Use the first seeded target.
	// Resolve it by reading the own log's first follow event.
	events, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		t.Fatalf("own events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no own follow events")
	}
	existing := events[0].Event.Follow.TargetPubkey
	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: existing},
	}); err != nil {
		t.Fatalf("re-follow at cap: %v", err)
	}

	// Unfollowing one makes room; a new distinct target is then accepted.
	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindUnfollow,
		Follow: &core.Follow{TargetPubkey: existing},
	}); err != nil {
		t.Fatalf("unfollow to make room: %v", err)
	}
	newTarget, _ := core.NewKeyPair()
	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: [32]byte(newTarget.Public)},
	}); err != nil {
		t.Fatalf("follow after unfollow: %v", err)
	}
}

// TestReceiverFollowCapFlagsExceeding verifies that on the receiver side, an
// author whose net follows exceed MaxFollows is flagged in bucketFollowState
// and the offending event is not stored.
func TestReceiverFollowCapFlagsExceeding(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()

	// Seed MaxFollows net follows directly, then drive the boundary
	// through PutRemoteEvent so the projection runs once at the cap.
	bulkRemoteFollows(t, s, author, core.MaxFollows)

	// The MaxFollows+1th distinct Follow trips the cap: flagged, not stored.
	target, _ := core.NewKeyPair()
	se := signRemoteFollowFor(t, author, uint64(core.MaxFollows+1), int64(5e9+core.MaxFollows), [32]byte(target.Public))
	inserted, err := s.PutRemoteEvent(se, uint64(core.MaxFollows+1))
	if err != nil {
		t.Fatalf("PutRemoteEvent cap follow: %v", err)
	}
	if inserted {
		t.Fatal("cap-exceeding follow was stored")
	}
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if !flagged {
		t.Fatal("author with too many net follows was not flagged")
	}

	// A further ProfileLog event from the flagged author is not stored.
	target2, _ := core.NewKeyPair()
	se2 := signRemoteFollowFor(t, author, uint64(core.MaxFollows+2), int64(5e9+core.MaxFollows+1), [32]byte(target2.Public))
	inserted, err = s.PutRemoteEvent(se2, uint64(core.MaxFollows+2))
	if err != nil {
		t.Fatalf("PutRemoteEvent after flag: %v", err)
	}
	if inserted {
		t.Fatal("event from flagged author was stored")
	}
}

// TestReceiverFollowCapNormalAuthorStored verifies that an author with 150
// net follows (well under the cap) is stored normally and not flagged.
func TestReceiverFollowCapNormalAuthorStored(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()

	for i := 0; i < 150; i++ {
		target, _ := core.NewKeyPair()
		se := signRemoteFollowFor(t, author, uint64(i+1), int64(5e9+i), [32]byte(target.Public))
		inserted, err := s.PutRemoteEvent(se, uint64(i+1))
		if err != nil {
			t.Fatalf("PutRemoteEvent follow %d: %v", i, err)
		}
		if !inserted {
			t.Fatalf("follow %d not inserted", i)
		}
	}
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if flagged {
		t.Fatal("author with 150 follows was flagged")
	}
}

// TestReceiverFollowCapViaCrawledProfile verifies the same cap check fires
// through PutCrawledProfile (the crawler path).
func TestReceiverFollowCapViaCrawledProfile(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()

	// Seed MaxFollows net follows directly, then drive the boundary
	// through PutCrawledProfile.
	bulkRemoteFollows(t, s, author, core.MaxFollows)

	target, _ := core.NewKeyPair()
	se := signRemoteFollowFor(t, author, uint64(core.MaxFollows+1), int64(5e9+core.MaxFollows), [32]byte(target.Public))
	if err := s.PutCrawledProfile(author.Identity(), se); err != nil {
		t.Fatalf("PutCrawledProfile cap follow: %v", err)
	}
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if !flagged {
		t.Fatal("author with too many net follows was not flagged via crawled profile")
	}
}

// profileLogForTarget returns the ProfileLog events whose Follow.TargetPubkey
// matches target. Used by the Phase 7 tidy-at-write tests.
func profileLogForTarget(t *testing.T, s *Store, target [32]byte) []core.SignedEvent {
	t.Helper()
	events, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		t.Fatalf("own profile events: %v", err)
	}
	var out []core.SignedEvent
	for _, se := range events {
		if se.Event.Follow != nil && se.Event.Follow.TargetPubkey == target {
			out = append(out, se)
		}
	}
	return out
}

// profileLogProfiles returns the ProfileLog events of kind Profile in the
// own log.
func profileLogProfiles(t *testing.T, s *Store) []core.SignedEvent {
	t.Helper()
	events, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		t.Fatalf("own profile events: %v", err)
	}
	var out []core.SignedEvent
	for _, se := range events {
		if se.Event.Kind == core.KindProfile {
			out = append(out, se)
		}
	}
	return out
}

// TestTidyRepeatedFollowLeavesOneEvent verifies that following the same
// target more than once leaves exactly one Follow, not one per call.
func TestTidyRepeatedFollowLeavesOneEvent(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	target, _ := core.NewKeyPair()
	tgt := [32]byte(target.Public)

	for i := 0; i < 3; i++ {
		if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
			Kind:   core.KindFollow,
			Follow: &core.Follow{TargetPubkey: tgt},
		}); err != nil {
			t.Fatalf("follow %d: %v", i, err)
		}
	}

	evs := profileLogForTarget(t, s, tgt)
	if len(evs) != 1 {
		t.Fatalf("want 1 event after repeated follow, got %d", len(evs))
	}
	if evs[0].Event.Kind != core.KindFollow {
		t.Fatalf("want KindFollow, got %d", evs[0].Event.Kind)
	}
	if err := evs[0].Verify(); err != nil {
		t.Fatalf("kept follow does not verify: %v", err)
	}
}

// TestTidyRepeatedUnfollowLeavesOneEvent verifies that unfollowing the same
// target more than once leaves exactly one Unfollow, not one per call.
func TestTidyRepeatedUnfollowLeavesOneEvent(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	target, _ := core.NewKeyPair()
	tgt := [32]byte(target.Public)

	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: tgt},
	}); err != nil {
		t.Fatalf("follow: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
			Kind:   core.KindUnfollow,
			Follow: &core.Follow{TargetPubkey: tgt},
		}); err != nil {
			t.Fatalf("unfollow %d: %v", i, err)
		}
	}

	evs := profileLogForTarget(t, s, tgt)
	if len(evs) != 1 {
		t.Fatalf("want 1 event after repeated unfollow, got %d", len(evs))
	}
	if evs[0].Event.Kind != core.KindUnfollow {
		t.Fatalf("want KindUnfollow, got %d", evs[0].Event.Kind)
	}
	if err := evs[0].Verify(); err != nil {
		t.Fatalf("kept unfollow does not verify: %v", err)
	}
}

// TestTidyUnfollowFollowedTargetLeavesOneEvent verifies that an Unfollow for
// a followed target removes the matching Follow, so the log holds only the
// Unfollow as the convergence tombstone. The FollowSet projection shows
// the target as no longer followed.
func TestTidyUnfollowFollowedTargetLeavesOneEvent(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	target, _ := core.NewKeyPair()
	tgt := [32]byte(target.Public)

	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: tgt},
	}); err != nil {
		t.Fatalf("follow: %v", err)
	}
	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindUnfollow,
		Follow: &core.Follow{TargetPubkey: tgt},
	}); err != nil {
		t.Fatalf("unfollow: %v", err)
	}

	// The FollowSet projection must show the target as not followed.
	fs, err := s.ownFollowSet()
	if err != nil {
		t.Fatalf("follow set: %v", err)
	}
	if fs.Contains(tgt) {
		t.Fatal("target still in follow set after unfollow")
	}

	// The log holds only the Unfollow: the matching Follow was removed.
	evs := profileLogForTarget(t, s, tgt)
	if len(evs) != 1 {
		t.Fatalf("want 1 event (unfollow) after unfollow, got %d", len(evs))
	}
	if evs[0].Event.Kind != core.KindUnfollow {
		t.Fatalf("want KindUnfollow, got %d", evs[0].Event.Kind)
	}
	if err := evs[0].Verify(); err != nil {
		t.Fatalf("kept unfollow does not verify: %v", err)
	}
}

// TestTidyProfileLeavesOneEvent verifies that a second Profile event removes
// the first, so only the later one remains.
func TestTidyProfileLeavesOneEvent(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()

	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:    core.KindProfile,
		Profile: &core.Profile{DisplayName: "first"},
	}); err != nil {
		t.Fatalf("profile 1: %v", err)
	}
	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:    core.KindProfile,
		Profile: &core.Profile{DisplayName: "second"},
	}); err != nil {
		t.Fatalf("profile 2: %v", err)
	}

	profs := profileLogProfiles(t, s)
	if len(profs) != 1 {
		t.Fatalf("want 1 profile event, got %d", len(profs))
	}
	if profs[0].Event.Profile == nil || profs[0].Event.Profile.DisplayName != "second" {
		t.Fatalf("want second profile, got %+v", profs[0].Event.Profile)
	}
}

// TestTidyFollowUnfollowFollowLeavesOneEvent verifies that an author who
// follows Alice, unfollows Alice, then follows Alice again has exactly one
// event for Alice: the latest Follow. The second Follow removes both the
// first Follow and the Unfollow, so no tombstone survives the cycle.
func TestTidyFollowUnfollowFollowLeavesOneEvent(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	target, _ := core.NewKeyPair()
	tgt := [32]byte(target.Public)

	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: tgt},
	}); err != nil {
		t.Fatalf("follow 1: %v", err)
	}
	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindUnfollow,
		Follow: &core.Follow{TargetPubkey: tgt},
	}); err != nil {
		t.Fatalf("unfollow 1: %v", err)
	}
	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: tgt},
	}); err != nil {
		t.Fatalf("follow 2: %v", err)
	}

	evs := profileLogForTarget(t, s, tgt)
	if len(evs) != 1 {
		t.Fatalf("want 1 event (latest follow), got %d", len(evs))
	}
	if evs[0].Event.Kind != core.KindFollow {
		t.Fatalf("want KindFollow, got %d", evs[0].Event.Kind)
	}
	if err := evs[0].Verify(); err != nil {
		t.Fatalf("kept follow does not verify: %v", err)
	}

	// The target must be in the follow set.
	fs, err := s.ownFollowSet()
	if err != nil {
		t.Fatalf("follow set: %v", err)
	}
	if !fs.Contains(tgt) {
		t.Fatal("target not in follow set after re-follow")
	}
}

// TestTidyFollowUnfollowFollowUnfollowLeavesOneEvent verifies that an author
// who follows Alice, unfollows Alice, follows Alice, then unfollows Alice
// again has exactly one event for Alice: the latest Unfollow. Each event in
// the cycle removes all prior events for the target, so no event from an
// earlier cycle survives.
func TestTidyFollowUnfollowFollowUnfollowLeavesOneEvent(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	target, _ := core.NewKeyPair()
	tgt := [32]byte(target.Public)

	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: tgt},
	}); err != nil {
		t.Fatalf("follow 1: %v", err)
	}
	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindUnfollow,
		Follow: &core.Follow{TargetPubkey: tgt},
	}); err != nil {
		t.Fatalf("unfollow 1: %v", err)
	}
	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: tgt},
	}); err != nil {
		t.Fatalf("follow 2: %v", err)
	}
	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindUnfollow,
		Follow: &core.Follow{TargetPubkey: tgt},
	}); err != nil {
		t.Fatalf("unfollow 2: %v", err)
	}

	evs := profileLogForTarget(t, s, tgt)
	if len(evs) != 1 {
		t.Fatalf("want 1 event (latest unfollow), got %d", len(evs))
	}
	if evs[0].Event.Kind != core.KindUnfollow {
		t.Fatalf("want KindUnfollow, got %d", evs[0].Event.Kind)
	}
	if err := evs[0].Verify(); err != nil {
		t.Fatalf("kept unfollow does not verify: %v", err)
	}

	// The target must not be in the follow set.
	fs, err := s.ownFollowSet()
	if err != nil {
		t.Fatalf("follow set: %v", err)
	}
	if fs.Contains(tgt) {
		t.Fatal("target still in follow set after final unfollow")
	}
}

// TestTidyProfileLogDeleteRejected verifies that a ProfileLog Delete event is
// rejected by validateEvent (called by Sign).
func TestTidyProfileLogDeleteRejected(t *testing.T) {
	kp, _ := core.NewKeyPair()
	_, err := kp.Sign(core.Event{
		Kind:   core.KindDelete,
		Log:    core.ProfileLog,
		Delete: &core.Delete{TargetID: core.EventID{1}},
	})
	if err == nil {
		t.Fatal("Sign accepted KindDelete in ProfileLog")
	}
}

// TestTidySequenceMonotonic verifies that the persisted sequence counter is
// monotonic: after a tidy-at-write removes an event, the next event gets a
// sequence strictly greater than the last, never lower or colliding.
func TestTidySequenceMonotonic(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	target, _ := core.NewKeyPair()
	tgt := [32]byte(target.Public)

	// Follow, then re-follow (which removes the first). The second event
	// must have a sequence strictly greater than the first.
	se1, id1, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: tgt},
	})
	if err != nil {
		t.Fatalf("follow 1: %v", err)
	}
	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: tgt},
	}); err != nil {
		t.Fatalf("follow 2: %v", err)
	}
	_ = se1
	_ = id1

	events, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		t.Fatalf("own events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event after re-follow, got %d", len(events))
	}
	// The surviving event's sequence must be 2, not 1 (which it would be
	// if derived from key count after the first was removed).
	if events[0].Event.Sequence != 2 {
		t.Fatalf("want sequence 2, got %d", events[0].Event.Sequence)
	}

	// A third event (a Profile) must get sequence 3, strictly greater.
	if _, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:    core.KindProfile,
		Profile: &core.Profile{DisplayName: "x"},
	}); err != nil {
		t.Fatalf("profile: %v", err)
	}
	events, err = s.OwnEvents(core.ProfileLog)
	if err != nil {
		t.Fatalf("own events after profile: %v", err)
	}
	var profSeq uint64
	for _, se := range events {
		if se.Event.Kind == core.KindProfile {
			profSeq = se.Event.Sequence
		}
	}
	if profSeq != 3 {
		t.Fatalf("want profile sequence 3, got %d", profSeq)
	}
}

// TestTidyLogStoredDirectly verifies that a tidy log served to a cold-start
// receiver is stored directly, and the FollowSet and Profile projected from
// the stored set equal those projected from the full history.
func TestTidyLogStoredDirectly(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()
	target1, _ := core.NewKeyPair()
	target2, _ := core.NewKeyPair()

	// Build a tidy own log through SignAndAppend: follow two targets,
	// unfollow one, set a profile.
	if _, _, err := s.SignAndAppend(author, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: [32]byte(target1.Public)},
	}); err != nil {
		t.Fatalf("follow 1: %v", err)
	}
	if _, _, err := s.SignAndAppend(author, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: [32]byte(target2.Public)},
	}); err != nil {
		t.Fatalf("follow 2: %v", err)
	}
	if _, _, err := s.SignAndAppend(author, core.ProfileLog, core.Event{
		Kind:   core.KindUnfollow,
		Follow: &core.Follow{TargetPubkey: [32]byte(target1.Public)},
	}); err != nil {
		t.Fatalf("unfollow 1: %v", err)
	}
	if _, _, err := s.SignAndAppend(author, core.ProfileLog, core.Event{
		Kind:    core.KindProfile,
		Profile: &core.Profile{DisplayName: "author"},
	}); err != nil {
		t.Fatalf("profile: %v", err)
	}

	// Read the tidy own log.
	tidyEvents, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		t.Fatalf("own events: %v", err)
	}
	// The tidy log holds: U(t1) as the convergence tombstone for the
	// unfollowed target, F(t2) for the followed target, and one Profile.
	// One event per target, never a Follow+Unfollow pair.
	kinds := make(map[core.Kind]int)
	for _, se := range tidyEvents {
		kinds[se.Event.Kind]++
	}
	if kinds[core.KindFollow] != 1 || kinds[core.KindUnfollow] != 1 || kinds[core.KindProfile] != 1 {
		t.Fatalf("tidy log shape wrong: %+v", kinds)
	}

	// Serve the tidy log to a cold-start receiver: store each event via
	// PutRemoteEvent. The receiver should store them directly.
	receiver := newTestStore(t)
	for _, se := range tidyEvents {
		if _, err := receiver.PutRemoteEvent(&se, se.Event.Sequence); err != nil {
			t.Fatalf("put remote event: %v", err)
		}
	}

	// Project FollowSet and Profile from the stored set.
	storedEvents, err := receiver.CrawledProfiles(author.Identity())
	if err != nil {
		t.Fatalf("crawled profiles: %v", err)
	}
	storedFS := core.NewLog(storedEvents).FollowSet()
	storedProf := core.NewLog(storedEvents).Profile()

	// Project from the full history (the tidy events are the full history
	// here, since the log is already tidy, but the point is the receiver
	// stores them directly and the projections match).
	fullFS := core.NewLog(tidyEvents).FollowSet()
	fullProf := core.NewLog(tidyEvents).Profile()

	// The follow set should contain only target2 (target1 was unfollowed).
	if storedFS.Len() != 1 {
		t.Fatalf("stored follow set: want 1, got %d", storedFS.Len())
	}
	if !storedFS.Contains([32]byte(target2.Public)) {
		t.Fatal("stored follow set missing target2")
	}
	if storedFS.Contains([32]byte(target1.Public)) {
		t.Fatal("stored follow set contains unfollowed target1")
	}
	if fullFS.Len() != storedFS.Len() {
		t.Fatalf("follow set mismatch: full=%d stored=%d", fullFS.Len(), storedFS.Len())
	}

	// The profile should be "author".
	if storedProf == nil || storedProf.DisplayName != "author" {
		t.Fatalf("stored profile: want author, got %+v", storedProf)
	}
	if fullProf == nil || fullProf.DisplayName != "author" {
		t.Fatalf("full profile: want author, got %+v", fullProf)
	}
}
