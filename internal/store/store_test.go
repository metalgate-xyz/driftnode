package store

import (
	"crypto/ed25519"
	"path/filepath"
	"testing"

	"driftnode/internal/core"
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
	if _, err := s.PutCrawledEvent(alicePost, 1); err != nil {
		t.Fatalf("put alice post: %v", err)
	}
	if _, err := s.PutCrawledEvent(bobPost, 1); err != nil {
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
		if _, err := s.PutCrawledEvent(se, 1); err != nil {
			t.Fatalf("PutCrawledEvent: %v", err)
		}
	}
	daveFollow := makeFollowEventFor(t, dave, 1, bob.Identity())
	_, _ = s.PutCrawledEvent(daveFollow, 1)

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
	if _, err := s.PutCrawledEvent(se, 1); err != nil {
		t.Fatalf("PutCrawledEvent: %v", err)
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
