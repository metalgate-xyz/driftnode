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
