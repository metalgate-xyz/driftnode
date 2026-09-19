package cli

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"driftnode/internal/core"
	"driftnode/internal/store"
)

// runCLI runs the given args against a fresh temp store, returning captured
// stdout. The --db flag is injected to point at a temp path.
func runCLI(t *testing.T, dbPath string, args []string) (string, error) {
	t.Helper()
	full := append([]string{"--db", dbPath}, args...)
	var buf bytes.Buffer
	root := Root()
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetArgs(full)
	err := root.Execute()
	return buf.String(), err
}

// runCLIStoreless runs commands that don't take --db (bootstrap keygen/
// sign/verify) without injecting it.
func runCLIStoreless(t *testing.T, args []string) (string, error) {
	t.Helper()
	var buf bytes.Buffer
	root := Root()
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetArgs(args)
	err := root.Execute()
	return buf.String(), err
}

func TestInitAndWhoami(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	out, err := runCLI(t, dbPath, []string{"init", "--passphrase", "testpass"})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	idStr := strings.TrimSpace(out)
	if _, err := core.ParseIdentity(idStr); err != nil {
		t.Fatalf("init returned invalid identity %q: %v", idStr, err)
	}

	out, err = runCLI(t, dbPath, []string{"whoami"})
	if err != nil {
		t.Fatalf("whoami: %v", err)
	}
	if strings.TrimSpace(out) != idStr {
		t.Fatalf("whoami: want %q, got %q", idStr, strings.TrimSpace(out))
	}
}

func TestPostAndFeed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	_, err := runCLI(t, dbPath, []string{"init", "--passphrase", "testpass"})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	_, err = runCLI(t, dbPath, []string{"post", "hello world", "--passphrase", "testpass"})
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	out, err := runCLI(t, dbPath, []string{"feed"})
	if err != nil {
		t.Fatalf("feed: %v", err)
	}
	if !strings.Contains(out, "hello world") {
		t.Fatalf("feed output missing post: %q", out)
	}
}

func TestFeedShowsZenName(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	if _, err := runCLI(t, dbPath, []string{"init", "--passphrase", "testpass"}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := runCLI(t, dbPath, []string{"profile", "--name", "alice", "--passphrase", "testpass"}); err != nil {
		t.Fatalf("profile: %v", err)
	}
	if _, err := runCLI(t, dbPath, []string{"post", "gm", "--passphrase", "testpass"}); err != nil {
		t.Fatalf("post: %v", err)
	}
	out, err := runCLI(t, dbPath, []string{"feed"})
	if err != nil {
		t.Fatalf("feed: %v", err)
	}
	if !strings.Contains(out, "alice> gm") {
		t.Fatalf("feed should show zen name as author: %q", out)
	}
}

func TestFeedLimit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	_, err := runCLI(t, dbPath, []string{"init", "--passphrase", "testpass"})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	for _, text := range []string{"one", "two", "three"} {
		_, err = runCLI(t, dbPath, []string{"post", text, "--passphrase", "testpass"})
		if err != nil {
			t.Fatalf("post %q: %v", text, err)
		}
	}
	out, err := runCLI(t, dbPath, []string{"feed", "--limit", "2"})
	if err != nil {
		t.Fatalf("feed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d: %q", len(lines), out)
	}
}

func TestFollowUnfollow(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	_, err := runCLI(t, dbPath, []string{"init", "--passphrase", "testpass"})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	targetKP, _ := core.NewKeyPair()
	targetID := string(targetKP.Identity())

	out, err := runCLI(t, dbPath, []string{"follow", targetID, "--passphrase", "testpass"})
	if err != nil {
		t.Fatalf("follow: %v", err)
	}
	if !strings.Contains(out, "followed") {
		t.Fatalf("follow output: %q", out)
	}

	out, err = runCLI(t, dbPath, []string{"unfollow", targetID, "--passphrase", "testpass"})
	if err != nil {
		t.Fatalf("unfollow: %v", err)
	}
	if !strings.Contains(out, "unfollowed") {
		t.Fatalf("unfollow output: %q", out)
	}

	// Verify the follow set is empty after unfollow by reading the store.
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	events, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		t.Fatalf("read profile log: %v", err)
	}
	log := core.NewLog(events)
	if log.FollowSet().Len() != 0 {
		t.Fatalf("follow set not empty after unfollow: %d", log.FollowSet().Len())
	}
}

func TestFollowsAndFollowersOffline(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	_, err := runCLI(t, dbPath, []string{"init", "--passphrase", "testpass"})
	if err != nil {
		t.Fatalf("init: %v", err)
	}

	targetKP, _ := core.NewKeyPair()
	targetID := string(targetKP.Identity())

	if _, err := runCLI(t, dbPath, []string{"follow", targetID, "--passphrase", "testpass"}); err != nil {
		t.Fatalf("follow: %v", err)
	}

	// follows lists the identity we just followed.
	out, err := runCLI(t, dbPath, []string{"follows"})
	if err != nil {
		t.Fatalf("follows: %v", err)
	}
	if !strings.Contains(out, targetID) {
		t.Fatalf("follows output: want %q, got %q", targetID, out)
	}

	// followers is empty: nobody has synced a Follow targeting us yet.
	out, err = runCLI(t, dbPath, []string{"followers"})
	if err != nil {
		t.Fatalf("followers: %v", err)
	}
	if strings.TrimSpace(out) != "" {
		t.Fatalf("followers output: want empty, got %q", out)
	}

	// Simulate a follower: store a Follow event targeting us via the sync
	// receive path, then confirm followers lists it.
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ownID, _, err := s.Identity()
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	followerKP, _ := core.NewKeyPair()
	ownPub, _ := ownID.PubkeyBytes()
	followEvent, err := followerKP.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: 100,
		Sequence:  1,
		Follow:    &core.Follow{TargetPubkey: [32]byte(ownPub)},
	})
	if err != nil {
		t.Fatalf("sign follow: %v", err)
	}
	if _, err := s.PutCrawledEvent(followEvent, 1); err != nil {
		t.Fatalf("put followed event: %v", err)
	}
	s.Close()

	out, err = runCLI(t, dbPath, []string{"followers"})
	if err != nil {
		t.Fatalf("followers after sync: %v", err)
	}
	followerID := string(followerKP.Identity())
	if !strings.Contains(out, followerID) {
		t.Fatalf("followers output: want %q, got %q", followerID, out)
	}
}

func TestBackupExportImport(t *testing.T) {
	home := t.TempDir()
	dbPath := filepath.Join(home, "node.db")

	_, err := runCLI(t, dbPath, []string{"init", "--passphrase", "testpass"})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	_, err = runCLI(t, dbPath, []string{"post", "backup me", "--passphrase", "testpass"})
	if err != nil {
		t.Fatalf("post: %v", err)
	}

	backupPath := filepath.Join(home, "backup.cbor")
	_, err = runCLI(t, dbPath, []string{"backup", "export", "--out", backupPath})
	if err != nil {
		t.Fatalf("backup export: %v", err)
	}
	if _, err := os.Stat(backupPath); err != nil {
		t.Fatalf("backup file not created: %v", err)
	}

	b, _ := os.ReadFile(backupPath)
	var bd core.BackupData
	if err := core.CanonicalDecode(b, &bd); err != nil {
		t.Fatalf("decode backup: %v", err)
	}
	if bd.Key == nil {
		t.Fatal("backup missing key")
	}
	found := false
	for _, se := range bd.OwnLogs {
		if se.Event.Post != nil && se.Event.Post.Text == "backup me" {
			found = true
		}
	}
	if !found {
		t.Fatal("backup missing the post")
	}

	// Import into a fresh store and verify the post survives.
	dbPath2 := filepath.Join(t.TempDir(), "node.db")
	_, err = runCLI(t, dbPath2, []string{"backup", "import", backupPath})
	if err != nil {
		t.Fatalf("backup import: %v", err)
	}
	s, err := store.Open(dbPath2)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	events, err := s.OwnEvents(core.PostLog)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	found = false
	for _, se := range events {
		if se.Event.Post != nil && se.Event.Post.Text == "backup me" {
			found = true
		}
	}
	if !found {
		t.Fatal("imported store missing the post")
	}
}

func TestWrongPassphrase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	_, err := runCLI(t, dbPath, []string{"init", "--passphrase", "correct"})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	_, err = runCLI(t, dbPath, []string{"post", "should fail", "--passphrase", "wrong"})
	if err == nil {
		t.Fatal("post with wrong passphrase should fail")
	}
}

func TestBootstrapKeygenSignVerify(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "bs.key")
	bootstrapPath := filepath.Join(dir, "bootstrap.yaml")

	// keygen prints the base64 public key to stdout and writes the raw
	// 64-byte private key to --key-out.
	pubB64, err := runCLIStoreless(t, []string{"bootstrap", "keygen", "--key-out", keyPath})
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	pubB64 = strings.TrimSpace(pubB64)
	if pubB64 == "" {
		t.Fatal("keygen printed empty public key")
	}
	pub, err := base64.StdEncoding.DecodeString(pubB64)
	if err != nil {
		t.Fatalf("decode public key: %v", err)
	}
	if len(pub) != 32 {
		t.Fatalf("public key: want 32 bytes, got %d", len(pub))
	}
	keyData, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read key file: %v", err)
	}
	if len(keyData) != 64 {
		t.Fatalf("private key file: want 64 bytes, got %d", len(keyData))
	}
	if fi, _ := os.Stat(keyPath); fi.Mode().Perm() != 0o600 {
		t.Fatalf("private key mode: want 0600, got %#o", fi.Mode().Perm())
	}

	// Write an unsigned bootstrap.yaml, then sign it with the keygen output.
	if err := os.WriteFile(bootstrapPath, []byte(
		"version: 1\nsignature: \"\"\nseed_zens:\n  - token: tctest\n    kind: native_zen\ncrawl_seeds:\n  - \"driftnode:abc\"\n"), 0o600); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	if _, err := runCLIStoreless(t, []string{"bootstrap", "sign", bootstrapPath, "--key", keyPath}); err != nil {
		t.Fatalf("sign: %v", err)
	}

	// verify --key reads the public key from a file. Write the base64
	// public key that keygen printed to a file and verify against it.
	pubKeyFile := filepath.Join(dir, "bs.pub")
	if err := os.WriteFile(pubKeyFile, []byte(pubB64), 0o600); err != nil {
		t.Fatalf("write pub key file: %v", err)
	}
	out, err := runCLIStoreless(t, []string{"bootstrap", "verify", bootstrapPath, "--key", pubKeyFile})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !strings.Contains(out, "signature: verified") {
		t.Fatalf("verify output: %q", out)
	}

	// A different key must reject it.
	otherPubB64, _ := runCLIStoreless(t, []string{"bootstrap", "keygen", "--key-out", filepath.Join(dir, "other.key")})
	otherPubB64 = strings.TrimSpace(otherPubB64)
	otherPubFile := filepath.Join(dir, "other.pub")
	if err := os.WriteFile(otherPubFile, []byte(otherPubB64), 0o600); err != nil {
		t.Fatalf("write other pub key: %v", err)
	}
	out, err = runCLIStoreless(t, []string{"bootstrap", "verify", bootstrapPath, "--key", otherPubFile})
	if err == nil {
		t.Fatalf("verify with wrong key should fail, output: %q", out)
	}

	// keygen --key derives the same public key from the saved private key
	// without generating or writing a new one.
	derivedPubB64, err := runCLIStoreless(t, []string{"bootstrap", "keygen", "--key", keyPath})
	if err != nil {
		t.Fatalf("keygen --key: %v", err)
	}
	derivedPubB64 = strings.TrimSpace(derivedPubB64)
	if derivedPubB64 != pubB64 {
		t.Fatalf("keygen --key derived %q, want %q", derivedPubB64, pubB64)
	}
}

func TestProfileCommand(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	if _, err := runCLI(t, dbPath, []string{"init", "--passphrase", "testpass"}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := runCLI(t, dbPath, []string{"profile", "--name", "alice", "--passphrase", "testpass"}); err != nil {
		t.Fatalf("profile: %v", err)
	}
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	events, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		t.Fatalf("read profile log: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 profile event, got %d", len(events))
	}
	if events[0].Event.Profile == nil || events[0].Event.Profile.DisplayName != "alice" {
		t.Fatalf("profile event missing zen name: %+v", events[0].Event.Profile)
	}
}

func TestDetailCommand(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	if _, err := runCLI(t, dbPath, []string{"init", "--passphrase", "testpass"}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := runCLI(t, dbPath, []string{"detail", "--bio", "field tester", "--first-name", "Alice", "--last-name", "Liddell", "--location", "Oxford", "--passphrase", "testpass"}); err != nil {
		t.Fatalf("detail: %v", err)
	}
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	events, err := s.OwnEvents(core.DetailLog)
	if err != nil {
		t.Fatalf("read detail log: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 detail event, got %d", len(events))
	}
	d := events[0].Event.Detail
	if d == nil {
		t.Fatal("detail event missing Detail payload")
	}
	if d.Bio != "field tester" || d.FirstName != "Alice" || d.LastName != "Liddell" || d.Location != "Oxford" {
		t.Fatalf("detail fields mismatch: %+v", d)
	}
}

func TestWhoamiShowsProfileAndDetail(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	if _, err := runCLI(t, dbPath, []string{"init", "--passphrase", "testpass"}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := runCLI(t, dbPath, []string{"profile", "--name", "alice", "--passphrase", "testpass"}); err != nil {
		t.Fatalf("profile: %v", err)
	}
	if _, err := runCLI(t, dbPath, []string{"detail", "--bio", "wanderer", "--location", "Wonderland", "--passphrase", "testpass"}); err != nil {
		t.Fatalf("detail: %v", err)
	}
	out, err := runCLI(t, dbPath, []string{"whoami"})
	if err != nil {
		t.Fatalf("whoami: %v", err)
	}
	if !strings.Contains(out, "zen name: alice") {
		t.Fatalf("whoami missing zen name: %q", out)
	}
	if !strings.Contains(out, "bio: wanderer") {
		t.Fatalf("whoami missing bio: %q", out)
	}
	if !strings.Contains(out, "location: Wonderland") {
		t.Fatalf("whoami missing location: %q", out)
	}
}

func TestZensVerifyOffline(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	if _, err := runCLI(t, dbPath, []string{"init", "--passphrase", "testpass"}); err != nil {
		t.Fatalf("init: %v", err)
	}
	peer, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	peerID := string(peer.Identity())

	out, err := runCLI(t, dbPath, []string{"zens", "verify", peerID})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !strings.Contains(out, "verified") {
		t.Fatalf("verify output: %q", out)
	}
	// Confirm it landed in the store, then close before the next CLI call
	// (bbolt allows only one writer at a time).
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	verified, _ := s.IsVerified(peer.Identity())
	s.Close()
	if !verified {
		t.Fatal("peer not verified in store")
	}
	// Unverify.
	if _, err := runCLI(t, dbPath, []string{"zens", "unverify", peerID}); err != nil {
		t.Fatalf("unverify: %v", err)
	}
	s, err = store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	if v, _ := s.IsVerified(peer.Identity()); v {
		t.Fatal("peer should be unverified after unverify")
	}
}
