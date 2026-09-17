package cli

import (
	"bytes"
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
