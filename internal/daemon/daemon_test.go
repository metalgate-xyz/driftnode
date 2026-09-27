package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"driftnode/internal/core"
	"driftnode/internal/proto/driftnodepb"
	"driftnode/internal/store"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// testSocketPath returns a short Unix socket path suitable for macOS's
// 104-byte sockaddr_un limit. t.TempDir() can exceed that when nested under
// /var/folders/.../T/TestName<digits>/<digits>/ during parallel runs.
func testSocketPath(t *testing.T) string {
	t.Helper()
	h := sha256.Sum256([]byte(t.Name()))
	sock := fmt.Sprintf("%s/driftnode-test-socks/ctrl-%s", os.TempDir(), hex.EncodeToString(h[:8]))
	if err := os.MkdirAll(filepath.Dir(sock), 0o700); err != nil {
		t.Fatalf("create socket dir: %v", err)
	}
	t.Cleanup(func() { os.Remove(sock) })
	return sock
}

// testClient dials the daemon over gRPC and returns the typed client plus the
// connection. The connection is closed on test cleanup.
func testClient(t *testing.T, sock string) (driftnodepb.DriftnodeClient, func()) {
	t.Helper()
	cl, conn, err := DialClient(sock)
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return cl, func() {}
}

func initTestIdentity(t *testing.T, s *store.Store) *core.KeyPair {
	t.Helper()
	kp, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	enc := core.DefaultKeyEncryption()
	ek, err := enc.Encrypt(kp.Private, []byte("pass"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("InitIdentity: %v", err)
	}
	return kp
}

func TestDaemonWhoami(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	cl, _ := testClient(t, sock)
	resp, err := cl.Whoami(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("Whoami: %v", err)
	}
	if _, err := core.ParseIdentity(resp.Identity); err != nil {
		t.Fatalf("invalid identity: %v", err)
	}
}

func TestDaemonStatus(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	cl, _ := testClient(t, sock)
	resp, err := cl.Status(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if !resp.Running {
		t.Fatal("not running")
	}
}

func TestDaemonZens(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	d.upsertZen("zen1", "native", "connected")
	d.upsertZen("zen2", "browser", "connected")

	cl, _ := testClient(t, sock)
	resp, err := cl.Zens(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("Zens: %v", err)
	}
	if len(resp.Zens) != 2 {
		t.Fatalf("want 2 zens, got %d", len(resp.Zens))
	}
}

func TestDialNotRunning(t *testing.T) {
	sock := testSocketPath(t)
	_, _, err := DialClient(sock)
	if err == nil {
		t.Fatal("expected error when daemon not running")
	}
}

func TestSocketPathCleanup(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	d.Stop()
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Fatal("socket file not cleaned up")
	}
}
