package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"driftnode/internal/core"
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

	conn, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)
	if err := enc.Encode(Request{Method: "whoami"}); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var resp Response
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("error: %s", resp.Error)
	}
	result := resp.Result.(map[string]any)
	id := result["identity"].(string)
	if _, err := core.ParseIdentity(id); err != nil {
		t.Fatalf("invalid identity: %v", err)
	}
}

func TestDaemonStatus(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	conn, _ := Dial(sock)
	defer conn.Close()
	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)
	enc.Encode(Request{Method: "status"})
	var resp Response
	dec.Decode(&resp)
	result := resp.Result.(map[string]any)
	if result["running"] != true {
		t.Fatal("not running")
	}
}

func TestDaemonZens(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	d.AddZen("zen1", "native", "connected")
	d.AddZen("zen2", "browser", "connected")

	conn, _ := Dial(sock)
	defer conn.Close()
	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)
	enc.Encode(Request{Method: "zens"})
	var resp Response
	dec.Decode(&resp)
	zens := resp.Result.([]any)
	if len(zens) != 2 {
		t.Fatalf("want 2 zens, got %d", len(zens))
	}
}

func TestDialNotRunning(t *testing.T) {
	sock := testSocketPath(t)
	_, err := Dial(sock)
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

// Compile-time check: ensure we use net and json.
var _ = json.Marshal
var _ net.Conn
