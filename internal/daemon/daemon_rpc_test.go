package daemon

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestSendRequestWhoami(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	resp, err := SendRequest(sock, "whoami", nil)
	if err != nil {
		t.Fatalf("SendRequest: %v", err)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result is not a map: %T", resp.Result)
	}
	if _, ok := result["identity"].(string); !ok {
		t.Fatal("missing identity in result")
	}
}

func TestSendRequestPeers(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	d.AddPeer("p1", "native", "connected")
	d.AddPeer("p2", "browser", "connected")

	resp, err := SendRequest(sock, "peers", nil)
	if err != nil {
		t.Fatalf("SendRequest: %v", err)
	}
	peers, ok := resp.Result.([]any)
	if !ok {
		t.Fatalf("result is not a slice: %T", resp.Result)
	}
	if len(peers) != 2 {
		t.Fatalf("want 2 peers, got %d", len(peers))
	}
}

func TestSendRequestStatus(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	resp, err := SendRequest(sock, "status", nil)
	if err != nil {
		t.Fatalf("SendRequest: %v", err)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result is not a map: %T", resp.Result)
	}
	if result["running"] != true {
		t.Fatal("not running")
	}
}

func TestSendRequestSync(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	resp, err := SendRequest(sock, "sync", map[string]any{"now": true})
	if err != nil {
		t.Fatalf("SendRequest: %v", err)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result is not a map: %T", resp.Result)
	}
	if result["status"] != "triggered" {
		t.Fatalf("unexpected status: %v", result["status"])
	}
}

func TestSendRequestNotRunning(t *testing.T) {
	sock := testSocketPath(t)
	_, err := SendRequest(sock, "whoami", nil)
	if err == nil {
		t.Fatal("expected error when daemon not running")
	}
}

func TestSendRequestParamsMarshal(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	resp, err := SendRequest(sock, "peers_add", map[string]any{"token": "tcexample"})
	if err != nil {
		t.Fatalf("SendRequest: %v", err)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result is not a map: %T", resp.Result)
	}
	if result["status"] != "connecting" {
		t.Fatalf("unexpected status: %v", result["status"])
	}
}

// Compile-time check.
var _ = json.Marshal

func TestSendRequestStop(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send the stop RPC. The daemon acknowledges, then shuts itself down.
	resp, err := SendRequest(sock, "stop", nil)
	if err != nil {
		t.Fatalf("SendRequest stop: %v", err)
	}
	if resp.Result.(map[string]any)["status"] != "stopping" {
		t.Fatalf("unexpected stop status: %v", resp.Result)
	}

	// Wait for the daemon to stop itself.
	select {
	case <-d.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("daemon did not stop after stop RPC")
	}

	// The socket file must be gone.
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Fatalf("socket file still exists: %v", err)
	}
}

// statusUnlocked reads the "unlocked" field from a status response.
func statusUnlocked(t *testing.T, sock string) bool {
	t.Helper()
	resp, err := SendRequest(sock, "status", nil)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	m, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("status result not a map: %T", resp.Result)
	}
	return m["unlocked"].(bool)
}

func TestUnlockPostWithoutPassphrase(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	// Before unlock, status reports locked and keyless post fails.
	if statusUnlocked(t, sock) {
		t.Fatal("key should be locked before unlock")
	}
	if _, err := SendRequest(sock, "post", map[string]any{"text": "no"}); err == nil {
		t.Fatal("keyless post should fail before unlock")
	}

	// Wrong passphrase is rejected and leaves the key locked.
	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "wrong"}); err == nil {
		t.Fatal("wrong passphrase should fail")
	}
	if statusUnlocked(t, sock) {
		t.Fatal("key should stay locked after wrong passphrase")
	}

	// Correct passphrase unlocks and lets post omit it.
	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "pass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if !statusUnlocked(t, sock) {
		t.Fatal("key should be unlocked after unlock RPC")
	}
	resp, err := SendRequest(sock, "post", map[string]any{"text": "hello"})
	if err != nil {
		t.Fatalf("keyless post: %v", err)
	}
	if _, ok := resp.Result.(map[string]any)["event_id"].(string); !ok {
		t.Fatalf("missing event_id: %v", resp.Result)
	}

	// Lock clears the key; keyless post fails again.
	if _, err := SendRequest(sock, "lock", nil); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if statusUnlocked(t, sock) {
		t.Fatal("key should be locked after lock RPC")
	}
	if _, err := SendRequest(sock, "post", map[string]any{"text": "no"}); err == nil {
		t.Fatal("keyless post should fail after lock")
	}
}

func TestPostWithPassphraseUnlocksImplicitly(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	// Supplying the passphrase on post unlocks implicitly, so the next
	// keyless post succeeds.
	resp, err := SendRequest(sock, "post", map[string]any{"text": "one", "passphrase": "pass"})
	if err != nil {
		t.Fatalf("post with passphrase: %v", err)
	}
	if _, ok := resp.Result.(map[string]any)["event_id"].(string); !ok {
		t.Fatalf("missing event_id: %v", resp.Result)
	}
	if !statusUnlocked(t, sock) {
		t.Fatal("post with passphrase should unlock implicitly")
	}
	if _, err := SendRequest(sock, "post", map[string]any{"text": "two"}); err != nil {
		t.Fatalf("keyless post after implicit unlock: %v", err)
	}
}

func TestIdleLockReapsKey(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	d.SetIdleLock(20 * time.Millisecond) // very short for the test
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "pass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if !statusUnlocked(t, sock) {
		t.Fatal("key should be unlocked")
	}
	time.Sleep(40 * time.Millisecond) // exceed the idle window

	// A keyless post after the idle window reaps the key and fails.
	if _, err := SendRequest(sock, "post", map[string]any{"text": "no"}); err == nil {
		t.Fatal("keyless post should fail after idle-lock reaped the key")
	}
	if statusUnlocked(t, sock) {
		t.Fatal("key should be reaped after idle-lock expiry")
	}
}
