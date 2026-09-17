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
