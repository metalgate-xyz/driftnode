package daemon

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"driftnode/internal/core"
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

func TestSendRequestZens(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	d.AddZen("p1", "native", "connected")
	d.AddZen("p2", "browser", "connected")

	resp, err := SendRequest(sock, "zens", nil)
	if err != nil {
		t.Fatalf("SendRequest: %v", err)
	}
	zens, ok := resp.Result.([]any)
	if !ok {
		t.Fatalf("result is not a slice: %T", resp.Result)
	}
	if len(zens) != 2 {
		t.Fatalf("want 2 zens, got %d", len(zens))
	}
}

func TestSendRequestVerifyZens(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	// Add a zen and bind its token to a known identity via the routing table.
	peerKP, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	peerID := peerKP.Identity()
	d.AddZen("p1", "native", "connected")
	if err := s.PutRouting(peerID, "p1"); err != nil {
		t.Fatalf("PutRouting: %v", err)
	}

	// Before verify, the zen is not marked verified.
	resp, err := SendRequest(sock, "zens", nil)
	if err != nil {
		t.Fatalf("zens: %v", err)
	}
	zens, ok := resp.Result.([]any)
	if !ok || len(zens) != 1 {
		t.Fatalf("want 1 zen, got %v", resp.Result)
	}
	if pm, _ := zens[0].(map[string]any); pm["verified"] == true {
		t.Fatal("zen should not be verified before verify RPC")
	}

	// Verify the peer identity out-of-band.
	if _, err := SendRequest(sock, "verify", map[string]any{"identity": string(peerID)}); err != nil {
		t.Fatalf("verify: %v", err)
	}

	// After verify, the zen is marked verified.
	resp, err = SendRequest(sock, "zens", nil)
	if err != nil {
		t.Fatalf("zens after verify: %v", err)
	}
	zens, _ = resp.Result.([]any)
	pm, ok := zens[0].(map[string]any)
	if !ok {
		t.Fatalf("zen is not a map: %T", zens[0])
	}
	if pm["verified"] != true {
		t.Fatalf("zen should be verified after verify RPC, got %v", pm["verified"])
	}

	// Unverify clears the flag.
	if _, err := SendRequest(sock, "unverify", map[string]any{"identity": string(peerID)}); err != nil {
		t.Fatalf("unverify: %v", err)
	}
	resp, _ = SendRequest(sock, "zens", nil)
	zens, _ = resp.Result.([]any)
	pm, _ = zens[0].(map[string]any)
	if pm["verified"] == true {
		t.Fatal("zen should not be verified after unverify")
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
	kp := initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	// follow <pubkey> (offline path): requires a passphrase to sign the
	// Follow event. Passing the node's own identity as target proves the
	// params marshal and dispatch.
	resp, err := SendRequest(sock, "follow", map[string]any{
		"target":     string(kp.Identity()),
		"passphrase": "pass",
	})
	if err != nil {
		t.Fatalf("SendRequest: %v", err)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result is not a map: %T", resp.Result)
	}
	if result["followed"] == nil {
		t.Fatalf("unexpected result: %v", result)
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

func TestProfileDetailRPC(t *testing.T) {
	s := newTestStore(t)
	kp := initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "pass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	if _, err := SendRequest(sock, "profile", map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("profile RPC: %v", err)
	}
	if _, err := SendRequest(sock, "detail", map[string]any{"bio": "wanderer", "location": "Wonderland"}); err != nil {
		t.Fatalf("detail RPC: %v", err)
	}

	// Verify the events landed in the own logs.
	profEvents, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		t.Fatalf("read profile log: %v", err)
	}
	if len(profEvents) != 1 || profEvents[0].Event.Profile == nil || profEvents[0].Event.Profile.DisplayName != "alice" {
		t.Fatalf("profile event mismatch: %+v", profEvents)
	}
	detEvents, err := s.OwnEvents(core.DetailLog)
	if err != nil {
		t.Fatalf("read detail log: %v", err)
	}
	if len(detEvents) != 1 || detEvents[0].Event.Detail == nil {
		t.Fatalf("detail event missing: %+v", detEvents)
	}
	det := detEvents[0].Event.Detail
	if det.Bio != "wanderer" || det.Location != "Wonderland" {
		t.Fatalf("detail fields mismatch: %+v", det)
	}
	_ = kp
}

func TestFollowsAndFollowersRPC(t *testing.T) {
	s := newTestStore(t)
	ownKP := initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "pass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	// Follow two identities.
	targetA, _ := core.NewKeyPair()
	targetB, _ := core.NewKeyPair()
	for _, target := range []core.Identity{targetA.Identity(), targetB.Identity()} {
		if _, err := SendRequest(sock, "follow", map[string]any{"target": string(target)}); err != nil {
			t.Fatalf("follow %s: %v", target, err)
		}
	}

	// follows RPC returns both.
	resp, err := SendRequest(sock, "follows", nil)
	if err != nil {
		t.Fatalf("follows: %v", err)
	}
	m, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("follows result: %v", resp.Result)
	}
	items, _ := m["follows"].([]any)
	if len(items) != 2 {
		t.Fatalf("follows: want 2, got %d (%v)", len(items), items)
	}

	// A follower follows us: store their Follow event via the sync path.
	followerKP, _ := core.NewKeyPair()
	followEvent, err := followerKP.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: 100,
		Sequence:  1,
		Follow:    &core.Follow{TargetPubkey: [32]byte(ownKP.Public)},
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if _, err := s.PutFollowedEvent(followEvent, 1); err != nil {
		t.Fatalf("PutFollowedEvent: %v", err)
	}

	// followers RPC returns the follower.
	resp, err = SendRequest(sock, "followers", nil)
	if err != nil {
		t.Fatalf("followers: %v", err)
	}
	m, ok = resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("followers result: %v", resp.Result)
	}
	items, _ = m["followers"].([]any)
	if len(items) != 1 {
		t.Fatalf("followers: want 1, got %d (%v)", len(items), items)
	}
}
