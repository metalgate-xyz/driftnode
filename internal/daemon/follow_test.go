package daemon

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"driftnode/internal/core"
	syncproto "driftnode/internal/sync"
)

// TestFollowByPubkeyFlipsConnected reproduces: a zen is discovered (token
// known via zen exchange), then followed by pubkey. The status should flip
// to connected by the follow call itself, without a manual sync.
func TestFollowByPubkeyFlipsConnected(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	aliceStore := openTestStore(t)
	bobStore := openTestStore(t)

	initTestIdentityAt(t, aliceStore, "alicepass")
	bobKP := initTestIdentityAt(t, bobStore, "bobpass")

	bobTok := "tok-bob"
	transport := multiPipeTransport{
		servers: map[string]pipeTransport{
			bobTok: {serverStore: bobStore, serverKey: bobKP, logger: logger},
		},
	}

	d := New(aliceStore, logger)
	d.SetTransport(transport)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "alicepass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	// Bob is discovered via zen exchange: token is known, status "discovered".
	d.learnZenRef(syncproto.ZenRef{Token: bobTok, Identity: bobKP.Identity()})

	// Follow bob by pubkey (as the TUI does with z.identity). The follow
	// call itself dials and flips status; no separate sync round needed.
	if _, err := SendRequest(sock, "follow", map[string]any{
		"target":     bobKP.Identity().String(),
		"passphrase": "alicepass",
	}); err != nil {
		t.Fatalf("follow: %v", err)
	}

	// The follow RPC returns only after the dial+handshake complete, so
	// connected should already be set. Poll briefly to tolerate the async
	// sync sink that runs in connectAndSync after the handshake.
	deadline := time.Now().Add(3 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		d.mu.Lock()
		if p, ok := d.zens[bobTok]; ok {
			status = p.Status
		}
		d.mu.Unlock()
		if status == "connected" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status != "connected" {
		t.Fatalf("status did not flip to connected; got %q", status)
	}

	// Routing must be bound so future sync rounds dial by identity.
	if tok, ok, _ := aliceStore.Routing(bobKP.Identity()); !ok || tok != bobTok {
		t.Fatalf("routing not bound: ok=%v tok=%q", ok, tok)
	}
}

// TestFollowByPubkeyPreservesName verifies that following a discovered zen
// by pubkey does not clobber the name/identity hint learned via exchange.
func TestFollowByPubkeyPreservesName(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	aliceStore := openTestStore(t)
	bobStore := openTestStore(t)

	initTestIdentityAt(t, aliceStore, "alicepass")
	bobKP := initTestIdentityAt(t, bobStore, "bobpass")

	bobTok := "tok-bob"
	transport := multiPipeTransport{
		servers: map[string]pipeTransport{
			bobTok: {serverStore: bobStore, serverKey: bobKP, logger: logger},
		},
	}

	d := New(aliceStore, logger)
	d.SetTransport(transport)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "alicepass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	// Bob is discovered with a name hint.
	d.learnZenRef(syncproto.ZenRef{Token: bobTok, Identity: bobKP.Identity(), Name: "bob"})

	if _, err := SendRequest(sock, "follow", map[string]any{
		"target":     bobKP.Identity().String(),
		"passphrase": "alicepass",
	}); err != nil {
		t.Fatalf("follow: %v", err)
	}

	d.mu.Lock()
	p := d.zens[bobTok]
	d.mu.Unlock()
	if p == nil {
		t.Fatalf("zen missing")
	}
	if p.Identity != string(bobKP.Identity()) {
		t.Fatalf("identity clobbered: got %q", p.Identity)
	}
	if p.Name != "bob" {
		t.Fatalf("name clobbered: got %q", p.Name)
	}
}

// TestFollowByPubkeyNoTokenFallsBack verifies that when no token is known for
// the identity, following by pubkey writes the event and leaves the zen for
// the sync loop to resolve, rather than erroring.
func TestFollowByPubkeyNoTokenFallsBack(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	aliceStore := openTestStore(t)
	bobStore := openTestStore(t)

	initTestIdentityAt(t, aliceStore, "alicepass")
	bobKP := initTestIdentityAt(t, bobStore, "bobpass")

	// Transport knows bob's token, but the daemon does not learn it via
	// exchange before the follow, so no token is resolvable at follow time.
	bobTok := "tok-bob"
	transport := multiPipeTransport{
		servers: map[string]pipeTransport{
			bobTok: {serverStore: bobStore, serverKey: bobKP, logger: logger},
		},
	}

	d := New(aliceStore, logger)
	d.SetTransport(transport)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "alicepass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	// Follow by pubkey with no known token: should succeed by writing the
	// event; the sync loop resolves the token later.
	resp, err := SendRequest(sock, "follow", map[string]any{
		"target":     bobKP.Identity().String(),
		"passphrase": "alicepass",
	})
	if err != nil {
		t.Fatalf("follow: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("follow errored with no known token: %s", resp.Error)
	}
	m, _ := resp.Result.(map[string]any)
	if m["followed"] != core.IdentityFromPubkey(bobKP.Public).String() {
		t.Fatalf("unexpected follow result: %v", resp.Result)
	}

	// The Follow event must be in the store's own ProfileLog.
	ids, err := aliceStore.FollowGraph()
	if err != nil {
		t.Fatalf("FollowGraph: %v", err)
	}
	found := false
	for _, id := range ids {
		if id == bobKP.Identity() {
			found = true
		}
	}
	if !found {
		t.Fatalf("bob not in follow graph")
	}
}
