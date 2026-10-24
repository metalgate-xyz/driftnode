package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"driftnode/internal/bootstrap"
	"driftnode/internal/core"
	"driftnode/internal/store"
	syncproto "driftnode/internal/sync"

	"gopkg.in/yaml.v3"
)

// seedZen is an in-process zen that serves sync sessions over a pipe and
// offers a fixed set of zen refs during zen exchange.
type seedZen struct {
	store *store.Store
	key   *core.KeyPair
	refs  []syncproto.ZenRef
	log   *slog.Logger
}

func (z *seedZen) serve(conn net.Conn) {
	defer conn.Close()
	sess := syncproto.NewSession(z.store, z.log)
	sess.SetKey(z.key)
	sess.SetZenSource(func() []syncproto.ZenRef { return z.refs })
	if _, err := sess.RunListener(conn, conn); err != nil {
		z.log.Debug("seed session ended", "err", err)
	}
}

// refsTransport dials each token to its seedZen.
type refsTransport struct {
	seeds map[string]*seedZen
}

func (t refsTransport) Dial(_ context.Context, token string) (net.Conn, error) {
	z, ok := t.seeds[token]
	if !ok {
		return nil, fmt.Errorf("no seed for token %q", token)
	}
	clientConn, serverConn := net.Pipe()
	go z.serve(serverConn)
	return clientConn, nil
}

// flakyTransport fails the first dial, then delegates.
type flakyTransport struct {
	after *seedZen
	dials int
}

func (t *flakyTransport) Dial(ctx context.Context, token string) (net.Conn, error) {
	t.dials++
	if t.dials <= 1 {
		return nil, errors.New("seed unreachable")
	}
	return refsTransport{seeds: map[string]*seedZen{"tctoken-a": t.after}}.Dial(ctx, token)
}

func writeBootstrap(t *testing.T, tokens ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bootstrap.yaml")
	seeds := make([]bootstrap.SeedZen, len(tokens))
	for i, tok := range tokens {
		seeds[i] = bootstrap.SeedZen{Token: tok, Kind: "native_zen"}
	}
	data, err := yaml.Marshal(&bootstrap.File{Version: 1, SeedZens: seeds})
	if err != nil {
		t.Fatalf("marshal bootstrap: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	return path
}

// TestBootstrapAutoFollowsAllSeeds: when seeds follow each other, following
// the first seed exchanges zen lists and makes the second seed's token
// known before the bootstrap loop reaches it. Both must still be followed.
func TestBootstrapAutoFollowsAllSeeds(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	marxStore := openTestStore(t)
	initTestIdentityAt(t, marxStore, "pass")

	seedAStore := openTestStore(t)
	seedAKP := initTestIdentityAt(t, seedAStore, "pass")
	seedBStore := openTestStore(t)
	seedBKP := initTestIdentityAt(t, seedBStore, "pass")

	seedA := &seedZen{store: seedAStore, key: seedAKP, log: logger,
		refs: []syncproto.ZenRef{{Token: "tctoken-b", Identity: seedBKP.Identity()}}}
	seedB := &seedZen{store: seedBStore, key: seedBKP, log: logger,
		refs: []syncproto.ZenRef{{Token: "tctoken-a", Identity: seedAKP.Identity()}}}

	path := writeBootstrap(t, "tctoken-a", "tctoken-b")
	d := New(marxStore, logger)
	d.SetTransport(refsTransport{seeds: map[string]*seedZen{
		"tctoken-a": seedA,
		"tctoken-b": seedB,
	}})
	d.SetBootstrap(path, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "pass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	want := map[core.Identity]bool{seedAKP.Identity(): true, seedBKP.Identity(): true}
	waitForFollows(t, marxStore, want)
}

// TestBootstrapRetriesUnreachableSeed: a seed that fails the first dial
// is retried and followed once it answers.
func TestBootstrapRetriesUnreachableSeed(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	marxStore := openTestStore(t)
	initTestIdentityAt(t, marxStore, "pass")

	seedStore := openTestStore(t)
	seedKP := initTestIdentityAt(t, seedStore, "pass")
	seed := &seedZen{store: seedStore, key: seedKP, log: logger}

	path := writeBootstrap(t, "tctoken-a")
	d := New(marxStore, logger)
	d.bootstrapRetryDelay = 10 * time.Millisecond
	d.SetTransport(&flakyTransport{after: seed})
	d.SetBootstrap(path, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "pass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	want := map[core.Identity]bool{seedKP.Identity(): true}
	waitForFollows(t, marxStore, want)
}

// TestBootstrapUnlockDoesNotDoubleFollow: unlock retriggers the pass while
// one may still be running; the seed must get exactly one Follow event.
func TestBootstrapUnlockDoesNotDoubleFollow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	marxStore := openTestStore(t)
	initTestIdentityAt(t, marxStore, "pass")

	seedStore := openTestStore(t)
	seedKP := initTestIdentityAt(t, seedStore, "pass")
	seed := &seedZen{store: seedStore, key: seedKP, log: logger}

	path := writeBootstrap(t, "tctoken-a")
	d := New(marxStore, logger)
	d.SetTransport(refsTransport{seeds: map[string]*seedZen{"tctoken-a": seed}})
	d.SetBootstrap(path, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	for i := 0; i < 3; i++ {
		if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "pass"}); err != nil {
			t.Fatalf("unlock: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		events, err := marxStore.OwnEvents(core.ProfileLog)
		if err != nil {
			t.Fatalf("OwnEvents: %v", err)
		}
		var follows int
		for _, se := range events {
			if se.Event.Kind == core.KindFollow {
				follows++
			}
		}
		if follows > 1 {
			t.Fatalf("duplicate Follow events for the seed: %d", follows)
		}
		if follows == 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("bootstrap never followed the seed")
}

// waitForFollows polls the follow graph until every wanted identity is
// present, then checks none is missing.
func waitForFollows(t *testing.T, s *store.Store, want map[core.Identity]bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		follows, err := s.FollowGraph()
		if err != nil {
			t.Fatalf("FollowGraph: %v", err)
		}
		missing := len(want)
		for _, id := range follows {
			if want[id] {
				missing--
			}
		}
		if missing == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	for id := range want {
		t.Errorf("bootstrap did not follow seed %s", id)
	}
}
