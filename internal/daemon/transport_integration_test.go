package daemon

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"driftnode/internal/core"
	"driftnode/internal/store"
	syncproto "driftnode/internal/sync"
)

// pipeTransport implements Transport by pairing an in-process net.Pipe to a
// running sync session, so the daemon's dial-and-sync path can be exercised
// without a tailcat network monitor. Each Dial spins up a server-side session
// (with handshake) on the remote end of the pipe, using the zen's keypair.
type pipeTransport struct {
	serverStore *store.Store
	serverKey   *core.KeyPair
	logger      *slog.Logger
}

func (t pipeTransport) Dial(ctx context.Context, token string) (net.Conn, error) {
	clientConn, serverConn := net.Pipe()
	go func() {
		defer serverConn.Close()
		sess := syncproto.NewSession(t.serverStore, t.logger)
		sess.SetKey(t.serverKey)
		if _, err := sess.RunListener(serverConn, serverConn); err != nil {
			t.logger.Debug("pipe sync session ended", "err", err)
		}
	}()
	return clientConn, nil
}

// TestDaemonZenSyncOverTransport proves the daemon's follow / sync path
// dials a zen transport, runs the real sync protocol, and merges the zen's
// events into the local store. This exercises the actual daemon code path
// (control-socket dispatch → handleFollow → Transport.Dial → handshake →
// sync protocol → store merge) over an injectable transport, since tailcat
// needs a network monitor unavailable in CI/sandboxes.
func TestDaemonZenSyncOverTransport(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Set up two stores: the daemon's (alice) and the zen's (bob).
	aliceStore := openTestStore(t)
	bobStore := openTestStore(t)

	aliceKP := initTestIdentityAt(t, aliceStore, "alicepass")
	bobKP := initTestIdentityAt(t, bobStore, "bobpass")

	// Bob posts to his own PostLog.
	if err := bobStore.AppendOwnEvent(core.PostLog, mustSignPost(t, bobKP, "hello from bob", 1)); err != nil {
		t.Fatalf("bob post: %v", err)
	}

	// Daemon backed by alice's store, with an in-process transport that
	// connects to bob's sync server.
	d := New(aliceStore, logger)
	d.SetTransport(pipeTransport{serverStore: bobStore, serverKey: bobKP, logger: logger})
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	// Unlock alice's key so the daemon can authenticate the session.
	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "alicepass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	// follow <token>: dial, learn identity, write Follow, bind routing.
	// Pass a tc-prefixed token so handleFollow takes the dial path (the
	// pipe transport ignores the token value).
	resp, err := SendRequest(sock, "follow", map[string]any{
		"target":     "tctest-token-bob",
		"passphrase": "alicepass",
	})
	if err != nil {
		t.Fatalf("follow: %v", err)
	}
	if _, ok := resp.Result.(map[string]any); !ok {
		t.Fatalf("follow result: %T", resp.Result)
	}

	// Give the async sync round time to complete.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		posts, _ := aliceStore.AllPosts()
		if len(posts) >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Alice's merged feed must now contain bob's post.
	posts, err := aliceStore.AllPosts()
	if err != nil {
		t.Fatalf("AllPosts: %v", err)
	}
	found := false
	for _, se := range posts {
		if se.Author == bobKP.Identity() && se.Event.Post != nil && se.Event.Post.Text == "hello from bob" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("bob's post not synced into alice's store; posts=%d", len(posts))
	}

	_ = aliceKP
}

// TestDaemonSyncNowOverTransport proves the sync control method dials
// all known zens and runs the protocol.
func TestDaemonSyncNowOverTransport(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	aliceStore := openTestStore(t)
	bobStore := openTestStore(t)
	bobKP := initTestIdentityAt(t, bobStore, "bobpass")
	initTestIdentityAt(t, aliceStore, "alicepass")

	if err := bobStore.AppendOwnEvent(core.PostLog, mustSignPost(t, bobKP, "bob again", 1)); err != nil {
		t.Fatalf("bob post: %v", err)
	}

	d := New(aliceStore, logger)
	d.SetTransport(pipeTransport{serverStore: bobStore, serverKey: bobKP, logger: logger})
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	// Unlock alice's key so the daemon can authenticate the session.
	if _, err := SendRequest(sock, "unlock", map[string]any{"passphrase": "alicepass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	// Follow bob (by token) first, then trigger an explicit sync
	// and confirm events still present.
	_, _ = SendRequest(sock, "follow", map[string]any{
		"target":     "tctest-token-bob",
		"passphrase": "alicepass",
	})

	// Wait for the first sync round.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		posts, _ := aliceStore.AllPosts()
		if len(posts) >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Trigger sync; should be idempotent (0 new events) but not error.
	resp, err := SendRequest(sock, "sync", nil)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if resp.Result.(map[string]any)["status"] != "triggered" {
		t.Fatalf("sync status: %v", resp.Result)
	}

	// Wait a moment for the background sync to land, then verify bob's post.
	time.Sleep(200 * time.Millisecond)
	posts, _ := aliceStore.AllPosts()
	found := false
	for _, se := range posts {
		if se.Author == bobKP.Identity() && se.Event.Post != nil && se.Event.Post.Text == "bob again" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("bob's post not present after sync; posts=%d", len(posts))
	}
}

// openTestStore opens a bbolt store in a temp dir.
func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// initTestIdentityAt initializes a store with a fresh identity and passphrase.
func initTestIdentityAt(t *testing.T, s *store.Store, passphrase string) *core.KeyPair {
	t.Helper()
	kp, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	enc := core.DefaultKeyEncryption()
	ek, err := enc.Encrypt(kp.Private, []byte(passphrase))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("InitIdentity: %v", err)
	}
	return kp
}

// mustSignPost signs a Post event with the given keypair and sequence.
func mustSignPost(t *testing.T, kp *core.KeyPair, text string, seq uint64) *core.SignedEvent {
	t.Helper()
	se, err := kp.Sign(core.Event{
		Kind:      core.KindPost,
		Log:       core.PostLog,
		Timestamp: core.Now64(),
		Sequence:  seq,
		Post:      &core.Post{Text: text},
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return se
}

// multiPipeTransport routes each Dial to one of several in-process sync
// servers keyed by token, so a test can stand up multiple zens behind a
// single Transport.
type multiPipeTransport struct {
	servers map[string]pipeTransport
}

func (m multiPipeTransport) Dial(ctx context.Context, token string) (net.Conn, error) {
	srv, ok := m.servers[token]
	if !ok {
		return nil, fmt.Errorf("unknown token %q", token)
	}
	return srv.Dial(ctx, token)
}

// TestResolvePendingFollowsDoesNotSyncNonFollowedZen proves that resolving a
// pending (pubkey-only) follow only probes known tokens to learn their
// identity, and does not pull logs from zens that are not the followed
// identity. Alice follows Carol by pubkey (no routing token); Bob is a
// known token but not followed. After sync, Carol's post is present and
// Bob's post is absent from Alice's follows cache.
func TestResolvePendingFollowsDoesNotSyncNonFollowedZen(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	aliceStore := openTestStore(t)
	bobStore := openTestStore(t)
	carolStore := openTestStore(t)

	_ = initTestIdentityAt(t, aliceStore, "alicepass")
	bobKP := initTestIdentityAt(t, bobStore, "bobpass")
	carolKP := initTestIdentityAt(t, carolStore, "carolpass")

	if err := bobStore.AppendOwnEvent(core.PostLog, mustSignPost(t, bobKP, "bob post", 1)); err != nil {
		t.Fatalf("bob post: %v", err)
	}
	if err := carolStore.AppendOwnEvent(core.PostLog, mustSignPost(t, carolKP, "carol post", 1)); err != nil {
		t.Fatalf("carol post: %v", err)
	}

	bobTok, carolTok := "tok-bob", "tok-carol"
	transport := multiPipeTransport{
		servers: map[string]pipeTransport{
			bobTok:   {serverStore: bobStore, serverKey: bobKP, logger: logger},
			carolTok: {serverStore: carolStore, serverKey: carolKP, logger: logger},
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

	// Follow Carol by pubkey: pending follow, no routing token bound.
	if _, err := SendRequest(sock, "follow", map[string]any{"target": carolKP.Identity().String()}); err != nil {
		t.Fatalf("follow carol: %v", err)
	}

	// Learn Bob's and Carol's tokens via zen exchange (as would happen
	// during a real sync). Both become known tokens, but neither is
	// auto-dialed until intent (a follow) exists.
	d.learnZenRef(syncproto.ZenRef{Token: bobTok, Identity: bobKP.Identity()})
	d.learnZenRef(syncproto.ZenRef{Token: carolTok, Identity: carolKP.Identity()})

	// Trigger a sync round: Carol is a pending follow, so
	// resolvePendingFollows probes both known tokens. Only Carol's token
	// matches a pending follow and gets synced; Bob's token is probed
	// (handshake only) and must not pull Bob's post.
	if _, err := SendRequest(sock, "sync", nil); err != nil {
		t.Fatalf("sync: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var carolPresent, bobPresent bool
	for time.Now().Before(deadline) && !carolPresent {
		carolPresent = storeHasPost(aliceStore, carolKP.Identity(), "carol post")
		time.Sleep(50 * time.Millisecond)
	}
	if !carolPresent {
		t.Fatalf("carol's post not synced into alice's store")
	}
	// Give any stray Bob sync a moment to land, then assert it never did.
	time.Sleep(300 * time.Millisecond)
	bobPresent = storeHasPost(aliceStore, bobKP.Identity(), "bob post")
	if bobPresent {
		t.Fatalf("bob's post leaked into alice's store via a non-followed token probe")
	}
}

// storeHasPost reports whether the store has a PostLog event by author with
// the given text in its follows cache.
func storeHasPost(s *store.Store, author core.Identity, text string) bool {
	events, err := s.CrawledEvents(author)
	if err != nil {
		return false
	}
	for _, se := range events {
		if se.Event.Log == core.PostLog && se.Event.Post != nil && se.Event.Post.Text == text {
			return true
		}
	}
	return false
}
