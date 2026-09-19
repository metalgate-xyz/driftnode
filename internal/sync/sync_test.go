package sync

import (
	"bytes"
	"crypto/ed25519"
	"io"
	"testing"

	"driftnode/internal/core"
	"driftnode/internal/store"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(dir + "/test.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func makeKey(t *testing.T) *core.KeyPair {
	t.Helper()
	kp, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	return kp
}

func signPost(t *testing.T, kp *core.KeyPair, seq uint64, ts int64, text string) *core.SignedEvent {
	t.Helper()
	se, err := kp.Sign(core.Event{
		Kind:      core.KindPost,
		Log:       core.PostLog,
		Timestamp: ts,
		Sequence:  seq,
		Post:      &core.Post{Text: text},
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return se
}

func TestMessageRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		msg  *Message
	}{
		{"request", NewRequest(core.PostLog, 12345, "")},
		{"events", NewEvents(nil)},
		{"done", NewDone()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteMsg(&buf, c.msg); err != nil {
				t.Fatalf("WriteMsg: %v", err)
			}
			got, err := ReadMsg(&buf)
			if err != nil {
				t.Fatalf("ReadMsg: %v", err)
			}
			if got.Kind != c.msg.Kind {
				t.Fatalf("kind: want %d, got %d", c.msg.Kind, got.Kind)
			}
		})
	}
}

func TestSyncFullRoundTrip(t *testing.T) {
	serverStore := newTestStore(t)
	clientStore := newTestStore(t)

	kp := makeKey(t)
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pass"))
	serverStore.InitIdentity(kp, ek)

	se1 := signPost(t, kp, 1, 100, "first")
	se2 := signPost(t, kp, 2, 200, "second")
	serverStore.AppendOwnEvent(core.PostLog, se1)
	serverStore.AppendOwnEvent(core.PostLog, se2)

	// Create a pipe pair for bidirectional communication.
	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()

	srv := NewServer(serverStore, nil)
	go func() {
		defer serverW.Close()
		srv.Serve(serverR, serverW)
	}()

	defer clientR.Close()
	defer clientW.Close()

	cl := NewClient(clientStore, nil)
	merged, err := cl.SyncLog(clientR, clientW, core.PostLog, 0, "")
	if err != nil {
		t.Fatalf("SyncLog: %v", err)
	}
	if merged != 2 {
		t.Fatalf("merged: want 2, got %d", merged)
	}

	// Verify the client has the events.
	clientEvents, err := clientStore.FollowedEvents(kp.Identity())
	if err != nil {
		t.Fatalf("FollowedEvents: %v", err)
	}
	if len(clientEvents) != 2 {
		t.Fatalf("client events: want 2, got %d", len(clientEvents))
	}
}

func TestSyncAfterTimestamp(t *testing.T) {
	serverStore := newTestStore(t)
	clientStore := newTestStore(t)

	kp := makeKey(t)
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pass"))
	serverStore.InitIdentity(kp, ek)

	serverStore.AppendOwnEvent(core.PostLog, signPost(t, kp, 1, 100, "first"))
	serverStore.AppendOwnEvent(core.PostLog, signPost(t, kp, 2, 200, "second"))
	serverStore.AppendOwnEvent(core.PostLog, signPost(t, kp, 3, 300, "third"))

	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()

	srv := NewServer(serverStore, nil)
	go func() {
		defer serverW.Close()
		srv.Serve(serverR, serverW)
	}()
	defer clientR.Close()
	defer clientW.Close()

	cl := NewClient(clientStore, nil)
	merged, err := cl.SyncLog(clientR, clientW, core.PostLog, 150, "")
	if err != nil {
		t.Fatalf("SyncLog: %v", err)
	}
	if merged != 2 {
		t.Fatalf("merged: want 2 (after 150), got %d", merged)
	}
}

func TestSyncRejectsForgedEvent(t *testing.T) {
	serverStore := newTestStore(t)
	clientStore := newTestStore(t)

	kp := makeKey(t)
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pass"))
	serverStore.InitIdentity(kp, ek)

	// Sign a valid post, then tamper with the text (breaks signature).
	se := signPost(t, kp, 1, 100, "original")
	se.Event.Post.Text = "forged"
	serverStore.AppendOwnEvent(core.PostLog, se)

	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()

	srv := NewServer(serverStore, nil)
	go func() {
		defer serverW.Close()
		srv.Serve(serverR, serverW)
	}()
	defer clientR.Close()
	defer clientW.Close()

	cl := NewClient(clientStore, nil)
	merged, err := cl.SyncLog(clientR, clientW, core.PostLog, 0, "")
	if err != nil {
		t.Fatalf("SyncLog: %v", err)
	}
	if merged != 0 {
		t.Fatalf("merged: want 0 (forged), got %d", merged)
	}
}

// TestSessionHandshake proves two sessions with their respective keys
// authenticate each other: each learns the other's driftnode identity, and
// the sync protocol runs to completion over the authenticated connection.
func TestSessionHandshake(t *testing.T) {
	aliceStore := newTestStore(t)
	bobStore := newTestStore(t)
	aliceKP := makeKey(t)
	bobKP := makeKey(t)

	enc := core.DefaultKeyEncryption()
	ae, _ := enc.Encrypt(aliceKP.Private, []byte("a"))
	aliceStore.InitIdentity(aliceKP, ae)
	be, _ := enc.Encrypt(bobKP.Private, []byte("b"))
	bobStore.InitIdentity(bobKP, be)

	// Give bob a post so alice has something to pull.
	bobStore.AppendOwnEvent(core.PostLog, signPost(t, bobKP, 1, 100, "hi"))

	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()

	// Server side (bob), listener.
	serverDone := make(chan error, 1)
	go func() {
		defer serverW.Close()
		defer serverR.Close()
		sess := NewSession(bobStore, nil)
		sess.SetKey(bobKP)
		if _, err := sess.RunListener(serverR, serverW); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	defer clientR.Close()
	defer clientW.Close()

	// Client side (alice), initiator.
	var authed core.Identity
	sess := NewSession(aliceStore, nil)
	sess.SetKey(aliceKP)
	sess.SetAuthed(func(id core.Identity) { authed = id })
	_, err := sess.RunInitiator(clientR, clientW)
	if err != nil {
		t.Fatalf("RunInitiator: %v", err)
	}
	if authed != bobKP.Identity() {
		t.Fatalf("authed identity: want %s, got %s", bobKP.Identity(), authed)
	}
	// Bob's post must have landed in alice's store.
	events, err := aliceStore.FollowedEvents(bobKP.Identity())
	if err != nil {
		t.Fatalf("FollowedEvents: %v", err)
	}
	found := false
	for _, se := range events {
		if se.Event.Post != nil && se.Event.Post.Text == "hi" {
			found = true
		}
	}
	if !found {
		t.Fatalf("bob's post not synced into alice's store; events=%d", len(events))
	}
}

// TestSessionHandshakeRejectsBadSignature proves a session rejects a zen
// whose Auth signature does not verify against the identity it announced.
func TestSessionHandshakeRejectsBadSignature(t *testing.T) {
	aliceKP := makeKey(t)
	malloryKP := makeKey(t) // different key, signs the nonce

	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()

	// Server (the "zen") announces alice's identity but signs with
	// mallory's key, so the Auth signature won't verify.
	serverDone := make(chan error, 1)
	go func() {
		defer serverW.Close()
		defer serverR.Close()
		ourNonce := [32]byte{1, 2, 3}
		if err := WriteMsg(serverW, NewHello(aliceKP.Identity(), ourNonce)); err != nil {
			serverDone <- err
			return
		}
		zenHello, err := ReadMsg(serverR)
		if err != nil {
			serverDone <- err
			return
		}
		sig := ed25519.Sign(malloryKP.Private, zenHello.Hello.Nonce[:])
		if err := WriteMsg(serverW, NewAuth(sig)); err != nil {
			serverDone <- err
			return
		}
		// Expect the client to hang up; a read error here is fine.
		ReadMsg(serverR)
		serverDone <- nil
	}()

	defer clientR.Close()
	defer clientW.Close()

	sess := NewSession(nil, nil)
	sess.SetKey(aliceKP)
	_, err := sess.Handshake(clientR, clientW)
	if err == nil {
		t.Fatal("Handshake: expected error for bad signature, got nil")
	}
}

// TestSessionHandshakeSkipsWhenNoKey proves a nil key skips the handshake,
// so direct Server/Client callers that don't use sessions still work.
func TestSessionHandshakeSkipsWhenNoKey(t *testing.T) {
	sess := NewSession(nil, nil)
	_, err := sess.Handshake(nil, nil)
	if err != nil {
		t.Fatalf("Handshake with nil key should skip, got: %v", err)
	}
}

// TestZensMessageRoundTrip proves a Zens message built with NewZens round-trips
// through WriteMsg/ReadMsg preserving the refs' tokens, identities, and name
// hints, and that the backward-compat Tokens list is still populated for peers
// that predate the Items field.
func TestZensMessageRoundTrip(t *testing.T) {
	refs := []ZenRef{
		{Token: "tok-a", Name: "alice"},
		{Token: "tok-b", Identity: core.Identity("id-b"), Name: "bob"},
		{Token: "tok-c"},
	}
	var buf bytes.Buffer
	if err := WriteMsg(&buf, NewZens(refs)); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	got, err := ReadMsg(&buf)
	if err != nil {
		t.Fatalf("ReadMsg: %v", err)
	}
	if got.Kind != MsgZens {
		t.Fatalf("kind: want %d, got %d", MsgZens, got.Kind)
	}
	// Items must round-trip exactly.
	if len(got.Zens.Items) != len(refs) {
		t.Fatalf("items: want %d, got %d", len(refs), len(got.Zens.Items))
	}
	for i, want := range refs {
		g := got.Zens.Items[i]
		if g.Token != want.Token || g.Identity != want.Identity || g.Name != want.Name {
			t.Fatalf("item %d: want %+v, got %+v", i, want, g)
		}
	}
	// Tokens is the backward-compat list, one per ref.
	if len(got.Zens.Tokens) != len(refs) {
		t.Fatalf("tokens: want %d, got %d", len(refs), len(got.Zens.Tokens))
	}
	for i, want := range refs {
		if got.Zens.Tokens[i] != want.Token {
			t.Fatalf("token %d: want %q, got %q", i, want.Token, got.Zens.Tokens[i])
		}
	}
}

// TestZensFromMsgBackwardCompat proves zensFromMsg recovers refs from a legacy
// Zens message that carries only Tokens (no Items), for peers that predate the
// Items field.
func TestZensFromMsgBackwardCompat(t *testing.T) {
	// Hand-build a legacy message: Tokens only, no Items.
	msg := &Message{Kind: MsgZens, Zens: &Zens{Tokens: []string{"old-tok-1", "old-tok-2"}}}
	refs := zensFromMsg(msg)
	if len(refs) != 2 {
		t.Fatalf("refs: want 2, got %d", len(refs))
	}
	for i, want := range []string{"old-tok-1", "old-tok-2"} {
		if refs[i].Token != want {
			t.Fatalf("ref %d token: want %q, got %q", i, want, refs[i].Token)
		}
		if refs[i].Identity != "" || refs[i].Name != "" {
			t.Fatalf("ref %d: legacy ref should have empty identity/name, got %+v", i, refs[i])
		}
	}
}
