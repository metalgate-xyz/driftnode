package sync

import (
	"bytes"
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
