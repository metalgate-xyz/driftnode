package tui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	tea "charm.land/bubbletea/v2"

	"driftnode/internal/core"
	"driftnode/internal/daemon"
	"driftnode/internal/proto/driftnodepb"
	"driftnode/internal/store"
)

// subscribeSocketPath returns a short, unique Unix socket path for a test
// daemon. It mirrors the daemon test helper to stay under macOS's sockaddr
// limit, using a per-test temp dir rather than the nested t.TempDir() path.
func subscribeSocketPath(t *testing.T) string {
	t.Helper()
	h := sha256.Sum256([]byte(t.Name()))
	sock := fmt.Sprintf("%s/driftnode-tui-socks/ctrl-%s", os.TempDir(), hex.EncodeToString(h[:8]))
	if err := os.MkdirAll(filepath.Dir(sock), 0o700); err != nil {
		t.Fatalf("create socket dir: %v", err)
	}
	t.Cleanup(func() { os.Remove(sock) })
	return sock
}

func subscribeTestStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func subscribeInitIdentity(t *testing.T, s *store.Store) *core.KeyPair {
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

// startSubscribeDaemon starts a daemon with an identity and unlocked key, so
// the post RPC works without a transport.
func startSubscribeDaemon(t *testing.T) (*daemon.Daemon, string) {
	t.Helper()
	s := subscribeTestStore(t)
	kp := subscribeInitIdentity(t, s)
	d := daemon.New(s, nil)
	d.SetUnlockedKey(kp)
	sock := subscribeSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(d.Stop)
	return d, sock
}

// drainMsgs runs a tea.Cmd and its continuations until done is true or the
// subscription closes, collecting the produced messages. It drives the real
// TUI subscription commands against a live daemon.
func drainMsgs(t *testing.T, cmd tea.Cmd, done func(tea.Msg) bool) []tea.Msg {
	t.Helper()
	var out []tea.Msg
	for i := 0; i < 1000; i++ {
		msg := cmd()
		out = append(out, msg)
		if done(msg) {
			return out
		}
		switch m := msg.(type) {
		case snapshotMsg:
			cmd = subscribeContinueCmd(m.sub)
		case diffMsg:
			if m.sub != nil {
				cmd = subscribeContinueCmd(m.sub)
			} else {
				return out
			}
		case subClosedMsg:
			return out
		default:
			return out
		}
	}
	t.Fatal("drainMsgs: did not terminate")
	return out
}

// TestSubscribeEndToEnd verifies the full push contract: the subscribe client
// reads a snapshot, then receives a feed diff when the daemon receives a
// post. The TUI never polls and never replaces the whole panel.
func TestSubscribeEndToEnd(t *testing.T) {
	d, sock := startSubscribeDaemon(t)

	// Open the subscription and read the snapshot.
	cmd := subscribeCmd(sock)
	msgs := drainMsgs(t, cmd, func(m tea.Msg) bool {
		_, ok := m.(snapshotMsg)
		return ok
	})
	var snap snapshotMsg
	for _, m := range msgs {
		if s, ok := m.(snapshotMsg); ok {
			snap = s
		}
	}
	if snap.err != nil {
		t.Fatalf("snapshot err: %v", snap.err)
	}
	if snap.sub == nil {
		t.Fatal("snapshot missing subscription handle")
	}

	// Post something via the one-shot RPC; the daemon pushes a feed diff.
	if cl, cc, err := daemon.DialClient(sock); err == nil {
		defer cc.Close()
		if _, err := cl.Post(context.Background(), &driftnodepb.PostReq{Text: "hello from test"}); err != nil {
			t.Fatalf("post: %v", err)
		}
	}

	// Continue draining the stream until the feed diff arrives.
	got := drainMsgs(t, subscribeContinueCmd(snap.sub), func(m tea.Msg) bool {
		d, ok := m.(diffMsg)
		return ok && d.panel == "feed"
	})
	var feedAdd diffMsg
	for _, m := range got {
		if d, ok := m.(diffMsg); ok && d.panel == "feed" {
			feedAdd = d
		}
	}
	if len(feedAdd.add) == 0 {
		t.Fatal("feed diff should carry the new post")
	}
	if feedAdd.add[0].text != "hello from test" {
		t.Fatalf("feed diff post: want %q, got %q", "hello from test", feedAdd.add[0].text)
	}
	if feedAdd.add[0].id == "" {
		t.Fatal("feed diff post should carry an event id")
	}
	_ = d
}

// TestSubscribeDaemonDown verifies the subscribe client surfaces a connection
// error as snapshotMsg.err when the daemon is not running, so the model can
// schedule a reconnect.
func TestSubscribeDaemonDown(t *testing.T) {
	sock := subscribeSocketPath(t)
	msg := subscribeCmd(sock)()
	snap, ok := msg.(snapshotMsg)
	if !ok {
		t.Fatalf("want snapshotMsg, got %T", msg)
	}
	if snap.err == nil {
		t.Fatal("want error when daemon not running, got nil")
	}
	if snap.sub != nil {
		t.Fatal("subscription handle should be nil on error")
	}
}

// TestSubscribeReconnectsAfterDrop verifies the model schedules a reconnect
// when the stream ends (connection closed), so a dropped subscriber
// re-snapshots instead of going dark.
func TestSubscribeReconnectsAfterDrop(t *testing.T) {
	d, sock := startSubscribeDaemon(t)
	m := newModel(sock, "driftnode:test")
	m.width, m.height = 80, 24
	m.layout()

	// Drive the subscribe command directly to a snapshot (Init returns a
	// batch with the focus command; calling a batch blocks collecting
	// sub-results, so we exercise subscribeCmd in isolation).
	snap, ok := subscribeCmd(sock)().(snapshotMsg)
	if !ok {
		t.Fatal("subscribe did not produce a snapshot")
	}
	if snap.err != nil {
		t.Fatalf("snapshot: %v", snap.err)
	}
	mm, next := m.Update(snap)
	m = mm.(model)
	if next == nil {
		t.Fatal("snapshot should schedule a continue command")
	}

	// Stopping the daemon closes the subscriber connection; the next read
	// yields subClosedMsg, which the model turns into a reconnect command.
	d.Stop()

	var gotClose bool
	for i := 0; i < 100 && !gotClose; i++ {
		msg := next()
		if c, ok := msg.(subClosedMsg); ok {
			gotClose = true
			mm, next = m.Update(c)
			m = mm.(model)
			break
		}
	}
	if !gotClose {
		t.Fatal("did not observe stream close")
	}
	if next == nil {
		t.Fatal("stream close should schedule a reconnect")
	}
}
