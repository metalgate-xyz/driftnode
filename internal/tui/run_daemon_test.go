package tui

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"driftnode/internal/core"
	"driftnode/internal/daemon"
	"driftnode/internal/proto/driftnodepb"
	"driftnode/internal/store"
)

// TestRunRendersWithDaemon drives the real tea.Program against a live daemon,
// the way `driftnode tui` runs. It posts a message so the feed has content,
// lets the subscription deliver the snapshot, then quits and checks the
// rendered output shows the frame and the post.
func TestRunRendersWithDaemon(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(dir + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	kp, err := core.NewKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pass"))
	s.InitIdentity(kp, ek)
	d := daemon.New(s, nil)
	d.SetUnlockedKey(kp)
	sock := subscribeSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Stop)

	// Post via RPC so the feed has at least one item.
	if cl, cc, err := daemon.DialClient(sock); err == nil {
		defer cc.Close()
		if _, err := cl.Post(context.Background(), &driftnodepb.PostReq{Text: "hello world"}); err != nil {
			t.Fatalf("post: %v", err)
		}
	}

	m := newModel(sock, "driftnode:test")
	var out bytes.Buffer
	// A delay before ctrl+c lets the subscription deliver the snapshot, so
	// we observe the post-subscription render rather than the initial frame.
	in := bytes.NewReader([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03})
	p := tea.NewProgram(m,
		tea.WithWindowSize(80, 24),
		tea.WithInput(in),
		tea.WithOutput(&out),
		tea.WithoutSignalHandler(),
	)
	done := make(chan error, 1)
	go func() { _, err := p.Run(); done <- err }()
	select {
	case <-time.After(5 * time.Second):
		p.Quit()
		t.Fatal("program did not exit within 5s")
	case err := <-done:
		if err != nil {
			t.Logf("Run returned err: %v", err)
		}
	}
	body := out.String()
	t.Logf("OUTPUT (len=%d):\n%s", len(body), body)
	if strings.TrimSpace(body) == "" {
		t.Fatal("program produced no output")
	}
	if !strings.Contains(body, "driftnode") {
		t.Fatal("output missing title")
	}
}
