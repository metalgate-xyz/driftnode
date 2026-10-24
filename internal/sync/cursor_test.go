package sync

import (
	"io"
	"testing"

	"driftnode/internal/core"
)

// TestProfileLogPullRequestsFromZero verifies that a ProfileLog pull
// requests from zero even when a stored cursor exists, so follower edges
// can never be stranded by stale-cursor state (the cursor trap), while a
// PostLog pull still uses its stored cursor.
func TestProfileLogPullRequestsFromZero(t *testing.T) {
	serverStore := newTestStore(t)
	clientStore := newTestStore(t)

	kp := makeKey(t)
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pass"))
	serverStore.InitIdentity(kp, ek)

	// Server holds a Profile event and a Post event, both older than the
	// seeded cursors.
	profile, err := kp.Sign(core.Event{
		Kind: core.KindProfile, Log: core.ProfileLog, Timestamp: 100, Sequence: 1,
		Profile: &core.Profile{DisplayName: "server"},
	})
	if err != nil {
		t.Fatalf("sign profile: %v", err)
	}
	if err := serverStore.AppendOwnEvent(core.ProfileLog, profile); err != nil {
		t.Fatalf("append profile: %v", err)
	}
	post := signPost(t, kp, 1, 200, "post")
	if err := serverStore.AppendOwnEvent(core.PostLog, post); err != nil {
		t.Fatalf("append post: %v", err)
	}

	// Seed cursors past both events, as if a previous session had pulled
	// everything. The ProfileLog pull must ignore its cursor and still
	// receive the profile event; the PostLog pull must honor its cursor
	// and receive nothing.
	if err := clientStore.PutSyncCursor(kp.Identity(), core.ProfileLog, 1000); err != nil {
		t.Fatalf("seed profile cursor: %v", err)
	}
	if err := clientStore.PutSyncCursor(kp.Identity(), core.PostLog, 1000); err != nil {
		t.Fatalf("seed post cursor: %v", err)
	}

	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()

	serverDone := make(chan error, 1)
	go func() {
		defer serverW.Close()
		defer serverR.Close()
		sess := NewSession(serverStore, nil)
		sess.SetKey(kp)
		if _, err := sess.RunListener(serverR, serverW); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()
	defer clientR.Close()
	defer clientW.Close()

	sess := NewSession(clientStore, nil)
	sess.SetKey(kp)
	sess.SetCursorSource(clientStore.SyncCursor)
	sess.SetCursorSink(clientStore.PutSyncCursor)
	if _, err := sess.RunInitiator(clientR, clientW); err != nil {
		t.Fatalf("RunInitiator: %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server: %v", err)
	}

	// The profile event was re-delivered despite the cursor.
	got, err := clientStore.CrawledEvents(kp.Identity())
	if err != nil {
		t.Fatalf("CrawledEvents: %v", err)
	}
	var haveProfile bool
	for _, se := range got {
		if se.Event.Kind == core.KindProfile {
			haveProfile = true
		}
		if se.Event.Kind == core.KindPost {
			t.Fatal("PostLog pull ignored its cursor: post was re-delivered")
		}
	}
	if !haveProfile {
		t.Fatal("ProfileLog pull used its cursor: profile event not re-delivered")
	}

	// The ProfileLog cursor is not written by the session; the seeded
	// value is unchanged. The PostLog cursor stays at the seeded value
	// (no events arrived, so no advance).
	pc, err := clientStore.SyncCursor(kp.Identity(), core.ProfileLog)
	if err != nil {
		t.Fatalf("profile cursor read: %v", err)
	}
	if pc != 1000 {
		t.Fatalf("profile cursor changed: want 1000, got %d", pc)
	}
}
