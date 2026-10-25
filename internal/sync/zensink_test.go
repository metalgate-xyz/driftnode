package sync

import (
	"io"
	"testing"

	"driftnode/internal/core"
)

// The zen sink receives the refs of one MsgZens as a single batch, so a
// full zen-exchange offer costs the daemon one token-learning pass instead
// of one per ref.
func TestZenSinkReceivesBatchPerMessage(t *testing.T) {
	serverStore := newTestStore(t)
	clientStore := newTestStore(t)

	kp := makeKey(t)
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pass"))
	serverStore.InitIdentity(kp, ek)

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

	var batches [][]ZenRef
	sess := NewSession(clientStore, nil)
	sess.SetKey(kp)
	sess.SetZenSink(func(refs []ZenRef) {
		batches = append(batches, refs)
	})
	if _, err := sess.RunInitiator(clientR, clientW); err != nil {
		t.Fatalf("RunInitiator: %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server: %v", err)
	}

	// The initiator's exchange delivers the server's (empty) ref list
	// as exactly one batch call.
	switch len(batches) {
	case 0:
		t.Fatal("zen sink never called")
	case 1:
		if len(batches[0]) != 0 {
			t.Fatalf("expected the server's empty offer, got %v", batches[0])
		}
	default:
		t.Fatalf("zen sink called %d times for one exchange, want 1", len(batches))
	}
}
