package store

import (
	"crypto/ed25519"
	"errors"
	"testing"

	"driftnode/internal/core"
)

// signFollowWithoutWork signs a KindFollow event carrying no ground nonce.
// Sign refuses to produce this form, so the helper signs the canonical bytes
// directly, exactly as a misbehaving author would.
func signFollowWithoutWork(t *testing.T, author *core.KeyPair, seq uint64, ts int64, target [32]byte) *core.SignedEvent {
	t.Helper()
	ev := core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: ts,
		Sequence:  seq,
		Follow:    &core.Follow{TargetPubkey: target},
	}
	canon, err := core.CanonicalEncode(ev)
	if err != nil {
		t.Fatalf("canonical encode: %v", err)
	}
	return &core.SignedEvent{
		Event:     ev,
		Author:    author.Identity(),
		Signature: ed25519.Sign(author.Private, canon),
	}
}

// TestReceiverDropsWorklessFollow verifies that PutRemoteEvent drops a
// workless Follow and flags its author, like an oversized event.
func TestReceiverDropsWorklessFollow(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()
	target, _ := core.NewKeyPair()

	se := signFollowWithoutWork(t, author, 1, 5e9, [32]byte(target.Public))
	inserted, err := s.PutRemoteEvent(se, 1)
	if err != nil {
		t.Fatalf("PutRemoteEvent workless follow: %v", err)
	}
	if inserted {
		t.Fatal("workless follow was stored")
	}
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if !flagged {
		t.Fatal("author of workless follow was not flagged")
	}
}

// TestReceiverDropsWorklessFollowViaCrawl verifies PutCrawledProfile applies
// the same work check as PutRemoteEvent, so a workless Follow cannot enter
// the store through the crawl path either.
func TestReceiverDropsWorklessFollowViaCrawl(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()
	target, _ := core.NewKeyPair()

	se := signFollowWithoutWork(t, author, 1, 5e9, [32]byte(target.Public))
	if err := s.PutCrawledProfile(author.Identity(), se); err != nil {
		t.Fatalf("PutCrawledProfile workless follow: %v", err)
	}
	events, err := s.CrawledProfiles(author.Identity())
	if err != nil {
		t.Fatalf("CrawledProfiles: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("workless follow stored via crawl: %d events", len(events))
	}
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if !flagged {
		t.Fatal("author of workless crawled follow was not flagged")
	}
}

// TestReceiverStoresWorkedFollow verifies a Follow with valid work passes
// the check and is stored unflagged.
func TestReceiverStoresWorkedFollow(t *testing.T) {
	s := newTestStore(t)
	author, _ := core.NewKeyPair()
	target, _ := core.NewKeyPair()

	nonce, err := core.GrindFollowNonce([32]byte(author.Public), [32]byte(target.Public))
	if err != nil {
		t.Fatalf("grind nonce: %v", err)
	}
	se, err := author.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: 5e9,
		Sequence:  1,
		Follow:    &core.Follow{TargetPubkey: [32]byte(target.Public), Nonce: nonce},
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	inserted, err := s.PutRemoteEvent(se, 1)
	if err != nil {
		t.Fatalf("PutRemoteEvent: %v", err)
	}
	if !inserted {
		t.Fatal("worked follow was not stored")
	}
	flagged, err := s.IsFlagged(author.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if flagged {
		t.Fatal("author of a valid worked follow was flagged")
	}
}

// TestBackupImportDoesNotRecheckWork verifies a backup containing a workless
// Follow imports without error: import restores the account, it does not
// re-sign or re-check author-side work on events the account already signed.
func TestBackupImportDoesNotRecheckWork(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	workless := signFollowWithoutWork(t, kp, 1, 5e9, [32]byte{9})
	ek, err := core.DefaultKeyEncryption().Encrypt(kp.Private, []byte("pw"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	bd := &core.BackupData{
		Key:     ek,
		OwnLogs: []core.SignedEvent{*workless},
	}
	if err := s.ImportBackup(bd, "pw"); err != nil {
		t.Fatalf("ImportBackup with workless follow: %v", err)
	}
	events, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		t.Fatalf("OwnEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("imported own ProfileLog: want 1 event, got %d", len(events))
	}
}

// TestSignAndAppendGrindsNonce verifies the author-side funnel: SignAndAppend
// grinds a valid nonce before signing, so every Follow the client emits
// carries work, and Sign still refuses a manually supplied workless Follow.
func TestSignAndAppendGrindsNonce(t *testing.T) {
	s := newTestStore(t)
	kp, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	if err := s.InitIdentity(kp, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	target, _ := core.NewKeyPair()
	se, _, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: [32]byte(target.Public)},
	})
	if err != nil {
		t.Fatalf("SignAndAppend follow: %v", err)
	}
	if se.Event.Follow.Nonce == 0 {
		t.Fatal("SignAndAppend emitted a follow with zero nonce")
	}
	if !hasRequiredFollowWork(se) {
		t.Fatal("SignAndAppend emitted a follow without valid work")
	}
	// A manually supplied zero nonce is refused by Sign itself.
	_, err = kp.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: 5e9,
		Sequence:  1,
		Follow:    &core.Follow{TargetPubkey: [32]byte(target.Public)},
	})
	if !errors.Is(err, core.ErrFollowWork) {
		t.Fatalf("Sign workless follow: want ErrFollowWork, got %v", err)
	}
}
