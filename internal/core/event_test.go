package core

import (
	"bytes"
	"crypto/ed25519"
	"testing"
)

func TestSignAndVerify(t *testing.T) {
	kp, err := NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	ev := Event{
		Kind:      KindPost,
		Log:       PostLog,
		Timestamp: Now64(),
		Sequence:  1,
		Post:      &Post{Text: "gm"},
	}
	se, err := kp.Sign(ev)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if err := se.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if se.Author != kp.Identity() {
		t.Fatal("author mismatch")
	}
}

// Tampering with any signed field must break verification, since the signature
// covers the canonical encoding of the event body.
func TestVerifyRejectsTampering(t *testing.T) {
	kp, _ := NewKeyPair()
	ev := Event{
		Kind:      KindPost,
		Log:       PostLog,
		Timestamp: Now64(),
		Sequence:  1,
		Post:      &Post{Text: "original"},
	}
	se, err := kp.Sign(ev)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	se.Event.Post.Text = "tampered"
	if err := se.Verify(); err == nil {
		t.Fatal("Verify accepted tampered event")
	}
}

// A signature from one key must not verify under another key's identity.
func TestVerifyRejectsWrongAuthor(t *testing.T) {
	alice, _ := NewKeyPair()
	bob, _ := NewKeyPair()
	ev := Event{
		Kind:      KindPost,
		Log:       PostLog,
		Timestamp: Now64(),
		Sequence:  1,
		Post:      &Post{Text: "forged"},
	}
	se, err := alice.Sign(ev)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	// Claim it was signed by bob.
	se.Author = bob.Identity()
	if err := se.Verify(); err == nil {
		t.Fatal("Verify accepted event with wrong author")
	}
}

func TestValidateEventLogConsistency(t *testing.T) {
	cases := []struct {
		name    string
		ev      Event
		wantErr bool
	}{
		{"post in postlog", Event{Kind: KindPost, Log: PostLog, Timestamp: 1, Sequence: 1, Post: &Post{}}, false},
		{"profile in profilelog", Event{Kind: KindProfile, Log: ProfileLog, Timestamp: 1, Sequence: 1, Profile: &Profile{}}, false},
		{"follow in profilelog", Event{Kind: KindFollow, Log: ProfileLog, Timestamp: 1, Sequence: 1, Follow: &Follow{}}, false},
		{"post in profilelog", Event{Kind: KindPost, Log: ProfileLog, Timestamp: 1, Sequence: 1, Post: &Post{}}, true},
		{"profile in postlog", Event{Kind: KindProfile, Log: PostLog, Timestamp: 1, Sequence: 1, Profile: &Profile{}}, true},
		{"detail in detaillog", Event{Kind: KindDetail, Log: DetailLog, Timestamp: 1, Sequence: 1, Detail: &Detail{}}, false},
		{"detail in profilelog", Event{Kind: KindDetail, Log: ProfileLog, Timestamp: 1, Sequence: 1, Detail: &Detail{}}, true},
		{"profile in detaillog", Event{Kind: KindProfile, Log: DetailLog, Timestamp: 1, Sequence: 1, Profile: &Profile{}}, true},
		{"like in postlog", Event{Kind: KindLike, Log: PostLog, Timestamp: 1, Sequence: 1, Like: &Like{TargetID: EventID{1}}}, false},
		{"like in profilelog", Event{Kind: KindLike, Log: ProfileLog, Timestamp: 1, Sequence: 1, Like: &Like{TargetID: EventID{1}}}, true},
		{"delete in postlog", Event{Kind: KindDelete, Log: PostLog, Timestamp: 1, Sequence: 1, Delete: &Delete{TargetID: EventID{1}}}, false},
		{"delete in profilelog", Event{Kind: KindDelete, Log: ProfileLog, Timestamp: 1, Sequence: 1, Delete: &Delete{TargetID: EventID{1}}}, false},
		{"delete in detaillog", Event{Kind: KindDelete, Log: DetailLog, Timestamp: 1, Sequence: 1, Delete: &Delete{TargetID: EventID{1}}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateEvent(c.ev)
			if (err != nil) != c.wantErr {
				t.Fatalf("validateEvent: got err=%v, wantErr=%v", err, c.wantErr)
			}
		})
	}
}

func TestValidateEventMissingFields(t *testing.T) {
	if err := validateEvent(Event{Log: PostLog, Timestamp: 1, Sequence: 1}); err == nil {
		t.Fatal("expected error for missing kind")
	}
	if err := validateEvent(Event{Kind: KindPost, Log: PostLog, Sequence: 1}); err == nil {
		t.Fatal("expected error for missing timestamp")
	}
}

// A SignedEvent must round-trip through canonical CBOR, since it is what zens
// exchange and what the store persists.
func TestSignedEventSerialization(t *testing.T) {
	kp, _ := NewKeyPair()
	ev := Event{
		Kind:      KindPost,
		Log:       PostLog,
		Timestamp: Now64(),
		Sequence:  1,
		Post:      &Post{Text: "serialize me"},
	}
	se, err := kp.Sign(ev)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	b, err := CanonicalEncode(se)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var se2 SignedEvent
	if err := CanonicalDecode(b, &se2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := se2.Verify(); err != nil {
		t.Fatalf("deserialized event does not verify: %v", err)
	}
	if se2.Author != se.Author {
		t.Fatal("author mismatch after round-trip")
	}
	if !bytes.Equal(se2.Signature, se.Signature) {
		t.Fatal("signature mismatch after round-trip")
	}
}

func TestEventIDConsistentAfterSerialization(t *testing.T) {
	kp, _ := NewKeyPair()
	se, _ := kp.Sign(Event{
		Kind: KindPost, Log: PostLog, Timestamp: 5, Sequence: 1,
		Post: &Post{Text: "id test"},
	})
	idBefore, _ := se.ID()
	b, _ := CanonicalEncode(se)
	var se2 SignedEvent
	_ = CanonicalDecode(b, &se2)
	idAfter, _ := se2.ID()
	if idBefore != idAfter {
		t.Fatal("event ID changed across serialization")
	}
}

func TestReplyEvent(t *testing.T) {
	kp, _ := NewKeyPair()
	se, err := kp.Sign(Event{
		Kind: KindReply, Log: PostLog, Timestamp: Now64(), Sequence: 1,
		Reply: &Reply{Post: Post{Text: "a reply"}, ParentID: EventID{0xAB}},
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if err := se.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestFollowEvent(t *testing.T) {
	alice, _ := NewKeyPair()
	bob, _ := NewKeyPair()
	se, err := alice.Sign(Event{
		Kind: KindFollow, Log: ProfileLog, Timestamp: Now64(), Sequence: 1,
		Follow: &Follow{TargetPubkey: [32]byte(bob.Public)},
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if err := se.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !bytes.Equal(se.Event.Follow.TargetPubkey[:], bob.Public) {
		t.Fatal("follow target mismatch")
	}
}

func TestEd25519Constant(t *testing.T) {
	if ed25519.SignatureSize != 64 {
		t.Fatal("unexpected signature size")
	}
}
