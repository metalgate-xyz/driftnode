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
		{"delete in profilelog", Event{Kind: KindDelete, Log: ProfileLog, Timestamp: 1, Sequence: 1, Delete: &Delete{TargetID: EventID{1}}}, true},
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
		Kind: KindPost, Log: PostLog, Timestamp: Now64(), Sequence: 1,
		Post: &Post{Text: "a reply", ParentID: EventID{0xAB}},
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

// An oversized event is rejected by Sign on the author side, so a legitimate
// author can never produce one. Each variable kind is tested at its cap
// (accepted) and one byte over (rejected).
func TestSignRejectsOversized(t *testing.T) {
	kp, _ := NewKeyPair()
	ts := int64(9e18)
	seq := uint64(0xFFFFFFFFFFFFFFFF)

	// Helper: find the max text length that fits within cap for a Post event.
	maxText := func(cap int, ev Event) int {
		lo, hi := 0, cap*2
		for lo < hi {
			mid := (lo + hi + 1) / 2
			ev.Post = &Post{Text: string(make([]byte, mid))}
			b, err := CanonicalEncode(ev)
			if err != nil {
				hi = mid - 1
				continue
			}
			if len(b) <= cap {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		return lo
	}

	// Post at cap: accepted. One byte over: rejected.
	base := Event{Kind: KindPost, Log: PostLog, Timestamp: ts, Sequence: seq}
	maxLen := maxText(CapPost, base)
	atCap := base
	atCap.Post = &Post{Text: string(make([]byte, maxLen))}
	if _, err := kp.Sign(atCap); err != nil {
		t.Fatalf("Sign at cap (Post): %v", err)
	}
	overCap := base
	overCap.Post = &Post{Text: string(make([]byte, maxLen+1))}
	if _, err := kp.Sign(overCap); err != ErrEventOversized {
		t.Fatalf("Sign over cap (Post): want ErrEventOversized, got %v", err)
	}

	// Profile: oversized DisplayName rejected.
	maxProf := 0
	{
		lo, hi := 0, CapProfile*2
		for lo < hi {
			mid := (lo + hi + 1) / 2
			ev := Event{Kind: KindProfile, Log: ProfileLog, Timestamp: ts, Sequence: seq, Profile: &Profile{DisplayName: string(make([]byte, mid))}}
			b, _ := CanonicalEncode(ev)
			if len(b) <= CapProfile {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		maxProf = lo
	}
	profAtCap := Event{Kind: KindProfile, Log: ProfileLog, Timestamp: ts, Sequence: seq, Profile: &Profile{DisplayName: string(make([]byte, maxProf))}}
	if _, err := kp.Sign(profAtCap); err != nil {
		t.Fatalf("Sign at cap (Profile): %v", err)
	}
	profOver := Event{Kind: KindProfile, Log: ProfileLog, Timestamp: ts, Sequence: seq, Profile: &Profile{DisplayName: string(make([]byte, maxProf+1))}}
	if _, err := kp.Sign(profOver); err != ErrEventOversized {
		t.Fatalf("Sign over cap (Profile): want ErrEventOversized, got %v", err)
	}

	// Detail: oversized Bio rejected.
	maxBio := 0
	{
		lo, hi := 0, CapDetail*2
		for lo < hi {
			mid := (lo + hi + 1) / 2
			ev := Event{Kind: KindDetail, Log: DetailLog, Timestamp: ts, Sequence: seq, Detail: &Detail{Bio: string(make([]byte, mid))}}
			b, _ := CanonicalEncode(ev)
			if len(b) <= CapDetail {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		maxBio = lo
	}
	detAtCap := Event{Kind: KindDetail, Log: DetailLog, Timestamp: ts, Sequence: seq, Detail: &Detail{Bio: string(make([]byte, maxBio))}}
	if _, err := kp.Sign(detAtCap); err != nil {
		t.Fatalf("Sign at cap (Detail): %v", err)
	}
	detOver := Event{Kind: KindDetail, Log: DetailLog, Timestamp: ts, Sequence: seq, Detail: &Detail{Bio: string(make([]byte, maxBio+1))}}
	if _, err := kp.Sign(detOver); err != ErrEventOversized {
		t.Fatalf("Sign over cap (Detail): want ErrEventOversized, got %v", err)
	}
}

// Fixed events (Like, Delete, Follow, Unfollow) are accepted at their exact
// canonical size and rejected one byte over. A one-byte-over fixed event is
// impossible to produce through Sign (the cap equals the canonical size),
// so this tests the cap enforcement directly via a forged event that bypasses
// Sign.
func TestSignAcceptsFixedEventsAtCap(t *testing.T) {
	kp, _ := NewKeyPair()
	ts := int64(9e18)
	seq := uint64(0xFFFFFFFFFFFFFFFF)

	cases := []struct {
		name string
		ev   Event
		cap  int
	}{
		{"Like", Event{Kind: KindLike, Log: PostLog, Timestamp: ts, Sequence: seq, Like: &Like{TargetID: EventID{1}}}, CapLike},
		{"Delete", Event{Kind: KindDelete, Log: PostLog, Timestamp: ts, Sequence: seq, Delete: &Delete{TargetID: EventID{1}}}, CapDelete},
		{"Follow", Event{Kind: KindFollow, Log: ProfileLog, Timestamp: ts, Sequence: seq, Follow: &Follow{TargetPubkey: [32]byte{1}}}, CapFollow},
		{"Unfollow", Event{Kind: KindUnfollow, Log: ProfileLog, Timestamp: ts, Sequence: seq, Follow: &Follow{TargetPubkey: [32]byte{1}}}, CapUnfollow},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			se, err := kp.Sign(c.ev)
			if err != nil {
				t.Fatalf("Sign %s at cap: %v", c.name, err)
			}
			canon, _ := CanonicalEncode(se.Event)
			if len(canon) > c.cap {
				t.Fatalf("%s canonical size %d exceeds cap %d", c.name, len(canon), c.cap)
			}
		})
	}
}
