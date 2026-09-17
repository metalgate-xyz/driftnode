package core

import (
	"bytes"
	"crypto/ed25519"
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

// The canonical form is whatever the shared function produces, and two
// encodings of the same event must be byte-identical so that event IDs and
// signatures are stable across peers and across the two build targets (§7.2).
func TestCanonicalEncodingDeterministic(t *testing.T) {
	ev := Event{
		Kind:      KindPost,
		Log:       PostLog,
		Timestamp: 1_700_000_000_000_000_000,
		Sequence:  7,
		Post:      &Post{Text: "gm from the field test"},
	}
	a, err := CanonicalEncode(ev)
	if err != nil {
		t.Fatalf("encode a: %v", err)
	}
	b, err := CanonicalEncode(ev)
	if err != nil {
		t.Fatalf("encode b: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("canonical encoding is not deterministic")
	}
}

// Omitted optional fields must not appear in the wire form, so two events of
// the same kind with different payloads do not encode the absent fields as
// null.
func TestCanonicalEncodingOmitsEmptyFields(t *testing.T) {
	ev := Event{
		Kind:      KindPost,
		Log:       PostLog,
		Timestamp: 1,
		Sequence:  1,
		Post:      &Post{Text: "x"},
	}
	b, err := CanonicalEncode(ev)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	diag, err := cbor.Diagnose(b)
	if err != nil {
		t.Fatalf("diagnose: %v", err)
	}
	// A Post event must carry the post field ("P") but not the profile ("p"),
	// follow ("f"), reply ("R"), like ("L"), or delete ("D") fields.
	for _, absent := range []string{`"p"`, `"f"`, `"R"`, `"L"`, `"D"`} {
		if strings.Contains(diag, absent) {
			t.Fatalf("diagnostic %q contains absent field %s", diag, absent)
		}
	}
	if !strings.Contains(diag, `"P"`) {
		t.Fatalf("diagnostic %q missing post field", diag)
	}
}

func TestCanonicalEncodingRoundTrip(t *testing.T) {
	ev := Event{
		Kind:      KindProfile,
		Log:       ProfileLog,
		Timestamp: 42,
		Sequence:  1,
		Profile:   &Profile{DisplayName: "alice", Bio: "field tester"},
	}
	b, err := CanonicalEncode(ev)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got Event
	if err := CanonicalDecode(b, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Kind != ev.Kind || got.Log != ev.Log || got.Timestamp != ev.Timestamp {
		t.Fatalf("round-trip mismatch: got %+v", got)
	}
	if got.Profile == nil || got.Profile.DisplayName != "alice" {
		t.Fatalf("profile not round-tripped: %+v", got.Profile)
	}
}

// Event IDs are BLAKE3-256 of the canonical bytes. The same event must
// produce the same ID regardless of where it is computed.
func TestEventIDStable(t *testing.T) {
	ev := Event{
		Kind:      KindPost,
		Log:       PostLog,
		Timestamp: 100,
		Sequence:  1,
		Post:      &Post{Text: "stable id"},
	}
	se := &SignedEvent{Event: ev, Author: "driftnode:x", Signature: bytes.Repeat([]byte{0}, ed25519.SignatureSize)}
	id1, err := se.ID()
	if err != nil {
		t.Fatalf("ID: %v", err)
	}
	id2, err := se.ID()
	if err != nil {
		t.Fatalf("ID: %v", err)
	}
	if id1 != id2 {
		t.Fatal("event ID not stable")
	}
}

func TestEventIDChangesOnContentChange(t *testing.T) {
	base := Event{
		Kind: KindPost, Log: PostLog, Timestamp: 100, Sequence: 1,
		Post: &Post{Text: "a"},
	}
	se1 := &SignedEvent{Event: base, Author: "driftnode:x", Signature: bytes.Repeat([]byte{0}, ed25519.SignatureSize)}
	id1, _ := se1.ID()
	se2 := &SignedEvent{Event: base, Author: "driftnode:x", Signature: bytes.Repeat([]byte{0}, ed25519.SignatureSize)}
	se2.Event.Post.Text = "b"
	id2, _ := se2.ID()
	if id1 == id2 {
		t.Fatal("event ID did not change when content changed")
	}
}

func TestContentHash(t *testing.T) {
	h := ContentHashOf([]byte("media blob"))
	if h.IsZero() {
		t.Fatal("content hash is zero")
	}
	// Different content, different hash.
	h2 := ContentHashOf([]byte("different blob"))
	if h == h2 {
		t.Fatal("distinct content produced identical hash")
	}
}

func TestEventIDStringEncoding(t *testing.T) {
	id := EventID{1, 2, 3}
	s := id.String()
	if s == "" || strings.ContainsAny(s, "=") {
		t.Fatalf("bad event id string %q", s)
	}
}
