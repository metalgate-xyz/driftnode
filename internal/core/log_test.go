package core

import (
	"testing"
)

func TestProfileProjectionLastWriteWins(t *testing.T) {
	kp, err := NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	sign := func(ts int64, seq uint64, name string) SignedEvent {
		se, err := kp.Sign(Event{
			Kind:      KindProfile,
			Log:       ProfileLog,
			Timestamp: ts,
			Sequence:  seq,
			Profile:   &Profile{DisplayName: name},
		})
		if err != nil {
			t.Fatalf("Sign: %v", err)
		}
		return *se
	}
	events := []SignedEvent{
		sign(100, 1, "old"),
		sign(200, 2, "new"),
		sign(150, 3, "middle"),
	}
	prof := NewLog(events).Profile()
	if prof == nil {
		t.Fatal("Profile is nil")
	}
	if prof.DisplayName != "new" {
		t.Fatalf("last-write-wins: want %q, got %q", "new", prof.DisplayName)
	}
}

func TestDetailProjectionLastWriteWins(t *testing.T) {
	kp, err := NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	sign := func(ts int64, seq uint64, d *Detail) SignedEvent {
		se, err := kp.Sign(Event{
			Kind:      KindDetail,
			Log:       DetailLog,
			Timestamp: ts,
			Sequence:  seq,
			Detail:    d,
		})
		if err != nil {
			t.Fatalf("Sign: %v", err)
		}
		return *se
	}
	events := []SignedEvent{
		sign(100, 1, &Detail{Bio: "old bio", Location: "Paris"}),
		sign(200, 2, &Detail{FirstName: "Alice", Location: "Oxford"}),
	}
	d := NewLog(events).Detail()
	if d == nil {
		t.Fatal("Detail is nil")
	}
	// Last-write-wins: the ts=200 event wins on every field it sets.
	if d.FirstName != "Alice" {
		t.Fatalf("first name: want %q, got %q", "Alice", d.FirstName)
	}
	if d.Location != "Oxford" {
		t.Fatalf("location: want %q, got %q", "Oxford", d.Location)
	}
	// Bio is only set by the older event; the newer event omits it (omitempty),
	// so it does not overwrite. The projection returns the latest event as-is.
	if d.Bio != "" {
		t.Fatalf("bio from superseded event should not survive: got %q", d.Bio)
	}
}

func TestDetailProjectionEmpty(t *testing.T) {
	events := []SignedEvent{}
	d := NewLog(events).Detail()
	if d != nil {
		t.Fatalf("empty log: want nil, got %+v", d)
	}
}
