package core

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"
)

// LogName identifies which of an identity's two logs an event belongs to
// (§7.1). The two logs are independently requestable so that discovering
// someone never requires downloading their content history.
type LogName string

const (
	// ProfileLog holds Profile (last-write-wins by timestamp), Follow and
	// Unfollow events: small, low-churn, and what the crawler fetches.
	ProfileLog LogName = "profile"
	// PostLog holds Post, Reply, Like and Delete events: the content stream,
	// potentially large and long-lived, synced only by followers.
	PostLog LogName = "post"
)

// Kind is the type of an event, encoded as a small integer in the canonical
// form.
type Kind uint8

const (
	KindProfile  Kind = 1
	KindFollow   Kind = 2
	KindUnfollow Kind = 3
	KindPost     Kind = 4
	KindReply    Kind = 5
	KindLike     Kind = 6
	KindDelete   Kind = 7
)

// Event is an unsigned event. Its canonical CBOR encoding is what the signature
// covers and what the event ID hashes. Field order and cbor tag numbers are
// fixed: reordering fields or renumbering tags changes every event ID.
type Event struct {
	Kind      Kind     `cbor:"k"`
	Log       LogName  `cbor:"l"`
	Timestamp int64    `cbor:"t"` // unix nanoseconds
	Sequence  uint64   `cbor:"s"` // per-author per-log monotonic counter
	Profile   *Profile `cbor:"p,omitempty"`
	Follow    *Follow  `cbor:"f,omitempty"`
	Post      *Post    `cbor:"P,omitempty"`
	Reply     *Reply   `cbor:"R,omitempty"`
	Like      *Like    `cbor:"L,omitempty"`
	Delete    *Delete  `cbor:"D,omitempty"`
}

// Profile is an upsert into the author's current profile (last-write-wins by
// timestamp).
type Profile struct {
	DisplayName string       `cbor:"d,omitempty"`
	Bio         string       `cbor:"b,omitempty"`
	AvatarHash  *ContentHash `cbor:"a,omitempty"`
}

// Follow adds an edge to target; Unfollow removes it. Both live in the
// Profile log because they are metadata about the author's own graph, not
// content (§9.4).
type Follow struct {
	TargetPubkey [32]byte `cbor:"t"` // raw public key of the followed identity
}

// Post is a top-level content event in the PostLog.
type Post struct {
	Text        string        `cbor:"t,omitempty"`
	MediaHashes []ContentHash `cbor:"m,omitempty"`
}

// Reply is a Post that references a parent event by ID. The parent is only
// resolvable once the referenced author's PostLog has been synced (§7.2).
type Reply struct {
	Post
	ParentID EventID `cbor:"p"`
}

// Like is recorded in the liker's own PostLog, never the target's: nobody can
// write into someone else's log without a valid signature from that
// identity's key, so a like count is always "events this zen has observed,"
// never a global truth (§7.2).
type Like struct {
	TargetID EventID `cbor:"t"`
}

// Delete is a tombstone, not erasure: zens who already replicated the
// original content before the delete propagated may still hold a copy (§7.2).
type Delete struct {
	TargetID EventID `cbor:"t"`
}

// SignedEvent is an Event together with its author identity and signature.
// The canonical bytes for hashing and verifying are the canonical encoding of
// the embedded Event; the author and signature are carried alongside, not
// inside, those bytes.
type SignedEvent struct {
	Event     Event    `cbor:"e"`
	Author    Identity `cbor:"a"`
	Signature []byte   `cbor:"s"`
}

// Sign produces a SignedEvent by signing the canonical encoding of ev with
// the author's private key.
func (k *KeyPair) Sign(ev Event) (*SignedEvent, error) {
	if err := validateEvent(ev); err != nil {
		return nil, err
	}
	canon, err := CanonicalEncode(ev)
	if err != nil {
		return nil, fmt.Errorf("canonical encode: %w", err)
	}
	sig := ed25519.Sign(k.Private, canon)
	return &SignedEvent{
		Event:     ev,
		Author:    k.Identity(),
		Signature: sig,
	}, nil
}

// Verify checks that the signature was produced by the author's private key
// over the event's canonical bytes. A verified event is authentic and belongs
// to the claimed identity's log, regardless of which zen relayed it (§7).
func (e *SignedEvent) Verify() error {
	if e == nil {
		return errors.New("nil event")
	}
	pub, err := e.Author.PubkeyBytes()
	if err != nil {
		return fmt.Errorf("author: %w", err)
	}
	if len(e.Signature) != ed25519.SignatureSize {
		return fmt.Errorf("signature: want %d bytes, got %d", ed25519.SignatureSize, len(e.Signature))
	}
	canon, err := CanonicalEncode(e.Event)
	if err != nil {
		return fmt.Errorf("canonical encode: %w", err)
	}
	if !ed25519.Verify(pub, canon, e.Signature) {
		return errors.New("signature does not verify")
	}
	return validateEvent(e.Event)
}

// ID returns the BLAKE3-256 of the event's canonical bytes.
func (e *SignedEvent) ID() (EventID, error) {
	canon, err := CanonicalEncode(e.Event)
	if err != nil {
		return EventID{}, fmt.Errorf("canonical encode: %w", err)
	}
	return EventIDOf(canon), nil
}

// validateEvent enforces that an event's Kind is consistent with the log it
// claims to live in, so a zen can reject a mis-logged event before accepting
// it into a store.
func validateEvent(ev Event) error {
	if ev.Kind == 0 {
		return errors.New("event kind must be set")
	}
	if ev.Timestamp == 0 {
		return errors.New("event timestamp must be set")
	}
	switch ev.Log {
	case ProfileLog:
		switch ev.Kind {
		case KindProfile, KindFollow, KindUnfollow, KindDelete:
			return nil
		}
	case PostLog:
		switch ev.Kind {
		case KindPost, KindReply, KindLike, KindDelete:
			return nil
		}
	}
	return fmt.Errorf("event kind %d not valid for log %q", ev.Kind, ev.Log)
}

// Now64 returns the current time as unix nanoseconds, the timestamp unit used
// throughout the data model.
func Now64() int64 { return time.Now().UnixNano() }
