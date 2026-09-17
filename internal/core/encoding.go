// Package core is the shared, platform-independent data model and crypto for
// the driftnode network: identity, the event-log model, canonical encoding, and
// signing. It has no networking and no storage backend. Phase 1 later compiles
// this package unmodified into the browser build.
package core

import (
	"crypto/ed25519"
	"encoding/base32"
	"fmt"
	"strings"

	"github.com/fxamacker/cbor/v2"
	"lukechampine.com/blake3"
)

// canonical is the deterministic CBOR encoding mode used for every signed
// event. Core Deterministic encoding (RFC 7049bis) sorts map keys bytewise and
// forbids indefinite-length items, so the same event produces identical bytes
// on any target. Because both the browser and native builds compile this same
// code, this is one implementation rather than a spec two must agree on.
var canonical cbor.EncMode

func init() {
	var err error
	canonical, err = cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		panic(fmt.Sprintf("core: init canonical cbor mode: %v", err))
	}
}

// CanonicalEncode returns the deterministic CBOR encoding of v.
func CanonicalEncode(v any) ([]byte, error) {
	return canonical.Marshal(v)
}

// CanonicalDecode decodes CBOR produced by CanonicalEncode into v.
func CanonicalDecode(b []byte, v any) error {
	return cbor.Unmarshal(b, v)
}

// HashSize is the length of a BLAKE3-256 digest in bytes.
const HashSize = 32

// EventID is the BLAKE3-256 of an event's canonical bytes (§7.2). References
// between events (Reply.parent_id, Like.target_id, Delete.target_id) point at
// this value; a reference is only resolvable once the referenced author's log
// has been synced.
type EventID [HashSize]byte

// String returns the lowercase base32 encoding of the ID, matching the
// identity-string encoding so the whole network shares one text form.
func (id EventID) String() string { return base32NoPadLower(id[:]) }

// IsZero reports whether the ID is the zero value.
func (id EventID) IsZero() bool { return id == EventID{} }

// ContentHash is the BLAKE3-256 of a media blob (§7.2). Attachments are
// content-addressed and fetched lazily, never inlined into events.
type ContentHash [HashSize]byte

// String returns the lowercase base32 encoding of the hash.
func (h ContentHash) String() string { return base32NoPadLower(h[:]) }

// IsZero reports whether the hash is the zero value.
func (h ContentHash) IsZero() bool { return h == ContentHash{} }

// EventIDOf returns the BLAKE3-256 of the given canonical bytes.
func EventIDOf(canon []byte) EventID { return EventID(blake3.Sum256(canon)) }

// ContentHashOf returns the BLAKE3-256 content hash of a media blob.
func ContentHashOf(b []byte) ContentHash { return ContentHash(blake3.Sum256(b)) }

// base32NoPadLower returns the lowercase, unpadded base32 encoding of b. It is
// used for identity strings, event IDs, and content hashes so they all share
// one text encoding.
func base32NoPadLower(b []byte) string {
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b))
}

// PubkeyFromBase32 decodes a lowercase base32 string back into a 32-byte
// public key. It accepts any-case input.
func PubkeyFromBase32(s string) (ed25519.PublicKey, error) {
	b, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(s))
	if err != nil {
		return nil, fmt.Errorf("base32 decode: %w", err)
	}
	if len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("pubkey: want %d bytes, got %d", ed25519.PublicKeySize, len(b))
	}
	return ed25519.PublicKey(b), nil
}
