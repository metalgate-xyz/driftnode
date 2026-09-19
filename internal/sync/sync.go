// Package sync implements the event-log sync protocol between two zens
// (design 7.3). Synchronizing a log is a set-reconciliation problem: each
// side learns which signed events the other has that it does not, and
// transfers only those. The baseline mechanism is a timestamp cursor:
// "send me every event in log X after time T." Messages are framed as
// length-prefixed CBOR and sent over a byte stream (a tailcat tunnel
// between native zens, or a WebRTC data channel for browsers).
package sync

import (
	"encoding/binary"
	"fmt"
	"io"

	"driftnode/internal/core"
)

// MsgKind identifies a wire message.
type MsgKind uint8

const (
	MsgRequest MsgKind = 1
	MsgEvents  MsgKind = 2
	MsgDone    MsgKind = 3
	// MsgReverse signals role reversal: the pulling zen has finished
	// requesting and now offers to serve. The zen that received the
	// signal switches from serving to requesting. This makes a single
	// connection carry a bidirectional sync (section 7.3): the zen
	// that initiated the dial pulls first, then the listening zen pulls
	// back, without either side needing to dial separately.
	MsgReverse MsgKind = 4
	// MsgZens carries known zen tokens so zens can discover each
	// other beyond the initial bootstrap seeds (section 6.1). The
	// receiver learns these tokens and may dial them later.
	MsgZens MsgKind = 5
	// MsgHello is the first message of the session handshake: each side
	// announces its driftnode identity and a fresh nonce for the other to
	// sign. Sent by both sides before any sync traffic.
	MsgHello MsgKind = 6
	// MsgAuth carries the Ed25519 signature of the zen's nonce, proving
	// possession of the private key for the identity announced in Hello.
	MsgAuth MsgKind = 7
	// MsgFollowers requests the set of identities following the receiving
	// zen. The response reuses Events to carry the Follow{target: me}
	// events the followed zen has received, each self-certifying so the
	// requester can verify the follower's signature without trusting the
	// relaying zen. This is how the crawler walks the in-edge of the
	// follow graph (section 9.3) by asking the followed zen directly.
	MsgFollowers MsgKind = 8
)

// Message is the wire envelope. The Kind field determines which payload
// field is set. This single type lets the receiver decode the Kind before
// committing to a specific payload struct.
type Message struct {
	Kind    MsgKind  `cbor:"k"`
	Request *Request `cbor:"r,omitempty"`
	Events  *Events  `cbor:"e,omitempty"`
	Done    *Done    `cbor:"d,omitempty"`
	Zens    *Zens    `cbor:"p,omitempty"`
	Hello   *Hello   `cbor:"h,omitempty"`
	Auth    *Auth    `cbor:"a,omitempty"`
}

// Request asks the zen to send events in the named log after the given
// timestamp (unix nanoseconds). Author is optional: an empty author means
// "your own logs"; a non-empty author means that specific identity's log.
type Request struct {
	Log       core.LogName  `cbor:"l"`
	AfterTime int64         `cbor:"t"`
	Author    core.Identity `cbor:"a,omitempty"`
}

// Events carries a batch of signed events.
type Events struct {
	Items []core.SignedEvent `cbor:"e"`
}

// Done signals the end of a batch.
type Done struct{}

// Zens carries known zen tokens for zen exchange (section 6.1).
// Items carries the richer form with identity and name hints, so the
// receiver can display a discovered zen by name without dialing it. Tokens
// is retained for backward compatibility with peers that predate Items.
type Zens struct {
	Tokens []string `cbor:"t"`
	Items  []ZenRef `cbor:"i,omitempty"`
}

// ZenRef is a zen's routing token together with optional identity and name
// hints, exchanged during zen discovery so a receiver can show who a
// discovered zen is without connecting to it. The hints are unverified:
// the name is only authoritative once the crawler fetches that identity's
// signed ProfileLog.
type ZenRef struct {
	Token    string          `cbor:"t"`
	Identity core.Identity   `cbor:"i,omitempty"`
	Name     string          `cbor:"n,omitempty"`
}

// Hello is the first handshake message: the sender's driftnode identity and a
// fresh 32-byte nonce the receiver must sign to prove possession of the
// identity's private key.
type Hello struct {
	Identity core.Identity `cbor:"i"`
	Nonce    [32]byte      `cbor:"n"`
}

// Auth carries the Ed25519 signature over the zen's Hello nonce, proving the
// sender holds the private key for the identity it announced.
type Auth struct {
	Signature []byte `cbor:"s"`
}

// maxFrameSize is the upper bound on a single message size.
const maxFrameSize = 16 * 1024 * 1024

// WriteMsg encodes msg as canonical CBOR and writes it as a 4-byte
// big-endian length prefix followed by the payload.
func WriteMsg(w io.Writer, msg *Message) error {
	data, err := core.CanonicalEncode(msg)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if len(data) > maxFrameSize {
		return fmt.Errorf("message too large: %d bytes", len(data))
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	return nil
}

// ReadMsg reads a length-prefixed message and decodes it into a Message.
func ReadMsg(r io.Reader) (*Message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return nil, fmt.Errorf("empty message")
	}
	if n > maxFrameSize {
		return nil, fmt.Errorf("message too large: %d bytes", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	var msg Message
	if err := core.CanonicalDecode(buf, &msg); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &msg, nil
}

// NewRequest builds a Request message.
func NewRequest(log core.LogName, after int64, author core.Identity) *Message {
	return &Message{Kind: MsgRequest, Request: &Request{Log: log, AfterTime: after, Author: author}}
}

// NewEvents builds an Events message.
func NewEvents(items []core.SignedEvent) *Message {
	return &Message{Kind: MsgEvents, Events: &Events{Items: items}}
}

// NewDone builds a Done message.
func NewDone() *Message {
	return &Message{Kind: MsgDone, Done: &Done{}}
}

// NewReverse builds a Reverse message, signalling that the pulling zen has
// finished and now offers to serve.
func NewReverse() *Message {
	return &Message{Kind: MsgReverse}
}

// NewZens builds a Zens message carrying known zen refs. Tokens is populated
// for backward compatibility with peers that do not understand Items.
func NewZens(refs []ZenRef) *Message {
	tokens := make([]string, 0, len(refs))
	for _, r := range refs {
		tokens = append(tokens, r.Token)
	}
	return &Message{Kind: MsgZens, Zens: &Zens{Tokens: tokens, Items: refs}}
}

// NewHello builds a Hello message announcing an identity and a nonce for the
// zen to sign.
func NewHello(id core.Identity, nonce [32]byte) *Message {
	return &Message{Kind: MsgHello, Hello: &Hello{Identity: id, Nonce: nonce}}
}

// NewAuth builds an Auth message carrying a signature over the zen's nonce.
func NewAuth(sig []byte) *Message {
	return &Message{Kind: MsgAuth, Auth: &Auth{Signature: sig}}
}

// NewFollowers builds a Followers request: ask the receiving zen for its
// own follower set. The response is a sequence of MsgEvents carrying signed
// Follow events, terminated by MsgDone.
func NewFollowers() *Message {
	return &Message{Kind: MsgFollowers}
}
