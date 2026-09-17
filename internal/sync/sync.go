// Package sync implements the event-log sync protocol between two peers
// (design 7.3). Synchronizing a log is a set-reconciliation problem: each
// side learns which signed events the other has that it does not, and
// transfers only those. The baseline mechanism is a timestamp cursor:
// "send me every event in log X after time T." Messages are framed as
// length-prefixed CBOR and sent over a byte stream (a tailcat tunnel
// between native peers, or a WebRTC data channel for browsers).
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
	// MsgReverse signals role reversal: the pulling peer has finished
	// requesting and now offers to serve. The peer that received the
	// signal switches from serving to requesting. This makes a single
	// connection carry a bidirectional sync (section 7.3): the peer
	// that initiated the dial pulls first, then the listening peer pulls
	// back, without either side needing to dial separately.
	MsgReverse MsgKind = 4
	// MsgPeers carries known peer tokens so peers can discover each
	// other beyond the initial bootstrap seeds (section 6.1). The
	// receiver learns these tokens and may dial them later.
	MsgPeers MsgKind = 5
)

// Message is the wire envelope. The Kind field determines which payload
// field is set. This single type lets the receiver decode the Kind before
// committing to a specific payload struct.
type Message struct {
	Kind    MsgKind            `cbor:"k"`
	Request *Request           `cbor:"r,omitempty"`
	Events  *Events            `cbor:"e,omitempty"`
	Done    *Done              `cbor:"d,omitempty"`
	Peers   *Peers             `cbor:"p,omitempty"`
}

// Request asks the peer to send events in the named log after the given
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

// Peers carries known peer tokens for peer exchange (section 6.1).
type Peers struct {
	Tokens []string `cbor:"t"`
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

// NewReverse builds a Reverse message, signalling that the pulling peer has
// finished and now offers to serve.
func NewReverse() *Message {
	return &Message{Kind: MsgReverse}
}

// NewPeers builds a Peers message carrying known peer tokens.
func NewPeers(tokens []string) *Message {
	return &Message{Kind: MsgPeers, Peers: &Peers{Tokens: tokens}}
}
