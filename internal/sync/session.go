package sync

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"driftnode/internal/core"
	"driftnode/internal/store"
)

// Server handles incoming sync requests from a zen. It reads requests and
// responds with events from the local store.
type Server struct {
	store *store.Store
	log   *slog.Logger
}

// NewServer creates a sync server backed by the given store.
func NewServer(s *store.Store, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{store: s, log: log}
}

// Serve handles one sync connection. It reads messages from r and writes
// responses to w until the zen sends Done or the connection closes.
func (s *Server) Serve(r io.Reader, w io.Writer) error {
	for {
		msg, err := ReadMsg(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("read msg: %w", err)
		}
		switch msg.Kind {
		case MsgRequest:
			if err := s.handleRequest(msg.Request, w); err != nil {
				return err
			}
		case MsgFollowers:
			if err := s.handleFollowers(w); err != nil {
				return err
			}
		case MsgDone:
			return nil
		default:
			s.log.Warn("sync: unknown message kind", "kind", msg.Kind)
		}
	}
}

// handleRequest responds to a request by sending events from the local
// store after the requested timestamp, followed by Done.
func (s *Server) handleRequest(req *Request, w io.Writer) error {
	if req == nil {
		return fmt.Errorf("request message missing payload")
	}
	events, err := s.fetchEvents(req)
	if err != nil {
		return err
	}
	const batchSize = 64
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}
		if err := WriteMsg(w, NewEvents(events[i:end])); err != nil {
			return fmt.Errorf("write events: %w", err)
		}
	}
	return WriteMsg(w, NewDone())
}

// handleFollowers responds to a MsgFollowers request by sending the signed
// Follow events the local identity has received targeting itself, followed
// by Done. Each event is self-certifying: the requester verifies the
// follower's signature, so the relaying zen cannot fabricate followers.
func (s *Server) handleFollowers(w io.Writer) error {
	events, err := s.store.ReceivedFollowEvents()
	if err != nil {
		return fmt.Errorf("fetch followers: %w", err)
	}
	const batchSize = 64
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}
		if err := WriteMsg(w, NewEvents(events[i:end])); err != nil {
			return fmt.Errorf("write followers: %w", err)
		}
	}
	return WriteMsg(w, NewDone())
}

// fetchEvents retrieves events from the store matching the request. If the
// author is the local user, their own events are returned; otherwise, cached
// events for that author (followed or crawled) are returned.
func (s *Server) fetchEvents(req *Request) ([]core.SignedEvent, error) {
	if req.Author == "" {
		return s.fetchOwnEvents(req)
	}
	// Check if the requested author is the local identity.
	ownID, ok, err := s.store.Identity()
	if err != nil {
		return nil, err
	}
	if ok && req.Author == ownID {
		return s.fetchOwnEvents(req)
	}
	// Return cached events for this author (followed or crawled).
	cached, err := s.store.CrawledEvents(req.Author)
	if err != nil {
		return nil, err
	}
	if len(cached) == 0 {
		crawled, err := s.store.CrawledProfiles(req.Author)
		if err != nil {
			return nil, err
		}
		cached = crawled
	}
	return s.filterAfterTime(cached, req.AfterTime), nil
}

// filterAfterTime returns events whose timestamp is after the given time,
// sorted by timestamp then sequence.
func (s *Server) filterAfterTime(events []core.SignedEvent, after int64) []core.SignedEvent {
	log := core.NewLog(events)
	sorted := log.SortedByTime()
	var out []core.SignedEvent
	for _, se := range sorted {
		if se.Event.Timestamp > after {
			out = append(out, se)
		}
	}
	return out
}

// fetchOwnEvents returns the local user's events in the requested log after
// the given timestamp, sorted by time then sequence.
func (s *Server) fetchOwnEvents(req *Request) ([]core.SignedEvent, error) {
	all, err := s.store.OwnEvents(req.Log)
	if err != nil {
		return nil, err
	}
	log := core.NewLog(all)
	sorted := log.SortedByTime()
	var out []core.SignedEvent
	for _, se := range sorted {
		if se.Event.Timestamp > req.AfterTime {
			out = append(out, se)
		}
	}
	return out, nil
}

// Client drives a sync session: it sends requests and receives events,
// merging them into the local store.
type Client struct {
	store *store.Store
	log   *slog.Logger
}

// NewClient creates a sync client backed by the given store.
func NewClient(s *store.Store, log *slog.Logger) *Client {
	if log == nil {
		log = slog.Default()
	}
	return &Client{store: s, log: log}
}

// SyncLog requests events in the given log after the last-synced timestamp
// and merges the received events into the local store. For the user's own
// logs, author is empty; for a followed identity's PostLog, author is that
// identity.
func (c *Client) SyncLog(r io.Reader, w io.Writer, log core.LogName, after int64, author core.Identity) (int, error) {
	events, err := c.SyncLogRaw(r, w, log, after, author)
	if err != nil {
		return 0, err
	}
	merged := 0
	for i := range events {
		inserted, err := c.mergeEvent(&events[i])
		if err != nil {
			c.log.Warn("sync: merge failed", "err", err)
			continue
		}
		if inserted {
			merged++
		}
	}
	return merged, nil
}

// SyncLogRaw requests events in the given log after the given timestamp and
// returns them without merging into the store. Used by the crawler, which
// stores profile-log events as crawled profiles rather than followed events.
func (c *Client) SyncLogRaw(r io.Reader, w io.Writer, log core.LogName, after int64, author core.Identity) ([]core.SignedEvent, error) {
	if err := WriteMsg(w, NewRequest(log, after, author)); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}
	var out []core.SignedEvent
	for {
		msg, err := ReadMsg(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return out, nil
			}
			return out, fmt.Errorf("read msg: %w", err)
		}
		switch msg.Kind {
		case MsgEvents:
			if msg.Events == nil {
				continue
			}
			for _, se := range msg.Events.Items {
				if err := se.Verify(); err != nil {
					c.log.Warn("sync: rejected unverified event", "err", err)
					continue
				}
				out = append(out, se)
			}
		case MsgDone:
			return out, nil
		default:
			c.log.Warn("sync: unknown message kind", "kind", msg.Kind)
		}
	}
}

// FetchFollowers requests the receiving zen's own follower set and returns
// the signed Follow events it has received targeting itself. Each event is
// verified, so the requester can trust the follower's identity without
// trusting the relaying zen. Used by the crawler to walk the in-edge of the
// follow graph by asking the followed zen directly (section 9.3).
func (c *Client) FetchFollowers(r io.Reader, w io.Writer) ([]core.SignedEvent, error) {
	if err := WriteMsg(w, NewFollowers()); err != nil {
		return nil, fmt.Errorf("write followers request: %w", err)
	}
	var out []core.SignedEvent
	for {
		msg, err := ReadMsg(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return out, nil
			}
			return out, fmt.Errorf("read msg: %w", err)
		}
		switch msg.Kind {
		case MsgEvents:
			if msg.Events == nil {
				continue
			}
			for _, se := range msg.Events.Items {
				if err := se.Verify(); err != nil {
					c.log.Warn("followers: rejected unverified event", "err", err)
					continue
				}
				out = append(out, se)
			}
		case MsgDone:
			return out, nil
		default:
			c.log.Warn("followers: unexpected message kind", "kind", msg.Kind)
		}
	}
}
// cache. Returns true if the event was newly stored.
func (c *Client) mergeEvent(se *core.SignedEvent) (bool, error) {
	ownID, _, err := c.store.Identity()
	if err != nil {
		return false, err
	}
	if se.Author == ownID {
		return true, c.store.AppendOwnEvent(se.Event.Log, se)
	}
	seq := uint64(se.Event.Sequence)
	return c.store.PutCrawledEvent(se, seq)
}

// Session runs a full bidirectional sync between two zens over a single
// connection (section 7.3). The initiator pulls first, then signals role
// reversal so the listener pulls back. Both sides exchange known zen
// tokens for discovery (section 6.1).
//
// A Session is the peer-to-peer primitive the daemon uses for outbound
// dials and inbound accepts. The two directions share one connection: the
// side that opened the connection requests and receives events for both
// logs, then sends MsgReverse; the other side then does the same. This
// avoids a second dial and makes a sync connection symmetric.
type Session struct {
	store *store.Store
	log   *slog.Logger
	zens  func() []ZenRef // known zen refs to offer; may be nil
	onZen func(ZenRef)    // called for each learned zen ref; may be nil

	// key is this node's Ed25519 keypair, used to authenticate the session.
	// Set with SetKey before RunInitiator/RunListener; a nil key means no
	// handshake is performed (for callers that use Server/Client directly).
	key *core.KeyPair

	// onAuthed is called with the zen's authenticated driftnode identity
	// after a successful handshake, before sync traffic begins. May be nil.
	onAuthed func(core.Identity)
}

// NewSession creates a bidirectional sync session.
func NewSession(s *store.Store, log *slog.Logger) *Session {
	if log == nil {
		log = slog.Default()
	}
	return &Session{store: s, log: log}
}

// SetKey sets the Ed25519 keypair used to authenticate the session handshake.
// Required before RunInitiator/RunListener; a nil key skips the handshake.
func (s *Session) SetKey(k *core.KeyPair) { s.key = k }

// SetAuthed sets the callback invoked with the zen's authenticated identity
// after a successful handshake.
func (s *Session) SetAuthed(fn func(core.Identity)) { s.onAuthed = fn }

// SetZenSource sets the function that returns this node's known zen refs to
// offer during zen exchange.
func (s *Session) SetZenSource(fn func() []ZenRef) { s.zens = fn }

// SetZenSink sets the callback invoked for each zen ref learned from the
// remote side.
func (s *Session) SetZenSink(fn func(ZenRef)) { s.onZen = fn }

// Handshake authenticates both sides of the connection by proving possession
// Handshake authenticates both sides' Ed25519 identities before sync traffic
// begins. Each side sends a Hello (identity + nonce), then signs the zen's
// nonce and sends Auth. The zen's identity is returned on success.
//
// Send and receive run concurrently: on synchronous, unbuffered connections
// (like net.Pipe), writing Hello before reading the zen's Hello would
// deadlock, since both sides would block on Write with neither reading.
// A nil key skips the handshake (for callers using Server/Client directly).
func (s *Session) Handshake(r io.Reader, w io.Writer) (core.Identity, error) {
	if s.key == nil {
		return "", nil
	}
	ourNonce, err := randNonce()
	if err != nil {
		return "", fmt.Errorf("handshake nonce: %w", err)
	}
	ourHello := NewHello(s.key.Identity(), ourNonce)

	// Send our Hello while reading the zen's, to avoid deadlock on
	// unbuffered connections.
	writeErr := make(chan error, 1)
	go func() { writeErr <- WriteMsg(w, ourHello) }()
	zenHello, err := ReadMsg(r)
	if err != nil {
		return "", fmt.Errorf("read hello: %w", err)
	}
	if err := <-writeErr; err != nil {
		return "", fmt.Errorf("write hello: %w", err)
	}
	if zenHello.Kind != MsgHello || zenHello.Hello == nil {
		return "", fmt.Errorf("handshake: expected hello, got kind %d", zenHello.Kind)
	}
	zenID := zenHello.Hello.Identity
	zenPub, err := zenID.PubkeyBytes()
	if err != nil {
		return "", fmt.Errorf("zen identity: %w", err)
	}

	// Sign the zen's nonce and send Auth while reading theirs.
	sig := ed25519.Sign(s.key.Private, zenHello.Hello.Nonce[:])
	writeErr = make(chan error, 1)
	go func() { writeErr <- WriteMsg(w, NewAuth(sig)) }()
	zenAuth, err := ReadMsg(r)
	if err != nil {
		return "", fmt.Errorf("read auth: %w", err)
	}
	if err := <-writeErr; err != nil {
		return "", fmt.Errorf("write auth: %w", err)
	}
	if zenAuth.Kind != MsgAuth || zenAuth.Auth == nil {
		return "", fmt.Errorf("handshake: expected auth, got kind %d", zenAuth.Kind)
	}
	if !ed25519.Verify(zenPub, ourNonce[:], zenAuth.Auth.Signature) {
		return "", errors.New("handshake: zen signature verification failed")
	}
	if s.onAuthed != nil {
		s.onAuthed(zenID)
	}
	return zenID, nil
}

func randNonce() ([32]byte, error) {
	var n [32]byte
	if _, err := rand.Read(n[:]); err != nil {
		return [32]byte{}, err
	}
	return n, nil
}

// RunInitiator is called by the zen that opened the connection. It pulls
// both logs from the remote zen, exchanges zen tokens, then signals role
// reversal and serves its own logs back.
func (s *Session) RunInitiator(r io.Reader, w io.Writer) (int, error) {
	if _, err := s.Handshake(r, w); err != nil {
		return 0, err
	}
	merged := 0
	// Pull phase: request the remote zen's PostLog and ProfileLog.
	for _, lg := range []core.LogName{core.PostLog, core.ProfileLog} {
		n, err := s.pullLog(r, w, lg, 0, "")
		if err != nil {
			return merged, err
		}
		merged += n
	}
	// Zen exchange: send our known tokens, receive theirs.
	if err := s.exchangeZens(r, w); err != nil {
		return merged, err
	}
	// Role reversal: tell the remote zen it is now its turn to pull.
	if err := WriteMsg(w, NewReverse()); err != nil {
		return merged, fmt.Errorf("write reverse: %w", err)
	}
	// Serve phase: the remote zen now requests our logs.
	n, err := s.serve(r, w)
	if err != nil {
		return merged, err
	}
	merged += n
	return merged, nil
}

// RunListener is called by the zen that accepted the connection. It serves
// the initiator's requests, exchanges zen tokens, then waits for the
// role-reversal signal and pulls back.
func (s *Session) RunListener(r io.Reader, w io.Writer) (int, error) {
	if _, err := s.Handshake(r, w); err != nil {
		return 0, err
	}
	merged := 0
	// Serve phase: respond to the initiator's requests and zen exchange.
	// serveWithZens returns when it receives MsgReverse (the initiator's
	// signal that it is done pulling and wants to be pulled from).
	n, err := s.serveWithZens(r, w)
	if err != nil {
		return merged, err
	}
	merged += n
	// Pull phase: request the remote zen's PostLog and ProfileLog.
	for _, lg := range []core.LogName{core.PostLog, core.ProfileLog} {
		n, err := s.pullLog(r, w, lg, 0, "")
		if err != nil {
			return merged, err
		}
		merged += n
	}
	return merged, nil
}

// pullLog sends a request for one log and merges the received events.
// A zen closing the connection (EOF) before MsgDone is treated as a clean
// end of stream: any events already received are kept, and the caller can
// retry on the next sync round.
func (s *Session) pullLog(r io.Reader, w io.Writer, log core.LogName, after int64, author core.Identity) (int, error) {
	if err := WriteMsg(w, NewRequest(log, after, author)); err != nil {
		return 0, fmt.Errorf("write request: %w", err)
	}
	merged := 0
	for {
		msg, err := ReadMsg(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return merged, nil
			}
			return merged, fmt.Errorf("read msg: %w", err)
		}
		switch msg.Kind {
		case MsgEvents:
			if msg.Events == nil {
				continue
			}
			for _, se := range msg.Events.Items {
				if err := se.Verify(); err != nil {
					s.log.Warn("sync: rejected unverified event", "err", err)
					continue
				}
				inserted, err := s.mergeEvent(&se)
				if err != nil {
					s.log.Warn("sync: merge failed", "err", err)
					continue
				}
				if inserted {
					merged++
				}
			}
		case MsgDone:
			return merged, nil
		default:
			s.log.Warn("sync: unexpected message kind", "kind", msg.Kind)
		}
	}
}

// serve reads requests and responds with events until the zen sends
// MsgReverse or closes.
func (s *Session) serve(r io.Reader, w io.Writer) (int, error) {
	served := 0
	srv := NewServer(s.store, s.log)
	for {
		msg, err := ReadMsg(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return served, nil
			}
			return served, fmt.Errorf("read msg: %w", err)
		}
		switch msg.Kind {
		case MsgRequest:
			if err := srv.handleRequest(msg.Request, w); err != nil {
				return served, err
			}
			served++
		case MsgFollowers:
			if err := srv.handleFollowers(w); err != nil {
				return served, err
			}
			served++
		case MsgReverse, MsgDone:
			return served, nil
		default:
			s.log.Warn("sync: unexpected message kind", "kind", msg.Kind)
		}
	}
}

// serveWithZens is like serve but also handles MsgZens by invoking the
// zen sink, and sends our zen tokens after the requests complete.
func (s *Session) serveWithZens(r io.Reader, w io.Writer) (int, error) {
	served := 0
	srv := NewServer(s.store, s.log)
	sentZens := false
	for {
		msg, err := ReadMsg(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return served, nil
			}
			return served, fmt.Errorf("read msg: %w", err)
		}
		switch msg.Kind {
		case MsgRequest:
			if err := srv.handleRequest(msg.Request, w); err != nil {
				return served, err
			}
			served++
		case MsgFollowers:
			if err := srv.handleFollowers(w); err != nil {
				return served, err
			}
			served++
		case MsgZens:
			if s.onZen != nil {
				for _, ref := range zensFromMsg(msg) {
					s.onZen(ref)
				}
			}
			// Reply with our zens once.
			if !sentZens {
				var refs []ZenRef
				if s.zens != nil {
					refs = s.zens()
				}
				if err := WriteMsg(w, NewZens(refs)); err != nil {
					return served, fmt.Errorf("write zens: %w", err)
				}
				sentZens = true
			}
		case MsgReverse:
			return served, nil
		case MsgDone:
			return served, nil
		default:
			s.log.Warn("sync: unexpected message kind", "kind", msg.Kind)
		}
	}
}

// exchangeZens sends our known refs and receives the remote side's refs.
func (s *Session) exchangeZens(r io.Reader, w io.Writer) error {
	var refs []ZenRef
	if s.zens != nil {
		refs = s.zens()
	}
	if err := WriteMsg(w, NewZens(refs)); err != nil {
		return fmt.Errorf("write zens: %w", err)
	}
	for {
		msg, err := ReadMsg(r)
		if err != nil {
			return fmt.Errorf("read zens: %w", err)
		}
		if msg.Kind == MsgZens {
			if s.onZen != nil {
				for _, ref := range zensFromMsg(msg) {
					s.onZen(ref)
				}
			}
			return nil
		}
		s.log.Warn("sync: unexpected message kind during zen exchange", "kind", msg.Kind)
	}
}

// zensFromMsg extracts zen refs from a MsgZens, preferring the richer Items
// form and falling back to bare Tokens for backward compatibility with
// peers that predate Items.
func zensFromMsg(msg *Message) []ZenRef {
	if msg.Zens == nil {
		return nil
	}
	if len(msg.Zens.Items) > 0 {
		return msg.Zens.Items
	}
	out := make([]ZenRef, 0, len(msg.Zens.Tokens))
	for _, tok := range msg.Zens.Tokens {
		out = append(out, ZenRef{Token: tok})
	}
	return out
}

func (s *Session) mergeEvent(se *core.SignedEvent) (bool, error) {
	ownID, _, err := s.store.Identity()
	if err != nil {
		return false, err
	}
	if se.Author == ownID {
		return true, s.store.AppendOwnEvent(se.Event.Log, se)
	}
	seq := uint64(se.Event.Sequence)
	return s.store.PutCrawledEvent(se, seq)
}
