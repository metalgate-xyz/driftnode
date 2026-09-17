package sync

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"driftnode/internal/core"
	"driftnode/internal/store"
)

// Server handles incoming sync requests from a peer. It reads requests and
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
// responses to w until the peer sends Done or the connection closes.
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
	cached, err := s.store.FollowedEvents(req.Author)
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

// mergeEvent stores a received event. Own-author events go into the own
// logs (dedup by event ID); followed-author events go into the follows
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
	return c.store.PutFollowedEvent(se, seq)
}

// Session runs a full bidirectional sync between two peers over a single
// connection (section 7.3). The initiator pulls first, then signals role
// reversal so the listener pulls back. Both sides exchange known peer
// tokens for discovery (section 6.1).
//
// A Session is the peer-to-peer primitive the daemon uses for outbound
// dials and inbound accepts. The two directions share one connection: the
// side that opened the connection requests and receives events for both
// logs, then sends MsgReverse; the other side then does the same. This
// avoids a second dial and makes a sync connection symmetric.
type Session struct {
	store  *store.Store
	log    *slog.Logger
	peers  func() []string // known peer tokens to offer; may be nil
	onPeer func(string)     // called for each learned peer token; may be nil
}

// NewSession creates a bidirectional sync session.
func NewSession(s *store.Store, log *slog.Logger) *Session {
	if log == nil {
		log = slog.Default()
	}
	return &Session{store: s, log: log}
}

// SetPeerSource sets the function that returns this node's known peer tokens
// to offer during peer exchange.
func (s *Session) SetPeerSource(fn func() []string) { s.peers = fn }

// SetPeerSink sets the callback invoked for each peer token learned from the
// remote side.
func (s *Session) SetPeerSink(fn func(string)) { s.onPeer = fn }

// RunInitiator is called by the peer that opened the connection. It pulls
// both logs from the remote peer, exchanges peer tokens, then signals role
// reversal and serves its own logs back.
func (s *Session) RunInitiator(r io.Reader, w io.Writer) (int, error) {
	merged := 0
	// Pull phase: request the remote peer's PostLog and ProfileLog.
	for _, lg := range []core.LogName{core.PostLog, core.ProfileLog} {
		n, err := s.pullLog(r, w, lg, 0, "")
		if err != nil {
			return merged, err
		}
		merged += n
	}
	// Peer exchange: send our known tokens, receive theirs.
	if err := s.exchangePeers(r, w); err != nil {
		return merged, err
	}
	// Role reversal: tell the remote peer it is now its turn to pull.
	if err := WriteMsg(w, NewReverse()); err != nil {
		return merged, fmt.Errorf("write reverse: %w", err)
	}
	// Serve phase: the remote peer now requests our logs.
	n, err := s.serve(r, w)
	if err != nil {
		return merged, err
	}
	merged += n
	return merged, nil
}

// RunListener is called by the peer that accepted the connection. It serves
// the initiator's requests, exchanges peer tokens, then waits for the
// role-reversal signal and pulls back.
func (s *Session) RunListener(r io.Reader, w io.Writer) (int, error) {
	merged := 0
	// Serve phase: respond to the initiator's requests and peer exchange.
	// serveWithPeers returns when it receives MsgReverse (the initiator's
	// signal that it is done pulling and wants to be pulled from).
	n, err := s.serveWithPeers(r, w)
	if err != nil {
		return merged, err
	}
	merged += n
	// Pull phase: request the remote peer's PostLog and ProfileLog.
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
// A peer closing the connection (EOF) before MsgDone is treated as a clean
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

// serve reads requests and responds with events until the peer sends
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
		case MsgReverse, MsgDone:
			return served, nil
		default:
			s.log.Warn("sync: unexpected message kind", "kind", msg.Kind)
		}
	}
}

// serveWithPeers is like serve but also handles MsgPeers by invoking the
// peer sink, and sends our peer tokens after the requests complete.
func (s *Session) serveWithPeers(r io.Reader, w io.Writer) (int, error) {
	served := 0
	srv := NewServer(s.store, s.log)
	sentPeers := false
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
		case MsgPeers:
			if msg.Peers != nil {
				for _, tok := range msg.Peers.Tokens {
					if s.onPeer != nil {
						s.onPeer(tok)
					}
				}
			}
			// Reply with our peers once.
			if !sentPeers {
				var tokens []string
				if s.peers != nil {
					tokens = s.peers()
				}
				if err := WriteMsg(w, NewPeers(tokens)); err != nil {
					return served, fmt.Errorf("write peers: %w", err)
				}
				sentPeers = true
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

// exchangePeers sends our known tokens and receives the remote side's tokens.
func (s *Session) exchangePeers(r io.Reader, w io.Writer) error {
	var tokens []string
	if s.peers != nil {
		tokens = s.peers()
	}
	if err := WriteMsg(w, NewPeers(tokens)); err != nil {
		return fmt.Errorf("write peers: %w", err)
	}
	for {
		msg, err := ReadMsg(r)
		if err != nil {
			return fmt.Errorf("read peers: %w", err)
		}
		if msg.Kind == MsgPeers {
			if msg.Peers != nil && s.onPeer != nil {
				for _, tok := range msg.Peers.Tokens {
					s.onPeer(tok)
				}
			}
			return nil
		}
		s.log.Warn("sync: unexpected message kind during peer exchange", "kind", msg.Kind)
	}
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
	return s.store.PutFollowedEvent(se, seq)
}
