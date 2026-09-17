package daemon

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"driftnode/internal/bootstrap"
	"driftnode/internal/core"
	"driftnode/internal/crawler"
	p2p "driftnode/internal/net"
	"driftnode/internal/store"
	syncproto "driftnode/internal/sync"

	"github.com/adrg/xdg"
	"github.com/tailscale/tailcat"
)

// Transport dials a peer by its address token and returns a duplex stream
// over which the sync protocol runs. The production implementation is the
// tailcat Dialer; tests inject an in-process transport to exercise the
// daemon's sync wiring without a network monitor.
type Transport interface {
	Dial(ctx context.Context, token string) (net.Conn, error)
}

// Daemon is the long-running process that holds network state and exposes a
// JSON-lines control API over a Unix domain socket (section 12.1). The CLI
// and TUI are thin clients of this socket.
//
// It owns the tailcat listener (section 5.1) for inbound peer sync and a
// dialer for outbound sync. The sync protocol (section 7.3) runs over each
// tailcat stream.
type Daemon struct {
	store    *store.Store
	socket   string
	logger   *slog.Logger
	mu       sync.Mutex
	listener net.Listener
	peers    map[string]*PeerInfo

	// Network state. The tailcat listener accepts inbound sync connections;
	// nil when the transport is unavailable (the daemon still serves the
	// control socket for local commands).
	tcListener *p2p.Listener
	transport  Transport
	// syncCancel stops the background sync loop on Stop.
	syncCancel context.CancelFunc
	// syncNow triggers an immediate sync round out of the periodic cadence.
	syncNow chan struct{}
	// done is closed when Stop is called, allowing Start's Wait to return.
	done chan struct{}

	// knownTokens is the set of peer tokens learned through bootstrap or
	// peer exchange. connectAndSync dials these; newly learned tokens are
	// added here and to the peers map.
	knownTokens map[string]bool

	// keyFile, if set, persists the tailcat private key so the node's
	// address token stays stable across restarts (section 5.2).
	keyFile string

	// bootstrapPath, if set, is loaded on start. Its seed_peers are
	// auto-dialed after the listener is up (section 6.1).
	bootstrapPath      string
	bootstrapVerifyKey ed25519.PublicKey

	// crawler runs the background BFS of the follow graph (section 9.3).
	crawler *crawler.Crawler
}

// PeerInfo describes a known peer and its connection state.
type PeerInfo struct {
	ID     string `json:"id"`     // tailcat token of the peer
	Kind   string `json:"kind"`   // native_peer or browser
	Status string `json:"status"` // connecting, connected, error
}

// New creates a Daemon backed by the given store.
func New(s *store.Store, logger *slog.Logger) *Daemon {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return &Daemon{
		store:       s,
		logger:      logger,
		peers:       make(map[string]*PeerInfo),
		knownTokens: make(map[string]bool),
		syncNow:     make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
}

// SetKeyFile configures the daemon to persist its tailcat private key to the
// given path, so the node's address token stays stable across restarts.
func (d *Daemon) SetKeyFile(path string) {
	d.mu.Lock()
	d.keyFile = path
	d.mu.Unlock()
}

// SetTransport overrides the outbound peer transport. Used by tests to
// inject an in-process transport without a network monitor.
func (d *Daemon) SetTransport(t Transport) {
	d.mu.Lock()
	d.transport = t
	d.mu.Unlock()
}

// SetBootstrap configures the daemon to load a bootstrap.yaml on start,
// verify its signature, and auto-dial its seed_peers (section 6.1).
func (d *Daemon) SetBootstrap(path string, verifyKey ed25519.PublicKey) {
	d.mu.Lock()
	d.bootstrapPath = path
	d.bootstrapVerifyKey = verifyKey
	d.mu.Unlock()
}

// SocketPath returns the default control socket path for the default store.
// For a non-default store, use SocketPathFor to derive a per-store path so
// multiple daemons on the same machine don't collide.
func SocketPath() (string, error) {
	p, err := xdg.RuntimeFile("driftnode/control.sock")
	if err != nil {
		return "", fmt.Errorf("resolve runtime dir: %w", err)
	}
	return p, nil
}

// SocketPathFor derives a control socket path from the store path, so each
// daemon (one per --db) gets its own socket. Sockets live in a temp dir to
// avoid permission issues with XDG runtime dirs in restricted environments.
func SocketPathFor(storePath string) (string, error) {
	h := sha256.Sum256([]byte(storePath))
	dir := filepath.Join(os.TempDir(), "driftnode-socks")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create socket dir: %w", err)
	}
	return filepath.Join(dir, "ctrl-"+hex.EncodeToString(h[:8])), nil
}

// Start opens the control socket and the tailcat listener, and begins
// accepting connections on both. A failure to start the tailcat listener is
// non-fatal: the daemon still serves the control socket so local commands
// work, and reports the transport as unavailable via status.
func (d *Daemon) Start(socketPath string) error {
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return fmt.Errorf("create socket dir: %w", err)
	}
	// Remove a stale socket file if present.
	os.Remove(socketPath)
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen on control socket: %w", err)
	}
	d.mu.Lock()
	d.listener = l
	d.socket = socketPath
	d.mu.Unlock()
	d.logger.Info("daemon started", "socket", socketPath)
	go d.acceptLoop()

	// Start the tailcat listener for inbound peer sync. Non-fatal if the
	// transport cannot start (the sandbox may lack a network monitor). A
	// transport injected via SetTransport (for tests) is not overwritten.
	d.mu.Lock()
	injected := d.transport
	d.mu.Unlock()
	if injected == nil {
		d.transport = tailcatTransport{p2p.NewDialer(d.logger)}
	}
	d.mu.Lock()
	keyFile := d.keyFile
	d.mu.Unlock()
	var keyCfg *p2p.KeyConfig
	if keyFile != "" {
		keyCfg = &p2p.KeyConfig{KeyFile: keyFile}
	}
	tcListener, err := p2p.NewListenerWithKey(func(conn net.Conn) {
		defer conn.Close()
		if _, err := d.runSession(conn, false); err != nil {
			d.logger.Warn("inbound sync ended", "err", err)
		}
	}, d.logger, keyCfg)
	if err != nil {
		d.logger.Warn("tailcat listener unavailable; inbound peer sync disabled", "err", err)
	} else {
		d.mu.Lock()
		d.tcListener = tcListener
		d.mu.Unlock()
		d.logger.Info("tailcat listener started", "addr", tcListener.Addr())
		// Persist the key so the token stays stable across restarts.
		if keyFile != "" {
			if err := tcListener.SaveKeyFile(keyFile); err != nil {
				d.logger.Warn("persist tailcat key", "err", err)
			}
		}
	}

	// Background sync loop: pull from known peers periodically and on demand.
	ctx, cancel := context.WithCancel(context.Background())
	d.syncCancel = cancel
	go d.syncLoop(ctx)

	// Auto-dial bootstrap seed peers (section 6.1). A fresh node finds its
	// first peers this way; the file is verified before any dial.
	d.mu.Lock()
	bootstrapPath := d.bootstrapPath
	bootstrapKey := d.bootstrapVerifyKey
	d.mu.Unlock()

	// Initialize the crawler (section 9.3). It walks the follow graph,
	// fetching Profile logs only, seeded from bootstrap's crawl_seeds.
	d.mu.Lock()
	d.crawler = crawler.New(d.store, d.logger)
	d.mu.Unlock()

	if bootstrapPath != "" {
		go d.dialBootstrapSeeds(bootstrapPath, bootstrapKey)
	}

	return nil
}

// Stop closes the control socket, the tailcat listener, and stops the
// background sync loop, then removes the socket file. It is safe to call
// from the control-socket handler (self-stop) or from a signal handler.
func (d *Daemon) Stop() {
	d.mu.Lock()
	if d.listener != nil {
		d.listener.Close()
		d.listener = nil
	}
	if d.tcListener != nil {
		d.tcListener.Close()
		d.tcListener = nil
	}
	if d.syncCancel != nil {
		d.syncCancel()
		d.syncCancel = nil
	}
	if d.socket != "" {
		os.Remove(d.socket)
		d.socket = ""
	}
	// Signal Start's Wait (or the foreground blocker) to return. Use a
	// guard so a double Stop is a no-op for the channel close.
	select {
	case <-d.done:
		// already closed
	default:
		close(d.done)
	}
	d.mu.Unlock()
	d.logger.Info("daemon stopped")
}

// Wait blocks until Stop is called. This lets the foreground command block
// on the daemon's own lifecycle rather than on an external signal.
func (d *Daemon) Wait() {
	<-d.done
}

// Done returns a channel that is closed when Stop is called, so callers can
// select on it alongside other channels (e.g. OS signals).
func (d *Daemon) Done() <-chan struct{} {
	return d.done
}

func (d *Daemon) acceptLoop() {
	for {
		d.mu.Lock()
		listener := d.listener
		d.mu.Unlock()
		if listener == nil {
			return
		}
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go d.handleConn(conn)
	}
}

// Request is the JSON-lines request envelope sent by CLI/TUI clients.
type Request struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

// Response is the JSON-lines response envelope.
type Response struct {
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

func (d *Daemon) handleConn(conn net.Conn) {
	defer conn.Close()
	dec := json.NewDecoder(conn)
	enc := json.NewEncoder(conn)
	for {
		var req Request
		if err := dec.Decode(&req); err != nil {
			return
		}
		resp := d.dispatch(req)
		if err := enc.Encode(resp); err != nil {
			return
		}
	}
}

func (d *Daemon) dispatch(req Request) Response {
	switch req.Method {
	case "whoami":
		id, ok, err := d.store.Identity()
		if err != nil {
			return Response{Error: err.Error()}
		}
		if !ok {
			return Response{Error: "no identity"}
		}
		return Response{Result: map[string]string{"identity": id.String()}}
	case "feed":
		var p struct {
			Limit int `json:"limit"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return Response{Error: fmt.Sprintf("parse params: %s", err)}
			}
		}
		return d.handleFeed(p.Limit)
	case "post":
		var p struct {
			Text       string `json:"text"`
			Passphrase string `json:"passphrase"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return Response{Error: fmt.Sprintf("parse params: %s", err)}
			}
		}
		if p.Text == "" || p.Passphrase == "" {
			return Response{Error: "text and passphrase required"}
		}
		return d.handlePost(p.Text, p.Passphrase)
	case "token":
		d.mu.Lock()
		var addr tailcat.Addr
		if d.tcListener != nil {
			addr = d.tcListener.Addr()
		}
		d.mu.Unlock()
		return Response{Result: map[string]string{"token": string(addr)}}
	case "peers":
		d.mu.Lock()
		peers := make([]*PeerInfo, 0, len(d.peers))
		for _, p := range d.peers {
			peers = append(peers, p)
		}
		d.mu.Unlock()
		return Response{Result: peers}
	case "peers_add":
		var p struct {
			Token string `json:"token"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return Response{Error: fmt.Sprintf("parse params: %s", err)}
			}
		}
		if p.Token == "" {
			return Response{Error: "token required"}
		}
		d.addPeer(p.Token, "native_peer", "connecting")
		d.mu.Lock()
		d.knownTokens[p.Token] = true
		d.mu.Unlock()
		go d.connectAndSync(p.Token)
		return Response{Result: map[string]string{"status": "connecting", "token": p.Token}}
	case "follow":
		var p struct {
			Target     string `json:"target"`
			Passphrase string `json:"passphrase"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return Response{Error: fmt.Sprintf("parse params: %s", err)}
			}
		}
		if p.Target == "" || p.Passphrase == "" {
			return Response{Error: "target and passphrase required"}
		}
		return d.handleFollow(p.Target, p.Passphrase)
	case "unfollow":
		var p struct {
			Target     string `json:"target"`
			Passphrase string `json:"passphrase"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return Response{Error: fmt.Sprintf("parse params: %s", err)}
			}
		}
		if p.Target == "" || p.Passphrase == "" {
			return Response{Error: "target and passphrase required"}
		}
		return d.handleUnfollow(p.Target, p.Passphrase)
	case "sync":
		var p struct {
			Now bool `json:"now"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return Response{Error: fmt.Sprintf("parse params: %s", err)}
			}
		}
		if p.Now {
			select {
			case d.syncNow <- struct{}{}:
			default:
			}
		}
		d.logger.Info("sync requested", "now", p.Now)
		return Response{Result: map[string]string{"status": "triggered"}}
	case "stop":
		// Acknowledge first, then shut down asynchronously so the response
		// is written before the listener closes and tears down this conn.
		go func() {
			time.Sleep(50 * time.Millisecond)
			d.Stop()
		}()
		return Response{Result: map[string]string{"status": "stopping"}}
	case "status":
		d.mu.Lock()
		tcUp := d.tcListener != nil
		var addr tailcat.Addr
		if tcUp {
			addr = d.tcListener.Addr()
		}
		d.mu.Unlock()
		return Response{Result: map[string]any{
			"running":     true,
			"peers":       len(d.peers),
			"socket":      d.socket,
			"transport":   tcUp,
			"listen_addr": string(addr),
		}}
	default:
		return Response{Error: fmt.Sprintf("unknown method: %s", req.Method)}
	}
}

// FeedItem is one post in the merged feed, serialized to the CLI.
type FeedItem struct {
	Timestamp int64  `json:"timestamp"`
	Author    string `json:"author"`
	Text      string `json:"text"`
}

// handleFeed returns the merged timeline (own PostLog plus synced PostLogs
// from followed identities), reverse-chronological, up to limit entries.
func (d *Daemon) handleFeed(limit int) Response {
	allPosts, err := d.store.AllPosts()
	if err != nil {
		return Response{Error: fmt.Sprintf("read posts: %s", err)}
	}
	log := core.NewLog(allPosts)
	posts := log.Posts()
	if limit > 0 && len(posts) > limit {
		posts = posts[:limit]
	}
	items := make([]FeedItem, 0, len(posts))
	for _, p := range posts {
		text := p.Event.Post.Text
		if p.Event.Reply != nil {
			text = "(reply) " + text
		}
		items = append(items, FeedItem{
			Timestamp: p.Event.Timestamp,
			Author:    p.Author.String(),
			Text:      text,
		})
	}
	return Response{Result: items}
}

// handlePost decrypts the private key with the passphrase, signs a Post
// event, and appends it to the local PostLog.
func (d *Daemon) handlePost(text, passphrase string) Response {
	ek, err := d.store.EncryptedKey()
	if err != nil {
		return Response{Error: fmt.Sprintf("read key: %s", err)}
	}
	enc := core.DefaultKeyEncryption()
	priv, err := enc.Decrypt(ek, []byte(passphrase))
	if err != nil {
		return Response{Error: fmt.Sprintf("decrypt key: %s", err)}
	}
	kp, err := core.KeyPairFromBytes(priv)
	if err != nil {
		return Response{Error: fmt.Sprintf("keypair: %s", err)}
	}
	seq, err := d.store.OwnEventCount(core.PostLog)
	if err != nil {
		return Response{Error: fmt.Sprintf("sequence: %s", err)}
	}
	se, err := kp.Sign(core.Event{
		Kind:      core.KindPost,
		Log:       core.PostLog,
		Timestamp: core.Now64(),
		Sequence:  seq + 1,
		Post:      &core.Post{Text: text},
	})
	if err != nil {
		return Response{Error: fmt.Sprintf("sign: %s", err)}
	}
	if err := d.store.AppendOwnEvent(core.PostLog, se); err != nil {
		return Response{Error: fmt.Sprintf("append: %s", err)}
	}
	id, _ := se.ID()
	return Response{Result: map[string]string{"event_id": id.String()}}
}

// handleFollow decrypts the private key, signs a Follow event, and appends
// it to the local ProfileLog.
func (d *Daemon) handleFollow(targetStr, passphrase string) Response {
	target, err := resolvePubkey(targetStr)
	if err != nil {
		return Response{Error: fmt.Sprintf("resolve target: %s", err)}
	}
	ek, err := d.store.EncryptedKey()
	if err != nil {
		return Response{Error: fmt.Sprintf("read key: %s", err)}
	}
	enc := core.DefaultKeyEncryption()
	priv, err := enc.Decrypt(ek, []byte(passphrase))
	if err != nil {
		return Response{Error: fmt.Sprintf("decrypt key: %s", err)}
	}
	kp, err := core.KeyPairFromBytes(priv)
	if err != nil {
		return Response{Error: fmt.Sprintf("keypair: %s", err)}
	}
	seq, err := d.store.OwnEventCount(core.ProfileLog)
	if err != nil {
		return Response{Error: fmt.Sprintf("sequence: %s", err)}
	}
	se, err := kp.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: core.Now64(),
		Sequence:  seq + 1,
		Follow:    &core.Follow{TargetPubkey: target},
	})
	if err != nil {
		return Response{Error: fmt.Sprintf("sign: %s", err)}
	}
	if err := d.store.AppendOwnEvent(core.ProfileLog, se); err != nil {
		return Response{Error: fmt.Sprintf("append: %s", err)}
	}
	return Response{Result: map[string]string{"followed": core.IdentityFromPubkey(ed25519.PublicKey(target[:])).String()}}
}

// handleUnfollow decrypts the private key, signs an Unfollow event, and
// appends it to the local ProfileLog.
func (d *Daemon) handleUnfollow(targetStr, passphrase string) Response {
	target, err := resolvePubkey(targetStr)
	if err != nil {
		return Response{Error: fmt.Sprintf("resolve target: %s", err)}
	}
	ek, err := d.store.EncryptedKey()
	if err != nil {
		return Response{Error: fmt.Sprintf("read key: %s", err)}
	}
	enc := core.DefaultKeyEncryption()
	priv, err := enc.Decrypt(ek, []byte(passphrase))
	if err != nil {
		return Response{Error: fmt.Sprintf("decrypt key: %s", err)}
	}
	kp, err := core.KeyPairFromBytes(priv)
	if err != nil {
		return Response{Error: fmt.Sprintf("keypair: %s", err)}
	}
	seq, err := d.store.OwnEventCount(core.ProfileLog)
	if err != nil {
		return Response{Error: fmt.Sprintf("sequence: %s", err)}
	}
	se, err := kp.Sign(core.Event{
		Kind:      core.KindUnfollow,
		Log:       core.ProfileLog,
		Timestamp: core.Now64(),
		Sequence:  seq + 1,
		Follow:    &core.Follow{TargetPubkey: target},
	})
	if err != nil {
		return Response{Error: fmt.Sprintf("sign: %s", err)}
	}
	if err := d.store.AppendOwnEvent(core.ProfileLog, se); err != nil {
		return Response{Error: fmt.Sprintf("append: %s", err)}
	}
	return Response{Result: map[string]string{"unfollowed": core.IdentityFromPubkey(ed25519.PublicKey(target[:])).String()}}
}

// connectAndSync dials a peer by its address token and runs a bidirectional
// sync session (section 7.3): this peer pulls the remote peer's logs, then
// serves its own logs back over the same connection. Peer tokens learned
// during the exchange are recorded for future dials (section 6.1).
func (d *Daemon) connectAndSync(token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	d.mu.Lock()
	transport := d.transport
	d.mu.Unlock()
	if transport == nil {
		d.logger.Warn("peer dial failed: no transport configured", "token", token)
		d.setPeerStatus(token, "error")
		return
	}
	conn, err := transport.Dial(ctx, token)
	if err != nil {
		d.logger.Warn("peer dial failed", "token", token, "err", err)
		d.setPeerStatus(token, "error")
		return
	}
	defer conn.Close()
	d.setPeerStatus(token, "connected")
	merged, err := d.runSession(conn, true)
	if err != nil {
		d.logger.Warn("sync round failed", "token", token, "err", err)
	}
	d.logger.Info("sync round done", "token", token, "merged", merged)
}

// dialBootstrapSeeds loads a bootstrap.yaml, verifies its signature, and
// dials each seed peer (section 6.1). Called asynchronously from Start.
func (d *Daemon) dialBootstrapSeeds(path string, verifyKey ed25519.PublicKey) {
	bf, err := bootstrap.Load(path)
	if err != nil {
		d.logger.Warn("bootstrap load failed", "err", err)
		return
	}
	if verifyKey != nil {
		if err := bf.Verify(verifyKey); err != nil {
			d.logger.Warn("bootstrap signature verification failed", "err", err)
			return
		}
		d.logger.Info("bootstrap verified", "seed_peers", len(bf.SeedPeers))
	} else {
		d.logger.Info("bootstrap loaded (no verification key)", "seed_peers", len(bf.SeedPeers))
	}
	for _, sp := range bf.SeedPeers {
		if sp.Token == "" {
			continue
		}
		d.mu.Lock()
		already := d.knownTokens[sp.Token]
		d.knownTokens[sp.Token] = true
		d.mu.Unlock()
		if already {
			continue
		}
		d.addPeer(sp.Token, sp.Kind, "connecting")
		d.connectAndSync(sp.Token)
	}

	// Seed the crawler from crawl_seeds (section 9.3). The crawler walks
	// the follow graph, fetching Profile logs only.
	var seeds []core.Identity
	for _, s := range bf.CrawlSeeds {
		if id, err := core.ParseIdentity(s); err == nil {
			seeds = append(seeds, id)
		}
	}
	if len(seeds) > 0 {
		d.mu.Lock()
		if d.crawler != nil {
			d.crawler.Seed(seeds...)
		}
		d.mu.Unlock()
		d.logger.Info("crawl seeds loaded", "count", len(seeds))
	}
}

// knownTokensList returns the current set of known peer tokens for peer
// exchange, including this node's own listener address so peers can dial
// it back and discover it through the exchange.
func (d *Daemon) knownTokensList() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, 0, len(d.knownTokens)+1)
	if d.tcListener != nil {
		out = append(out, string(d.tcListener.Addr()))
	}
	for tok := range d.knownTokens {
		if d.tcListener != nil && tok == string(d.tcListener.Addr()) {
			continue
		}
		out = append(out, tok)
	}
	return out
}

// learnPeerToken records a peer token learned through peer exchange and adds
// it to the peers map for a future sync round.
func (d *Daemon) learnPeerToken(token string) {
	if token == "" {
		return
	}
	d.mu.Lock()
	if d.knownTokens[token] {
		d.mu.Unlock()
		return
	}
	d.knownTokens[token] = true
	_, exists := d.peers[token]
	d.mu.Unlock()
	if !exists {
		d.addPeer(token, "native_peer", "discovered")
	}
}

// syncLoop periodically syncs with all known peers and also fires on demand
// when syncNow is signaled. It also runs the crawler periodically to walk
// the follow graph (section 9.3).
func (d *Daemon) syncLoop(ctx context.Context) {
	const syncInterval = 5 * time.Minute
	const crawlInterval = 10 * time.Minute
	syncTicker := time.NewTicker(syncInterval)
	crawlTicker := time.NewTicker(crawlInterval)
	defer syncTicker.Stop()
	defer crawlTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.syncNow:
			d.syncAllPeers()
			d.runCrawl(ctx)
		case <-syncTicker.C:
			d.syncAllPeers()
		case <-crawlTicker.C:
			d.runCrawl(ctx)
		}
	}
}

// runCrawl runs one crawl pass if the crawler has seeds.
func (d *Daemon) runCrawl(ctx context.Context) {
	d.mu.Lock()
	c := d.crawler
	d.mu.Unlock()
	if c == nil {
		return
	}
	fetched, err := c.Run(ctx, &crawlFetcher{d: d})
	if err != nil {
		d.logger.Warn("crawl failed", "err", err)
		return
	}
	d.logger.Info("crawl complete", "fetched", fetched, "visited", c.VisitedCount())
}

// crawlFetcher implements crawler.Fetcher by dialing known peers to fetch
// Profile logs.
type crawlFetcher struct{ d *Daemon }

func (f *crawlFetcher) FetchProfileLog(ctx context.Context, id core.Identity) ([]core.SignedEvent, error) {
	return f.d.crawlFetch(ctx, id)
}

// syncAllPeers runs a sync round against every known peer token.
func (d *Daemon) syncAllPeers() {
	d.mu.Lock()
	tokens := make([]string, 0, len(d.peers))
	for tok := range d.peers {
		tokens = append(tokens, tok)
	}
	d.mu.Unlock()
	for _, tok := range tokens {
		d.connectAndSync(tok)
	}
}

// AddPeer records a connected peer.
func (d *Daemon) AddPeer(id, kind, status string) {
	d.mu.Lock()
	d.peers[id] = &PeerInfo{ID: id, Kind: kind, Status: status}
	d.mu.Unlock()
}

// addPeer is the internal recorder used by peers_add.
func (d *Daemon) addPeer(id, kind, status string) {
	d.AddPeer(id, kind, status)
}

// RemovePeer removes a peer record.
func (d *Daemon) RemovePeer(id string) {
	d.mu.Lock()
	delete(d.peers, id)
	d.mu.Unlock()
}

// setPeerStatus updates a recorded peer's status.
func (d *Daemon) setPeerStatus(id, status string) {
	d.mu.Lock()
	if p, ok := d.peers[id]; ok {
		p.Status = status
	}
	d.mu.Unlock()
}

// crawlFetch fetches a remote identity's ProfileLog by dialing each known
// peer and requesting that identity's Profile log (section 9.3). Returns
// the events from the first peer that has them.
func (d *Daemon) crawlFetch(ctx context.Context, id core.Identity) ([]core.SignedEvent, error) {
	tokens := d.knownTokensList()
	for _, tok := range tokens {
		conn, err := d.transport.Dial(ctx, tok)
		if err != nil {
			continue
		}
		events, err := syncproto.NewClient(d.store, d.logger).SyncLogRaw(conn, conn, core.ProfileLog, 0, id)
		conn.Close()
		if err != nil {
			continue
		}
		if len(events) > 0 {
			return events, nil
		}
	}
	return nil, nil
}

// runSession runs a bidirectional sync session over conn. When initiator is
// true, this side opened the connection and pulls first; otherwise the
// remote side opened it and we serve first. Peer tokens learned during the
// exchange are recorded for future dials.
func (d *Daemon) runSession(conn net.Conn, initiator bool) (int, error) {
	sess := syncproto.NewSession(d.store, d.logger)
	sess.SetPeerSource(d.knownTokensList)
	sess.SetPeerSink(d.learnPeerToken)
	if initiator {
		return sess.RunInitiator(conn, conn)
	}
	return sess.RunListener(conn, conn)
}

// resolvePubkey parses a driftnode identity string or raw base32 pubkey into
// a 32-byte Ed25519 public key.
func resolvePubkey(arg string) ([32]byte, error) {
	if id, err := core.ParseIdentity(arg); err == nil {
		pub, err := id.PubkeyBytes()
		if err != nil {
			return [32]byte{}, err
		}
		return [32]byte(pub), nil
	}
	pub, err := core.PubkeyFromBase32(arg)
	if err != nil {
		return [32]byte{}, fmt.Errorf("invalid pubkey %q: %w", arg, err)
	}
	return [32]byte(pub), nil
}

// Dial connects to a running daemon's control socket and returns the
// connection. It returns an error if the daemon is not running.
func Dial(socketPath string) (net.Conn, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, errors.New("daemon not running (is 'driftnode daemon' started?)")
	}
	return conn, nil
}

// SendRequest opens a connection to the daemon, sends a request, and reads
// the response. It closes the connection after one request.
func SendRequest(socketPath, method string, params map[string]any) (*Response, error) {
	conn, err := Dial(socketPath)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var paramsRaw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("marshal params: %w", err)
		}
		paramsRaw = b
	}
	req := Request{Method: method, Params: paramsRaw}
	enc := json.NewEncoder(conn)
	if err := enc.Encode(&req); err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	var resp Response
	dec := json.NewDecoder(conn)
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.Error != "" {
		return &resp, fmt.Errorf("%s", resp.Error)
	}
	return &resp, nil
}

// tailcatTransport adapts the net.Dialer (which dials a tailcat.Addr) to the
// daemon's Transport interface (which dials a string token).
type tailcatTransport struct {
	d *p2p.Dialer
}

func (t tailcatTransport) Dial(ctx context.Context, token string) (net.Conn, error) {
	return t.d.Dial(ctx, tailcat.Addr(token))
}
