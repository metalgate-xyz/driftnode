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

	"github.com/tailscale/tailcat"
)

// Transport dials a zen by its address token and returns a duplex stream
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
// It owns the tailcat listener (section 5.1) for inbound zen sync and a
// dialer for outbound sync. The sync protocol (section 7.3) runs over each
// tailcat stream.
type Daemon struct {
	store    *store.Store
	socket   string
	logger   *slog.Logger
	mu       sync.Mutex
	listener net.Listener
	zens     map[string]*zenInfo

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

	// unlocked holds the signing key after a successful unlock RPC, so
	// signing commands (post, follow, unfollow) don't re-supply the
	// passphrase on every call. Cleared by lock, by Stop, or by idle-lock
	// expiry (see SetIdleLock). Guarded by mu.
	unlocked   *core.KeyPair
	lastSignAt time.Time
	idleLock   time.Duration

	// knownTokens is the set of zen tokens learned through bootstrap or
	// zen exchange, used for zen-exchange offers and dedup. The auto-dial
	// set is the follow graph (FollowGraph) plus the routing table
	// (identity -> token); knownTokens is only for offering tokens to
	// zens and recording discovered tokens for future intent.
	knownTokens map[string]bool

	// ephemeralKey, when set, disables key persistence: a fresh tailcat
	// key is generated each run and never saved, so the address token
	// changes on every restart. The default (false) is a store-persisted
	// key with a stable token.
	ephemeralKey bool

	// bootstrapPath, if set, is loaded on start. Its seed_zens are
	// auto-dialed after the listener is up (section 6.1).
	bootstrapPath      string
	bootstrapVerifyKey ed25519.PublicKey

	// crawler runs the background BFS of the follow graph (section 9.3).
	crawler *crawler.Crawler

	// bootstrapDone is true once the bootstrap seeds have been followed.
	// Cleared while the key is locked, so the next unlock retries the
	// bootstrap auto-follow if it ran before the key was available.
	bootstrapDone bool

	// syncConcurrency caps the number of zen dials that run in parallel
	// during a sync round. Default 8; 0 falls back to that.
	syncConcurrency int
}

// zenInfo describes a known zen and its connection state.
type zenInfo struct {
	ID       string `json:"id"`       // tailcat token of the zen
	Identity string `json:"identity"` // driftnode:<pubkey> once learned, else empty
	Name     string `json:"name"`     // zen name once known, else empty
	Kind     string `json:"kind"`     // native_zen or browser
	Status   string `json:"status"`   // connecting, connected, discovered, error
	Verified bool   `json:"verified"` // identity confirmed out-of-band (§7)
}

// New creates a Daemon backed by the given store.
func New(s *store.Store, logger *slog.Logger) *Daemon {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return &Daemon{
		store:       s,
		logger:      logger,
		zens:        make(map[string]*zenInfo),
		knownTokens: make(map[string]bool),
		syncNow:     make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
}

// SetEphemeralKey disables key persistence so the address token is fresh
// each run and never saved.
func (d *Daemon) SetEphemeralKey() {
	d.mu.Lock()
	d.ephemeralKey = true
	d.mu.Unlock()
}

// SetTransport overrides the outbound zen transport. Used by tests to
// inject an in-process transport without a network monitor.
func (d *Daemon) SetTransport(t Transport) {
	d.mu.Lock()
	d.transport = t
	d.mu.Unlock()
}

// SetBootstrap configures the daemon to load a bootstrap.yaml on start,
// verify its signature, and auto-dial its seed_zens (section 6.1).
func (d *Daemon) SetBootstrap(path string, verifyKey ed25519.PublicKey) {
	d.mu.Lock()
	d.bootstrapPath = path
	d.bootstrapVerifyKey = verifyKey
	d.mu.Unlock()
}

// SetIdleLock configures how long the unlocked signing key stays resident in
// memory after the last signing command. A zero duration (the default) keeps
// the key unlocked until an explicit lock or Stop.
func (d *Daemon) SetIdleLock(dur time.Duration) {
	d.mu.Lock()
	d.idleLock = dur
	d.mu.Unlock()
}

// SetSyncConcurrency caps the number of zen dials that run in parallel
// during a sync round. A zero or negative value falls back to the default.
func (d *Daemon) SetSyncConcurrency(n int) {
	if n <= 0 {
		n = 8
	}
	d.mu.Lock()
	d.syncConcurrency = n
	d.mu.Unlock()
}

// SetUnlockedKey installs a decrypted keypair so the daemon can sign and run
// bootstrap auto-follow from Start, without waiting for an unlock RPC. Used
// by interactive daemon starts that prompt for the passphrase before Start.
func (d *Daemon) SetUnlockedKey(kp *core.KeyPair) {
	d.mu.Lock()
	d.unlocked = kp
	d.lastSignAt = time.Now()
	d.mu.Unlock()
}

// ErrNotRunning is returned when a client dials a socket with no daemon
// listening, so callers can distinguish a down daemon from an in-daemon
// error.
var ErrNotRunning = errors.New("daemon not running (is 'driftnode daemon' started?)")

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

	// Start the tailcat listener for inbound zen sync. Non-fatal if the
	// transport cannot start (the sandbox may lack a network monitor). A
	// transport injected via SetTransport (for tests) is not overwritten.
	d.mu.Lock()
	injected := d.transport
	d.mu.Unlock()
	if injected == nil {
		d.transport = tailcatTransport{p2p.NewDialer(d.logger)}
	}
	if err := d.startTransport(); err != nil {
		d.logger.Warn("tailcat listener unavailable; inbound zen sync disabled", "err", err)
	}

	// Background sync loop: pull from known zens periodically and on demand.
	ctx, cancel := context.WithCancel(context.Background())
	d.syncCancel = cancel
	go d.syncLoop(ctx)
	// Trigger an immediate first round so a reattached daemon pulls the
	// logs it missed while down, instead of waiting up to syncInterval.
	d.triggerSyncNow()

	// Auto-dial bootstrap seed zens (section 6.1). A fresh node finds its
	// first zens this way; the file is verified before any dial.
	d.mu.Lock()
	bootstrapPath := d.bootstrapPath
	bootstrapKey := d.bootstrapVerifyKey
	d.mu.Unlock()

	// Initialize the crawler (section 9.3). It walks the follow graph,
	// fetching Profile logs only, seeded from bootstrap's crawl_seeds.
	d.mu.Lock()
	d.crawler = crawler.New(d.store, d.logger)
	d.mu.Unlock()

	// Rehydrate the zens map from the persisted routing table so a restart
	// shows the known network immediately, before the next sync round
	// re-dials each token. The routing table binds followed identities to
	// their tailcat tokens; a fresh daemon otherwise has an empty zens map
	// until a dial repopulates it. Status starts as "connecting" and is
	// refreshed to connected/error as the sync loop reaches each token.
	d.rehydrateZens()

	if bootstrapPath != "" {
		go d.dialBootstrapSeeds(bootstrapPath, bootstrapKey)
	}

	return nil
}

// rehydrateZens loads the persisted routing bindings (identity -> token)
// into the in-memory zens map. Called from Start so the known network is
// visible to the zens RPC right after a restart, instead of appearing empty
// until the background sync loop redials each token. Tokens learned only
// through zen exchange are not persisted, so they are not restored here;
// they are relearned on the next exchange.
func (d *Daemon) rehydrateZens() {
	routing, err := d.store.AllRouting()
	if err != nil {
		d.logger.Warn("rehydrate zens: read routing", "err", err)
		return
	}
	if len(routing) == 0 {
		return
	}
	d.mu.Lock()
	for id, tok := range routing {
		if _, exists := d.zens[tok]; exists {
			continue
		}
		d.zens[tok] = &zenInfo{
			ID:       tok,
			Identity: string(id),
			Kind:     "native_zen",
			Status:   "connecting",
		}
		if name, _ := d.store.DisplayName(id); name != "" {
			d.zens[tok].Name = name
		}
		if v, _ := d.store.IsVerified(id); v {
			d.zens[tok].Verified = true
		}
	}
	d.mu.Unlock()
}

// startTransport opens the tailcat listener for inbound zen sync using the
// key persisted in the local store (or a fresh key when no stored key exists
// or ephemeral mode is set). A fresh key is persisted on first use so the
// address token stays stable across restarts. Caller must hold no lock.
func (d *Daemon) startTransport() error {
	d.mu.Lock()
	ephemeral := d.ephemeralKey
	d.mu.Unlock()
	var keyCfg *p2p.KeyConfig
	if !ephemeral {
		if stored, ok, _ := d.store.TransportKey(); ok {
			keyCfg = &p2p.KeyConfig{KeyBytes: stored}
		}
	}
	tcListener, err := p2p.NewListenerWithKey(func(conn net.Conn) {
		defer conn.Close()
		if _, err := d.runSession(conn, false, nil); err != nil {
			d.logger.Warn("inbound sync ended", "err", err)
		}
	}, d.logger, keyCfg)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.tcListener = tcListener
	d.mu.Unlock()
	d.logger.Info("tailcat listener started", "addr", tcListener.Addr())
	// Persist a freshly generated key so the token stays stable across
	// restarts. keyCfg is nil only when no stored key was found yet, in
	// which case the listener generated a fresh one. Ephemeral mode
	// skips the save.
	if !ephemeral && keyCfg == nil {
		if kb, err := tcListener.KeyBytes(); err == nil {
			if err := d.store.PutTransportKey(kb); err != nil {
				d.logger.Warn("persist tailcat key", "err", err)
			}
		} else {
			d.logger.Warn("marshal tailcat key", "err", err)
		}
	}
	return nil
}

// rotateKey replaces the tailcat transport key with a fresh one and
// restarts the listener, changing the node's address token. The old key is
// overwritten in the store. Requires the daemon to be running with a
// transport that is not ephemeral. Returns the new token.
func (d *Daemon) rotateKey() (string, error) {
	d.mu.Lock()
	ephemeral := d.ephemeralKey
	old := d.tcListener
	d.mu.Unlock()
	if ephemeral {
		return "", errors.New("cannot rotate an ephemeral key")
	}
	// Drop the old listener so its key is freed before the replacement
	// binds a fresh one.
	if old != nil {
		old.Close()
	}
	// Clear any persisted key so startTransport generates and persists a
	// fresh one.
	if err := d.store.DeleteTransportKey(); err != nil {
		return "", fmt.Errorf("delete old transport key: %w", err)
	}
	if err := d.startTransport(); err != nil {
		return "", fmt.Errorf("restart transport: %w", err)
	}
	d.mu.Lock()
	newAddr := ""
	if d.tcListener != nil {
		newAddr = string(d.tcListener.Addr())
	}
	d.mu.Unlock()
	d.logger.Info("transport key rotated", "addr", newAddr)
	return newAddr, nil
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
	d.unlocked = nil // drop the signing key on shutdown
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
		return d.handleWhoami(id)
	case "follows":
		return d.handleFollows()
	case "followers":
		return d.handleFollowers()
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
		if p.Text == "" {
			return Response{Error: "text required"}
		}
		kp, err := d.signingKey(p.Passphrase)
		if err != nil {
			return Response{Error: err.Error()}
		}
		return d.handlePost(p.Text, kp)
	case "profile":
		var p struct {
			Name       string `json:"name"`
			Passphrase string `json:"passphrase"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return Response{Error: fmt.Sprintf("parse params: %s", err)}
			}
		}
		if p.Name == "" {
			return Response{Error: "name required"}
		}
		kp, err := d.signingKey(p.Passphrase)
		if err != nil {
			return Response{Error: err.Error()}
		}
		return d.handleProfile(p.Name, kp)
	case "detail":
		var p struct {
			Bio        string `json:"bio"`
			FirstName  string `json:"first_name"`
			LastName   string `json:"last_name"`
			Location   string `json:"location"`
			Passphrase string `json:"passphrase"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return Response{Error: fmt.Sprintf("parse params: %s", err)}
			}
		}
		if p.Bio == "" && p.FirstName == "" && p.LastName == "" && p.Location == "" {
			return Response{Error: "at least one detail field required"}
		}
		kp, err := d.signingKey(p.Passphrase)
		if err != nil {
			return Response{Error: err.Error()}
		}
		return d.handleDetail(p.Bio, p.FirstName, p.LastName, p.Location, kp)
	case "zens":
		d.mu.Lock()
		zens := make([]*zenInfo, 0, len(d.zens))
		for _, p := range d.zens {
			zens = append(zens, p)
		}
		d.mu.Unlock()
		// Enrich each zen with the connected identity's name when a
		// routing binding exists. After a sync binds the token to an
		// identity, the crawl cache holds the authoritative name; that
		// overrides any unverified hint set during discovery. Zens
		// discovered only via exchange keep their hint name/identity.
		for _, z := range zens {
			z.Verified = false
			if id, ok, _ := d.store.RoutingByToken(z.ID); ok {
				z.Identity = string(id)
				if name, _ := d.store.DisplayName(id); name != "" {
					z.Name = name
				}
				if v, _ := d.store.IsVerified(id); v {
					z.Verified = true
				}
			}
		}
		return Response{Result: zens}
	case "verify":
		var p struct {
			Identity string `json:"identity"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return Response{Error: fmt.Sprintf("parse params: %s", err)}
			}
		}
		if p.Identity == "" {
			return Response{Error: "identity required"}
		}
		id, err := core.ParseIdentity(p.Identity)
		if err != nil {
			return Response{Error: err.Error()}
		}
		if err := d.store.VerifyIdentity(id); err != nil {
			return Response{Error: err.Error()}
		}
		d.logger.Info("identity verified out-of-band", "identity", id)
		return Response{Result: map[string]string{"verified": string(id)}}
	case "unverify":
		var p struct {
			Identity string `json:"identity"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return Response{Error: fmt.Sprintf("parse params: %s", err)}
			}
		}
		if p.Identity == "" {
			return Response{Error: "identity required"}
		}
		id, err := core.ParseIdentity(p.Identity)
		if err != nil {
			return Response{Error: err.Error()}
		}
		if err := d.store.UnverifyIdentity(id); err != nil {
			return Response{Error: err.Error()}
		}
		return Response{Result: map[string]string{"unverified": string(id)}}
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
		if p.Target == "" {
			return Response{Error: "target required"}
		}
		kp, err := d.signingKey(p.Passphrase)
		if err != nil {
			return Response{Error: err.Error()}
		}
		return d.handleFollow(p.Target, kp)
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
		if p.Target == "" {
			return Response{Error: "target required"}
		}
		kp, err := d.signingKey(p.Passphrase)
		if err != nil {
			return Response{Error: err.Error()}
		}
		return d.handleUnfollow(p.Target, kp)
	case "unlock":
		var p struct {
			Passphrase string `json:"passphrase"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &p); err != nil {
				return Response{Error: fmt.Sprintf("parse params: %s", err)}
			}
		}
		if p.Passphrase == "" {
			return Response{Error: "passphrase required"}
		}
		return d.handleUnlock(p.Passphrase).resp
	case "lock":
		d.mu.Lock()
		d.unlocked = nil
		d.mu.Unlock()
		d.logger.Info("key locked")
		return Response{Result: map[string]string{"status": "locked"}}
	case "sync":
		d.triggerSyncNow()
		d.logger.Info("sync requested")
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
		unlocked := d.unlocked != nil
		d.mu.Unlock()
		return Response{Result: map[string]any{
			"running":   true,
			"zens":      len(d.zens),
			"socket":    d.socket,
			"transport": tcUp,
			"unlocked":  unlocked,
		}}
	case "rotate-key":
		newAddr, err := d.rotateKey()
		if err != nil {
			return Response{Error: err.Error()}
		}
		return Response{Result: map[string]any{"token": newAddr}}
	default:
		return Response{Error: fmt.Sprintf("unknown method: %s", req.Method)}
	}
}

// feedItem is one post in the merged feed, serialized to the CLI.
type feedItem struct {
	Timestamp int64  `json:"timestamp"`
	Author    string `json:"author"`
	Name      string `json:"name"`
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
	items := make([]feedItem, 0, len(posts))
	for _, p := range posts {
		text := p.Event.Post.Text
		if p.Event.Reply != nil {
			text = "(reply) " + p.Event.Reply.Text
		}
		name, _ := d.store.DisplayName(p.Author)
		items = append(items, feedItem{
			Timestamp: p.Event.Timestamp,
			Author:    p.Author.String(),
			Name:      name,
			Text:      text,
		})
	}
	return Response{Result: items}
}

// signingKey returns the key to sign with. If the caller supplies a
// passphrase, it decrypts the at-rest key (and caches the result so later
// calls can omit the passphrase). Otherwise it returns the cached key, or an
// error if the key is locked. An idle-lock that has elapsed since the last
// signing call clears the key first.
func (d *Daemon) signingKey(passphrase string) (*core.KeyPair, error) {
	if passphrase != "" {
		return d.handleUnlock(passphrase).key()
	}
	d.mu.Lock()
	kp := d.unlocked
	if kp != nil && d.idleLock > 0 && time.Since(d.lastSignAt) > d.idleLock {
		d.unlocked = nil
		kp = nil
	}
	d.mu.Unlock()
	if kp == nil {
		return nil, errors.New("key is locked; run 'driftnode daemon unlock'")
	}
	return kp, nil
}

// handleUnlock decrypts the private key with the passphrase and caches it for
// subsequent keyless signing calls. A wrong passphrase leaves any previously
// unlocked key intact.
func (d *Daemon) handleUnlock(passphrase string) unlockResult {
	ek, err := d.store.EncryptedKey()
	if err != nil {
		return unlockResult{resp: Response{Error: fmt.Sprintf("read key: %s", err)}}
	}
	enc := core.DefaultKeyEncryption()
	priv, err := enc.Decrypt(ek, []byte(passphrase))
	if err != nil {
		return unlockResult{resp: Response{Error: "decrypt key: invalid passphrase"}}
	}
	kp, err := core.KeyPairFromBytes(priv)
	if err != nil {
		return unlockResult{resp: Response{Error: fmt.Sprintf("keypair: %s", err)}}
	}
	d.mu.Lock()
	d.unlocked = kp
	d.lastSignAt = time.Now()
	bootstrapPath := d.bootstrapPath
	bootstrapKey := d.bootstrapVerifyKey
	bootstrapDone := d.bootstrapDone
	d.mu.Unlock()
	d.logger.Info("key unlocked")
	// If the bootstrap auto-follow ran while the key was locked, retry
	// it now that the key is available so seeds enter the follow graph.
	if !bootstrapDone && bootstrapPath != "" {
		go d.dialBootstrapSeeds(bootstrapPath, bootstrapKey)
	}
	return unlockResult{kp: kp, resp: Response{Result: map[string]string{"status": "unlocked", "identity": kp.Identity().String()}}}
}

// unlockResult carries the parsed keypair out of handleUnlock to signingKey
// without re-reading the cache.
type unlockResult struct {
	kp   *core.KeyPair
	resp Response
}

func (u unlockResult) key() (*core.KeyPair, error) {
	if u.kp != nil {
		return u.kp, nil
	}
	return nil, errors.New(u.resp.Error)
}

// handlePost signs a Post event with the given key and appends it to the
// local PostLog.
func (d *Daemon) handlePost(text string, kp *core.KeyPair) Response {
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
	d.touchSignAt()
	d.triggerSyncNow()
	id, _ := se.ID()
	return Response{Result: map[string]string{"event_id": id.String()}}
}

// handleWhoami returns the local identity plus the current profile and
// detail projected from the own logs, so a user can see what they've filled
// in without a separate command.
func (d *Daemon) handleWhoami(id core.Identity) Response {
	out := map[string]any{"identity": id.String()}
	d.mu.Lock()
	if d.tcListener != nil {
		out["token"] = string(d.tcListener.Addr())
	}
	out["stable"] = !d.ephemeralKey
	d.mu.Unlock()
	profEvents, err := d.store.OwnEvents(core.ProfileLog)
	if err == nil {
		if prof := core.NewLog(profEvents).Profile(); prof != nil {
			if prof.DisplayName != "" {
				out["display_name"] = prof.DisplayName
			}
			if prof.AvatarHash != nil && !prof.AvatarHash.IsZero() {
				out["has_avatar"] = true
			}
		}
	}
	detEvents, err := d.store.OwnEvents(core.DetailLog)
	if err == nil {
		if det := core.NewLog(detEvents).Detail(); det != nil {
			if det.Bio != "" {
				out["bio"] = det.Bio
			}
			if det.FirstName != "" {
				out["first_name"] = det.FirstName
			}
			if det.LastName != "" {
				out["last_name"] = det.LastName
			}
			if det.Location != "" {
				out["location"] = det.Location
			}
		}
	}
	return Response{Result: out}
}

// handleFollows returns the identities this zen currently follows, derived
// by replaying its own ProfileLog Follow/Unfollow events (section 7.1).
func (d *Daemon) handleFollows() Response {
	ids, err := d.store.FollowGraph()
	if err != nil {
		return Response{Error: err.Error()}
	}
	out := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		entry := map[string]string{"identity": id.String()}
		if name, err := d.store.DisplayName(id); err == nil && name != "" {
			entry["name"] = name
		}
		out = append(out, entry)
	}
	return Response{Result: map[string]any{"follows": out}}
}

// handleFollowers returns the identities that follow this zen, derived from
// Follow events it has received targeting itself (section 9.3). This is the
// zen's own observed follower set, not a global truth.
func (d *Daemon) handleFollowers() Response {
	ids, err := d.store.ReceivedFollowers()
	if err != nil {
		return Response{Error: err.Error()}
	}
	out := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		entry := map[string]string{"identity": id.String()}
		if name, err := d.store.DisplayName(id); err == nil && name != "" {
			entry["name"] = name
		}
		out = append(out, entry)
	}
	return Response{Result: map[string]any{"followers": out}}
}

// handleProfile signs a Profile event (public: zen name) and appends it to
// the local ProfileLog. The ProfileLog is what the crawler fetches and caches
// durably, so only public-facing fields belong here.
func (d *Daemon) handleProfile(name string, kp *core.KeyPair) Response {
	prof := &core.Profile{DisplayName: name}
	seq, err := d.store.OwnEventCount(core.ProfileLog)
	if err != nil {
		return Response{Error: fmt.Sprintf("sequence: %s", err)}
	}
	se, err := kp.Sign(core.Event{
		Kind:      core.KindProfile,
		Log:       core.ProfileLog,
		Timestamp: core.Now64(),
		Sequence:  seq + 1,
		Profile:   prof,
	})
	if err != nil {
		return Response{Error: fmt.Sprintf("sign: %s", err)}
	}
	if err := d.store.AppendOwnEvent(core.ProfileLog, se); err != nil {
		return Response{Error: fmt.Sprintf("append: %s", err)}
	}
	d.touchSignAt()
	d.triggerSyncNow()
	id, _ := se.ID()
	return Response{Result: map[string]string{"event_id": id.String()}}
}

// handleDetail signs a Detail event (bio, first/last name, location) and
// appends it to the local DetailLog. The DetailLog is never crawled and never
// cached durably by peers; it is fetched on demand for display and
// discarded. The owner's own DetailLog is backup-critical.
func (d *Daemon) handleDetail(bio, firstName, lastName, location string, kp *core.KeyPair) Response {
	seq, err := d.store.OwnEventCount(core.DetailLog)
	if err != nil {
		return Response{Error: fmt.Sprintf("sequence: %s", err)}
	}
	se, err := kp.Sign(core.Event{
		Kind:      core.KindDetail,
		Log:       core.DetailLog,
		Timestamp: core.Now64(),
		Sequence:  seq + 1,
		Detail:    &core.Detail{Bio: bio, FirstName: firstName, LastName: lastName, Location: location},
	})
	if err != nil {
		return Response{Error: fmt.Sprintf("sign: %s", err)}
	}
	if err := d.store.AppendOwnEvent(core.DetailLog, se); err != nil {
		return Response{Error: fmt.Sprintf("append: %s", err)}
	}
	d.touchSignAt()
	d.triggerSyncNow()
	id, _ := se.ID()
	return Response{Result: map[string]string{"event_id": id.String()}}
}

// handleFollow is the single "add a zen" gesture: it records a Follow event
// for the target identity. The target may be a driftnode identity/pubkey or
// a tailcat token. When given a token, it dials the zen, learns the identity
// from the session handshake, writes the Follow event, and binds the token
// to that identity in the routing table. When given a pubkey, it does the
// same when a token for that identity is already known (the common case:
// the zen was discovered via exchange), so the follow connects at once.
// Only when no token is known yet does it fall back to writing the Follow
// event and letting the sync loop resolve the token asynchronously.
func (d *Daemon) handleFollow(targetStr string, kp *core.KeyPair) Response {
	// Identities and raw base32 pubkeys can be parsed unambiguously, so
	// try that first; anything else is treated as a token.
	if target, err := resolvePubkey(targetStr); err == nil {
		return d.followByPubkey(target, kp)
	}
	return d.followByToken(targetStr, kp)
}

// followByPubkey writes a Follow event for the given pubkey. When a token is
// already known for that identity (the zen was discovered via exchange), it
// dials, binds the token to the identity in the routing table, and marks the
// zen connected before returning. When no token is known, it writes the
// Follow event and triggers a sync round so the sync loop can resolve the
// token asynchronously.
func (d *Daemon) followByPubkey(target [32]byte, kp *core.KeyPair) Response {
	identity := core.IdentityFromPubkey(ed25519.PublicKey(target[:]))
	if tok, ok := d.tokenForIdentity(identity); ok {
		return d.followByToken(tok, kp)
	}
	return d.writeFollow(target, kp)
}

// tokenForIdentity returns the token of a known zen whose learned identity
// matches, if any. Used so a follow-by-pubkey can dial a zen that was
// already discovered via exchange without waiting for the sync loop.
func (d *Daemon) tokenForIdentity(identity core.Identity) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for tok, p := range d.zens {
		if core.Identity(p.Identity) == identity {
			return tok, true
		}
	}
	return "", false
}

// followByToken dials a tailcat token, runs the handshake to learn the zen's
// identity, writes a Follow event for it, and binds the token to that
// identity in the routing table.
func (d *Daemon) followByToken(token string, kp *core.KeyPair) Response {
	d.mu.Lock()
	if p, ok := d.zens[token]; ok {
		// Preserve identity/name learned via exchange; just update status.
		p.Status = "connecting"
	} else {
		d.zens[token] = &zenInfo{ID: token, Kind: "native_zen", Status: "connecting"}
	}
	transport := d.transport
	d.mu.Unlock()
	if transport == nil {
		return Response{Error: "no transport configured"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := transport.Dial(ctx, token)
	if err != nil {
		d.setZenStatus(token, "error")
		return Response{Error: fmt.Sprintf("dial: %s", err)}
	}
	defer conn.Close()
	d.setZenStatus(token, "connected")
	var zenID core.Identity
	if _, err := d.runSession(conn, true, &zenID); err != nil {
		d.logger.Warn("follow: session failed", "token", token, "err", err)
		return Response{Error: fmt.Sprintf("session: %s", err)}
	}
	if zenID == "" {
		return Response{Error: "session: zen did not authenticate"}
	}
	pub, err := zenID.PubkeyBytes()
	if err != nil {
		return Response{Error: fmt.Sprintf("zen identity: %s", err)}
	}
	if err := d.store.PutRouting(zenID, token); err != nil {
		return Response{Error: fmt.Sprintf("routing: %s", err)}
	}
	d.mu.Lock()
	d.knownTokens[token] = true
	d.mu.Unlock()
	resp := d.writeFollow([32]byte(pub), kp)
	if resp.Error != "" {
		return resp
	}
	resp.Result = map[string]string{"followed": zenID.String()}
	return resp
}

// writeFollow signs and appends a Follow event for the given target pubkey.
// It does not dial; callers that can resolve a token should dial via
// followByToken so the routing is bound immediately.
func (d *Daemon) writeFollow(target [32]byte, kp *core.KeyPair) Response {
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
	d.touchSignAt()
	d.triggerSyncNow()
	return Response{Result: map[string]string{"followed": core.IdentityFromPubkey(ed25519.PublicKey(target[:])).String()}}
}

// handleUnfollow signs an Unfollow event, removes the routing binding for
// the target, and drops the zen from the zens map. This is the single
// "remove a zen" gesture: unfollowing stops the daemon from dialing the
// identity on future sync rounds.
func (d *Daemon) handleUnfollow(targetStr string, kp *core.KeyPair) Response {
	target, err := resolvePubkey(targetStr)
	if err != nil {
		return Response{Error: fmt.Sprintf("resolve target: %s", err)}
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
	id := core.IdentityFromPubkey(ed25519.PublicKey(target[:]))
	token, hadToken, _ := d.store.Routing(id)
	if err := d.store.DeleteRouting(id); err != nil {
		d.logger.Warn("unfollow: delete routing", "zen", id, "err", err)
	}
	if hadToken {
		d.removeZen(token)
	} else {
		d.removeZen(string(id))
	}
	d.touchSignAt()
	d.triggerSyncNow()
	return Response{Result: map[string]string{"unfollowed": id.String()}}
}

// touchSignAt records that a signing call just happened, for idle-lock
// expiry, and reaps an expired key when idle-lock is configured.
func (d *Daemon) touchSignAt() {
	d.mu.Lock()
	d.lastSignAt = time.Now()
	d.mu.Unlock()
}

// connectAndSync dials a zen by its address token and runs a bidirectional
// sync session (section 7.3): this zen pulls the remote zen's logs, then
// serves its own logs back over the same connection. On success, the
// authenticated zen identity is bound to the token in the routing table so
// future sync rounds can dial it by identity. Zen tokens learned during the
// exchange are recorded for future dials (section 6.1).
func (d *Daemon) connectAndSync(token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	d.mu.Lock()
	transport := d.transport
	d.mu.Unlock()
	if transport == nil {
		d.logger.Warn("zen dial failed: no transport configured", "token", token)
		d.setZenStatus(token, "error")
		return
	}
	conn, err := transport.Dial(ctx, token)
	if err != nil {
		d.logger.Warn("zen dial failed", "token", token, "err", err)
		d.setZenStatus(token, "error")
		return
	}
	defer conn.Close()
	d.setZenStatus(token, "connected")
	var zenID core.Identity
	merged, err := d.runSession(conn, true, &zenID)
	if err != nil {
		d.logger.Warn("sync round failed", "token", token, "err", err)
	}
	if zenID != "" {
		if err := d.store.PutRouting(zenID, token); err != nil {
			d.logger.Warn("routing bind failed", "zen", zenID, "err", err)
		}
	}
	d.logger.Info("sync round done", "token", token, "zen", zenID, "merged", merged)
}

// dialBootstrapSeeds loads a bootstrap.yaml, verifies its signature, and
// dials each seed zen (section 6.1). Called asynchronously from Start.
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
		d.logger.Info("bootstrap verified", "seed_zens", len(bf.SeedZens))
	} else {
		d.logger.Info("bootstrap loaded (no verification key)", "seed_zens", len(bf.SeedZens))
	}
	// Seed the crawler from crawl_seeds (section 9.3). The crawler walks
	// the follow graph, fetching Profile logs only. Crawl seeds can be
	// loaded without the signing key; the fetches will fail until unlock.
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

	d.mu.Lock()
	kp := d.unlocked
	d.mu.Unlock()
	if kp == nil {
		// Key is locked: defer the auto-follow until unlock. Leave
		// bootstrapDone false so handleUnlock retries dialBootstrapSeeds.
		d.logger.Warn("bootstrap: key locked; deferring seed auto-follow")
		return
	}
	for _, sp := range bf.SeedZens {
		if sp.Token == "" {
			continue
		}
		d.mu.Lock()
		already := d.knownTokens[sp.Token]
		d.mu.Unlock()
		if already {
			continue
		}
		d.upsertZen(sp.Token, sp.Kind, "connecting")
		// handleFollow -> followByToken dials, handshakes, writes the
		// Follow event, binds routing, and marks the token known on
		// success. On failure the token stays unmarked so a retry can
		// attempt it again.
		resp := d.handleFollow(sp.Token, kp)
		if resp.Error != "" {
			d.logger.Warn("bootstrap: auto-follow seed failed", "token", sp.Token, "err", resp.Error)
			continue
		}
	}

	// Mark bootstrap complete so handleUnlock doesn't retry it.
	d.mu.Lock()
	d.bootstrapDone = true
	d.mu.Unlock()
}

// knownRefsList returns this node's known zen refs for zen exchange. It
// includes this node's own listener address (with identity and name) so
// zens can dial it back and discover it through the exchange. Each ref
// carries the identity and name when they are known, so the receiver can
// display a discovered zen by name without dialing it.
func (d *Daemon) knownRefsList() []syncproto.ZenRef {
	d.mu.Lock()
	ownID, _, _ := d.store.Identity()
	d.mu.Unlock()
	out := make([]syncproto.ZenRef, 0, len(d.knownTokens)+1)
	if d.tcListener != nil {
		ref := syncproto.ZenRef{Token: string(d.tcListener.Addr())}
		if ownID != "" {
			ref.Identity = ownID
			if name, _ := d.store.DisplayName(ownID); name != "" {
				ref.Name = name
			}
		}
		out = append(out, ref)
	}
	for tok := range d.knownTokens {
		if d.tcListener != nil && tok == string(d.tcListener.Addr()) {
			continue
		}
		ref := syncproto.ZenRef{Token: tok}
		// Prefer the routing binding (authoritative): a followed zen's
		// identity is bound and its name comes from the crawl cache.
		if id, ok, _ := d.store.RoutingByToken(tok); ok {
			ref.Identity = id
			if name, _ := d.store.DisplayName(id); name != "" {
				ref.Name = name
			}
		} else if p, ok := d.zens[tok]; ok {
			// Discovered zen: no routing binding, but the exchange that
			// learned this token may have carried identity/name hints
			// that learnZenRef stored on the zen entry. Forward them so
			// names propagate transitively beyond followed zens.
			ref.Identity = core.Identity(p.Identity)
			ref.Name = p.Name
		}
		out = append(out, ref)
	}
	return out
}

// learnZenRef records a zen ref learned through zen exchange. The token is
// added to knownTokens (for zen-exchange offers and dedup) and the zens map
// (for display), but NOT to seedZens, so it is not auto-dialed. When the ref
// carries an identity, it seeds the crawler so the ProfileLog is fetched
// authoritatively on the next crawl, and the name hint is stored for display
// until the crawl confirms it. Dialing a discovered zen requires explicit
// intent: a follow by token or a follow-resolved dial.
func (d *Daemon) learnZenRef(ref syncproto.ZenRef) {
	if ref.Token == "" {
		return
	}
	d.mu.Lock()
	already := d.knownTokens[ref.Token]
	if !already {
		d.knownTokens[ref.Token] = true
	}
	_, exists := d.zens[ref.Token]
	d.mu.Unlock()
	if !exists {
		d.upsertZen(ref.Token, "native_zen", "discovered")
	}
	if ref.Identity != "" {
		// Seed the crawler so the signed ProfileLog is fetched on the
		// next crawl, confirming the name hint authoritatively.
		d.mu.Lock()
		if d.crawler != nil {
			if id, err := core.ParseIdentity(string(ref.Identity)); err == nil {
				d.crawler.Seed(id)
			}
		}
		d.mu.Unlock()
		// Record the identity on the zen entry so it displays even
		// before the crawl fetches the name. A token can arrive first
		// as a bare token and later pick up an identity hint from
		// another exchange, so update even when the token was already
		// known. Only set the name when the ref carries one, so an
		// identity-only ref does not clobber a name learned earlier.
		d.mu.Lock()
		if p, ok := d.zens[ref.Token]; ok {
			p.Identity = string(ref.Identity)
			if ref.Name != "" {
				p.Name = ref.Name
			}
		}
		d.mu.Unlock()
	}
}

// triggerSyncNow signals the background sync loop to run an immediate sync
// round. It is non-blocking: the channel is buffered with capacity 1, so a
// pending signal is simply dropped if one is already queued.
func (d *Daemon) triggerSyncNow() {
	select {
	case d.syncNow <- struct{}{}:
	default:
	}
}

// syncLoop periodically syncs with all known zens and also fires on demand
// when syncNow is signaled. It also runs the crawler periodically to walk
// the follow graph (section 9.3).
func (d *Daemon) syncLoop(ctx context.Context) {
	const syncInterval = 1 * time.Minute
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
			d.syncAllZens()
			d.runCrawl(ctx)
		case <-syncTicker.C:
			d.syncAllZens()
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

// crawlFetcher implements crawler.Fetcher by dialing known zens to fetch
// Profile logs.
type crawlFetcher struct{ d *Daemon }

func (f *crawlFetcher) FetchProfileLog(ctx context.Context, id core.Identity) ([]core.SignedEvent, error) {
	return f.d.crawlFetch(ctx, id)
}

func (f *crawlFetcher) FetchFollowers(ctx context.Context, id core.Identity) ([]core.SignedEvent, error) {
	return f.d.crawlFetchFollowers(ctx, id)
}

// syncAllZens runs a sync round against every followed identity whose
// routing token is known. The follow graph is the auto-dial set: a zen is
// dialed while it is followed and its token is bound. Unfollowing removes
// the identity from the dial set; a followed identity with no bound token
// (e.g. followed offline by pubkey) is resolved by probing known tokens:
// each is dialed, the handshake reveals the identity, and if it matches a
// pending follow, the token is bound and the identity is synced.
func (d *Daemon) syncAllZens() {
	ids, err := d.store.FollowGraph()
	if err != nil {
		d.logger.Warn("sync: read follow graph", "err", err)
		return
	}
	// Split into bound (have a token) and pending (need resolution).
	var bound []core.Identity
	var pending []core.Identity
	for _, id := range ids {
		_, ok, err := d.store.Routing(id)
		if err != nil {
			d.logger.Warn("sync: read routing", "zen", id, "err", err)
			continue
		}
		if ok {
			bound = append(bound, id)
		} else {
			pending = append(pending, id)
		}
	}
	// Dial bound identities in parallel with a bounded fan-out, so a
	// large follow graph syncs in wall-clock time proportional to
	// syncConcurrency rather than to the number of follows. The shared
	// state each session touches is already safe: daemon state is under
	// d.mu, and bbolt serializes store writes internally.
	d.mu.Lock()
	syncConcurrency := d.syncConcurrency
	d.mu.Unlock()
	if syncConcurrency <= 0 {
		syncConcurrency = 8
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, syncConcurrency)
	for _, id := range bound {
		token, _, _ := d.store.Routing(id)
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			d.connectAndSync(token)
		}()
	}
	wg.Wait()
	// Resolve pending follows by probing known tokens.
	if len(pending) > 0 {
		d.resolvePendingFollows(pending)
	}
}

// resolvePendingFollows dials known tokens to find the routing for followed
// identities that have no bound token. Each token is dialed and a handshake
// reveals the zen's identity; if that identity is in the pending set, the
// token is bound and the identity is synced. Used when a follow was
// recorded by pubkey (offline) and the token is learned later via zen
// exchange or another sync.
//
// Only the handshake runs for non-matching tokens: a full sync session pulls
// the remote zen's PostLog into the local follows cache, so syncing with a
// token that is not a followed identity would leak that zen's posts into the
// feed's source set. Binding the token here makes the confirmed follow
// "bound", and the regular sync loop pulls its logs on the next round.
func (d *Daemon) resolvePendingFollows(pending []core.Identity) {
	pendingSet := make(map[core.Identity]bool, len(pending))
	for _, id := range pending {
		pendingSet[id] = true
	}
	d.mu.Lock()
	kp := d.unlocked
	transport := d.transport
	d.mu.Unlock()
	if kp == nil {
		d.logger.Warn("resolve: key locked; cannot probe pending follows")
		return
	}
	for _, ref := range d.knownRefsList() {
		tok := ref.Token
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		conn, err := transport.Dial(ctx, tok)
		if err != nil {
			cancel()
			continue
		}
		sess := syncproto.NewSession(d.store, d.logger)
		sess.SetKey(kp)
		zenID, err := sess.Handshake(conn, conn)
		conn.Close()
		cancel()
		if err != nil {
			d.logger.Warn("resolve: handshake failed", "token", tok, "err", err)
			continue
		}
		if !pendingSet[zenID] {
			continue
		}
		if err := d.store.PutRouting(zenID, tok); err != nil {
			d.logger.Warn("resolve: routing bind", "zen", zenID, "err", err)
			continue
		}
		// Now that the follow is bound, sync its logs. This is the only
		// sync in this path, and it runs solely for a confirmed follow.
		d.connectAndSync(tok)
	}
}

// upsertZen records a zen in the given state, overwriting any existing entry.
func (d *Daemon) upsertZen(id, kind, status string) {
	d.mu.Lock()
	d.zens[id] = &zenInfo{ID: id, Kind: kind, Status: status}
	d.mu.Unlock()
}

// removeZen removes a zen record.
func (d *Daemon) removeZen(id string) {
	d.mu.Lock()
	delete(d.zens, id)
	d.mu.Unlock()
}

// setZenStatus updates a recorded zen's status.
func (d *Daemon) setZenStatus(id, status string) {
	d.mu.Lock()
	if p, ok := d.zens[id]; ok {
		p.Status = status
	}
	d.mu.Unlock()
}

// crawlFetch fetches a remote identity's ProfileLog by dialing each known
// zen and requesting that identity's Profile log (section 9.3). Returns
// the events from the first zen that has them. Each dial runs the session
// handshake first, so the connection is authenticated before any sync
// traffic.
func (d *Daemon) crawlFetch(ctx context.Context, id core.Identity) ([]core.SignedEvent, error) {
	d.mu.Lock()
	kp := d.unlocked
	transport := d.transport
	d.mu.Unlock()
	if kp == nil {
		return nil, errors.New("key is locked; cannot authenticate crawl session")
	}
	tokens := d.knownRefsList()
	for _, ref := range tokens {
		tok := ref.Token
		conn, err := transport.Dial(ctx, tok)
		if err != nil {
			continue
		}
		sess := syncproto.NewSession(d.store, d.logger)
		sess.SetKey(kp)
		if _, err := sess.Handshake(conn, conn); err != nil {
			d.logger.Warn("crawl: handshake failed", "token", tok, "err", err)
			conn.Close()
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

// crawlFetchFollowers fetches the follower set of the target identity's zen
// by dialing it and issuing MsgFollowers. Unlike crawlFetch, which can ask
// any zen that has replicated a log, the follower set is the followed zen's
// own state, so the request must reach that specific zen. The target's
// bound token is used if known; otherwise known tokens are probed until a
// handshake reveals the target identity.
func (d *Daemon) crawlFetchFollowers(ctx context.Context, id core.Identity) ([]core.SignedEvent, error) {
	d.mu.Lock()
	kp := d.unlocked
	transport := d.transport
	d.mu.Unlock()
	if kp == nil {
		return nil, errors.New("key is locked; cannot authenticate crawl session")
	}
	// Prefer the bound token if one exists.
	if tok, ok, _ := d.store.Routing(id); ok {
		conn, err := transport.Dial(ctx, tok)
		if err == nil {
			events, err := d.fetchFollowersOverConn(ctx, conn, kp)
			if err == nil {
				return events, nil
			}
		}
	}
	// Probe known tokens until a handshake reveals the target identity.
	for _, ref := range d.knownRefsList() {
		tok := ref.Token
		conn, err := transport.Dial(ctx, tok)
		if err != nil {
			continue
		}
		sess := syncproto.NewSession(d.store, d.logger)
		sess.SetKey(kp)
		var zenID core.Identity
		sess.SetAuthed(func(got core.Identity) { zenID = got })
		if _, err := sess.Handshake(conn, conn); err != nil {
			conn.Close()
			continue
		}
		if zenID != id {
			conn.Close()
			continue
		}
		events, err := syncproto.NewClient(d.store, d.logger).FetchFollowers(conn, conn)
		conn.Close()
		if err != nil {
			continue
		}
		return events, nil
	}
	return nil, nil
}

// fetchFollowersOverConn runs a handshake and a FetchFollowers request over
// an already-dialed connection.
func (d *Daemon) fetchFollowersOverConn(ctx context.Context, conn net.Conn, kp *core.KeyPair) ([]core.SignedEvent, error) {
	sess := syncproto.NewSession(d.store, d.logger)
	sess.SetKey(kp)
	if _, err := sess.Handshake(conn, conn); err != nil {
		conn.Close()
		return nil, err
	}
	events, err := syncproto.NewClient(d.store, d.logger).FetchFollowers(conn, conn)
	conn.Close()
	if err != nil {
		return nil, err
	}
	return events, nil
}

// runSession runs a bidirectional sync session over conn. When initiator is
// true, this side opened the connection and pulls first; otherwise the
// remote side opened it and we serve first. The session handshake
// authenticates both sides' Ed25519 identities before any sync traffic;
// zenID (if non-empty) receives the authenticated zen identity. The
// listener must have its signing key unlocked to participate; a locked
// daemon rejects inbound sessions.
func (d *Daemon) runSession(conn net.Conn, initiator bool, zenID *core.Identity) (int, error) {
	sess := syncproto.NewSession(d.store, d.logger)
	sess.SetZenSource(d.knownRefsList)
	sess.SetZenSink(d.learnZenRef)
	sess.SetCursorSource(d.store.SyncCursor)
	sess.SetCursorSink(d.store.PutSyncCursor)
	d.mu.Lock()
	kp := d.unlocked
	d.mu.Unlock()
	if kp == nil {
		return 0, errors.New("key is locked; cannot authenticate session")
	}
	sess.SetKey(kp)
	if zenID != nil {
		sess.SetAuthed(func(id core.Identity) { *zenID = id })
	}
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
// connection. It returns ErrNotRunning if the daemon is not running.
func Dial(socketPath string) (net.Conn, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, ErrNotRunning
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
