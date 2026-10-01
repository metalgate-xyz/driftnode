// Package daemon runs the long-lived driftnode process and exposes a gRPC
// control API over a Unix domain socket. The CLI and TUI are clients.
//
//go:generate ../../scripts/gen-proto.sh
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
	"driftnode/internal/proto/driftnodepb"
	"driftnode/internal/store"
	syncproto "driftnode/internal/sync"

	"github.com/tailscale/tailcat"
	"google.golang.org/grpc"
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

	// grpcServer serves the typed control API over the Unix socket. nil
	// before Start and after Stop. Guarded by mu.
	grpcServer *grpc.Server

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

	// subscribers are open subscribe connections. Each holds a buffered
	// queue of push events. The daemon broadcasts state changes (zens
	// upsert/remove, follow/follower add/remove, feed add, status) to all
	// subscribers so the TUI never polls and never rebuilds from scratch.
	subs []*subscriber

	// feedCache holds the merged timeline sorted newest-first, rebuilt only
	// when feedGen changes (a post is created or synced). Without it every
	// subscribe snapshot reloads and re-sorts the full PostLog from disk,
	// which is seconds for a large feed. feedMu guards the cache and gen.
	feedMu    sync.Mutex
	feedGen   uint64
	feedCache []core.SignedEvent

	// nameCache memoizes DisplayName per identity so the snapshot and feed
	// diffs do not each re-run a bbolt projection per item. Invalidated on
	// profile writes. Guarded by feedMu.
	nameCache map[core.Identity]string
}

// zenInfo describes a known zen and its connection state.
type zenInfo struct {
	ID       string `json:"id"`       // tailcat token of the zen
	Identity string `json:"identity"` // driftnode:<pubkey> once learned, else empty
	Name     string `json:"name"`     // zen name once known, else empty
	Kind     string `json:"kind"`     // native_zen or browser
	Status   string `json:"status"`   // connecting, connected, discovered, error
	Verified bool   `json:"verified"` // identity confirmed out-of-band (§7)
	Pinned   bool   `json:"pinned"`   // follow pinned for top sync priority
}

// subscriber is one open subscribe stream. The daemon pushes typed events
// onto events; if events fills (slow TUI), done is closed to drop the
// subscriber, and its pump exits so the TUI reconnects.
type subscriber struct {
	events chan *driftnodepb.Event
	done   chan struct{}
}

// subscribeQueue bounds how many push events a subscriber queues before the
// daemon drops it. A bounded buffer keeps the daemon from blocking on a slow
// TUI while still protecting against a truly stuck client.
const subscribeQueue = 256

// gracefulStopTimeout bounds how long Stop waits for in-flight RPCs to
// finish cleanly before force-cancelling them. A healthy shutdown flushes
// in microseconds; the bound only fires when a stuck client (a Subscribe
// pump blocked in srv.Send on a full flow-control window) would otherwise
// hang teardown.
const gracefulStopTimeout = 5 * time.Second

// newSubscriber constructs a subscriber with the standard buffer size.
func newSubscriber() *subscriber {
	return &subscriber{
		events: make(chan *driftnodepb.Event, subscribeQueue),
		done:   make(chan struct{}),
	}
}

// broadcast sends ev to every subscriber without blocking the daemon. A
// subscriber whose queue is full is dropped (its done channel is closed so
// its pump exits and the stream is torn down).
func (d *Daemon) broadcast(ev *driftnodepb.Event) {
	d.mu.Lock()
	subs := make([]*subscriber, 0, len(d.subs))
	for _, s := range d.subs {
		subs = append(subs, s)
	}
	d.mu.Unlock()
	for _, s := range subs {
		select {
		case s.events <- ev:
		default:
			// Queue full: drop the slow subscriber.
			select {
			case <-s.done:
			default:
				close(s.done)
			}
		}
	}
}

// emitZenUpsert pushes a zens upsert for one zen, enriching it with the
// authoritative name/identity/verified state from the crawl cache. The
// caller must pass a value copied under d.mu so a concurrent setZenStatus
// cannot race the enrichment reads.
func (d *Daemon) emitZenUpsert(z zenInfo) {
	d.broadcast(d.zenDiffEvent(z))
}

// zenDiffEvent builds a zens upsert event for one zen, enriched with the
// name/identity/verified state bound to its token. The enrichment mirrors
// the zens RPC so a push carries the same fields a poll would.
func (d *Daemon) zenDiffEvent(z zenInfo) *driftnodepb.Event {
	zen := d.zenToProto(z)
	return &driftnodepb.Event{Kind: &driftnodepb.Event_ZenUpsert{ZenUpsert: &driftnodepb.ZenUpsert{Zen: zen}}}
}

// emitZenRemove pushes a zens removal by token.
func (d *Daemon) emitZenRemove(id string) {
	d.broadcast(&driftnodepb.Event{Kind: &driftnodepb.Event_ZenRemove{ZenRemove: &driftnodepb.ZenRemove{Id: id}}})
}

// emitFollowsAdd pushes a follows addition.
func (d *Daemon) emitFollowsAdd(entry map[string]string) {
	ident := identityEntryToProto(entry)
	d.broadcast(&driftnodepb.Event{Kind: &driftnodepb.Event_FollowsDiff{FollowsDiff: &driftnodepb.FollowsDiff{Add: []*driftnodepb.Identity{ident}}}})
}

// emitFollowsRemove pushes a follows removal by identity.
func (d *Daemon) emitFollowsRemove(identity string) {
	d.broadcast(&driftnodepb.Event{Kind: &driftnodepb.Event_FollowsDiff{FollowsDiff: &driftnodepb.FollowsDiff{Remove: identity}}})
}

// emitFollowersAdd pushes a followers addition.
func (d *Daemon) emitFollowersAdd(entry map[string]string) {
	ident := identityEntryToProto(entry)
	d.broadcast(&driftnodepb.Event{Kind: &driftnodepb.Event_FollowersDiff{FollowersDiff: &driftnodepb.FollowersDiff{Add: []*driftnodepb.Identity{ident}}}})
}

// emitFeedAdd pushes a feed addition (newly merged posts, newest-first).
func (d *Daemon) emitFeedAdd(items []feedItem) {
	if len(items) == 0 {
		return
	}
	add := make([]*driftnodepb.FeedItem, 0, len(items))
	for _, it := range items {
		add = append(add, feedItemToProto(it))
	}
	d.broadcast(&driftnodepb.Event{Kind: &driftnodepb.Event_FeedDiff{FeedDiff: &driftnodepb.FeedDiff{Add: add}}})
}

// emitStatus pushes a status snapshot.
func (d *Daemon) emitStatus() {
	d.mu.Lock()
	tcUp := d.tcListener != nil
	unlocked := d.unlocked != nil
	zenCount := len(d.zens)
	d.mu.Unlock()
	d.broadcast(&driftnodepb.Event{Kind: &driftnodepb.Event_StatusDiff{StatusDiff: &driftnodepb.StatusDiff{
		Running:   true,
		Zens:      int32(zenCount),
		Transport: tcUp,
		Unlocked:  unlocked,
	}}})
}

// addSubscriber registers a subscriber for push events.
func (d *Daemon) addSubscriber(s *subscriber) {
	d.mu.Lock()
	d.subs = append(d.subs, s)
	d.mu.Unlock()
}

// removeSubscriber unregisters a subscriber.
func (d *Daemon) removeSubscriber(s *subscriber) {
	d.mu.Lock()
	for i, sub := range d.subs {
		if sub == s {
			d.subs = append(d.subs[:i], d.subs[i+1:]...)
			break
		}
	}
	d.mu.Unlock()
}

// populateLikes computes the liker set for each feed item from Like events
// held locally (own PostLog plus synced PostLogs of followed identities).
// Likes are observational: the count is "how many Like events this zen has
// observed," never a global truth (section 7.2).
func (d *Daemon) populateLikes(items []feedItem) {
	if len(items) == 0 {
		return
	}
	ids := make([]core.EventID, len(items))
	idToIdx := make(map[core.EventID]int, len(items))
	for i, it := range items {
		id, err := core.ParseEventID(it.ID)
		if err != nil {
			continue
		}
		ids[i] = id
		idToIdx[id] = i
	}
	likes, err := d.store.LikesForPosts(ids)
	if err != nil {
		return
	}
	for _, se := range likes {
		if se.Event.Like == nil {
			continue
		}
		idx, ok := idToIdx[se.Event.Like.TargetID]
		if !ok {
			continue
		}
		items[idx].LikeCount++
		likerName := d.displayName(se.Author)
		if likerName == "" {
			likerName = se.Author.String()
		}
		items[idx].Likers = append(items[idx].Likers, likerName)
	}
}

// cachedPosts returns the merged timeline sorted newest-first, rebuilding it
// from disk only when the feed generation has changed since the last build.
// A large feed (millions of posts) otherwise reloads and re-sorts on every
// subscribe snapshot, freezing the TUI for seconds.
func (d *Daemon) cachedPosts() []core.SignedEvent {
	d.feedMu.Lock()
	gen := d.feedGen
	cached := d.feedCache
	d.feedMu.Unlock()
	if cached != nil {
		return cached
	}
	// Cache miss: rebuild. feedGen is bumped on any post mutation, so a
	// concurrent bump is caught on the next call (we rebuild under the lock
	// and store the result for the current gen).
	d.feedMu.Lock()
	defer d.feedMu.Unlock()
	if d.feedCache != nil && d.feedGen == gen {
		return d.feedCache
	}
	allPosts, err := d.store.AllPostsOneTx()
	if err != nil {
		d.logger.Info("cachedPosts AllPostsOneTx error", "err", err)
		return nil
	}
	posts := core.NewLog(allPosts).Posts()
	d.feedCache = posts
	return posts
}

// invalidateFeed bumps the feed generation so the next cachedPosts call
// rebuilds from disk. Called when a post is created or synced.
func (d *Daemon) invalidateFeed() {
	d.feedMu.Lock()
	d.feedGen++
	d.feedCache = nil
	d.feedMu.Unlock()
}

// invalidateNames drops the name cache so the next DisplayName lookup
// re-reads from disk. Called when a profile is written.
func (d *Daemon) invalidateNames() {
	d.feedMu.Lock()
	d.nameCache = make(map[core.Identity]string)
	d.feedMu.Unlock()
}

// displayName returns the cached display name for an identity, populating
// the cache on first access. The per-identity projection over the
// ProfileLog is expensive at scale (one bbolt scan per item), so memoizing
// keeps the snapshot and feed diffs from re-running it per row.
func (d *Daemon) displayName(id core.Identity) string {
	d.feedMu.Lock()
	if d.nameCache == nil {
		d.nameCache = make(map[core.Identity]string)
	}
	if name, ok := d.nameCache[id]; ok {
		d.feedMu.Unlock()
		return name
	}
	d.feedMu.Unlock()
	name, _ := d.store.DisplayName(id)
	d.feedMu.Lock()
	d.nameCache[id] = name
	d.feedMu.Unlock()
	return name
}

// followEntries returns the identities this zen follows, with display names,
// for the subscribe snapshot.
func (d *Daemon) followEntries() []map[string]string {
	ids, err := d.store.FollowGraph()
	if err != nil {
		return nil
	}
	out := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		entry := map[string]string{"identity": id.String()}
		if name := d.displayName(id); name != "" {
			entry["name"] = name
		}
		out = append(out, entry)
	}
	return out
}

// followerEntries returns the identities that follow this zen, with display
// names, for the subscribe snapshot.
func (d *Daemon) followerEntries() []map[string]string {
	ids, err := d.store.ReceivedFollowers()
	if err != nil {
		return nil
	}
	out := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		entry := map[string]string{"identity": id.String()}
		if name := d.displayName(id); name != "" {
			entry["name"] = name
		}
		out = append(out, entry)
	}
	return out
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
		nameCache:   make(map[core.Identity]string),
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
	// Serve the control API as gRPC over the Unix socket. The typed
	// Driftnode service replaces the former JSON-lines request/response
	// and push protocol; the CLI and TUI are gRPC clients.
	grpcSrv := grpc.NewServer()
	RegisterServer(grpcSrv, d)
	d.mu.Lock()
	d.listener = l
	d.grpcServer = grpcSrv
	d.socket = socketPath
	d.mu.Unlock()
	d.logger.Info("daemon started", "socket", socketPath)
	// Pre-warm the feed cache in the background so the first subscribe
	// snapshot does not stall on loading and sorting the full PostLog.
	go d.cachedPosts()
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
	} else {
		d.emitStatus()
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
		if p, _ := d.store.IsPinned(id); p {
			d.zens[tok].Pinned = true
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
//
// Teardown ordering matters: subscribers are closed first so the Subscribe
// pumps exit and their streams complete; then GracefulStop waits for in-flight
// RPCs (the Stop ack flush plus the just-ended subscribe streams) before
// tearing down transports. GracefulStop is bounded so a stuck client cannot
// hang shutdown: if it has not returned by the deadline, a hard Stop cancels
// the remaining streams. The blocking teardown runs outside d.mu so RPC
// handlers that need the lock are not deadlocked against shutdown.
func (d *Daemon) Stop() {
	d.mu.Lock()
	srv := d.grpcServer
	d.grpcServer = nil
	listener := d.listener
	d.listener = nil
	tc := d.tcListener
	d.tcListener = nil
	syncCancel := d.syncCancel
	d.syncCancel = nil
	subs := d.subs
	d.subs = nil
	socket := d.socket
	d.socket = ""
	d.unlocked = nil
	d.mu.Unlock()

	// 1. Close subscribers so Subscribe pumps parked in their select exit
	// and their streams complete. Pumps blocked in srv.Send (slow client
	// with a full flow-control window) are handled by the deadline below.
	for _, s := range subs {
		select {
		case <-s.done:
		default:
			close(s.done)
		}
	}

	// 2. GracefulStop waits for in-flight RPCs to finish (the Stop ack is
	// flushed, subscribe streams that just exited complete) before tearing
	// down transports. A stuck stream gets force-stopped after the deadline.
	if srv != nil {
		stopped := make(chan struct{})
		go func() { srv.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(gracefulStopTimeout):
			srv.Stop()
		}
	}

	// 3. Close listeners and finish cleanup.
	if listener != nil {
		listener.Close()
	}
	if tc != nil {
		tc.Close()
	}
	if syncCancel != nil {
		syncCancel()
	}
	if socket != "" {
		os.Remove(socket)
	}
	// Signal Start's Wait (or the foreground blocker) to return. Guard so
	// a double Stop is a no-op for the channel close.
	select {
	case <-d.done:
	default:
		close(d.done)
	}
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
	d.mu.Lock()
	listener := d.listener
	srv := d.grpcServer
	d.mu.Unlock()
	if listener == nil || srv == nil {
		return
	}
	// Serve blocks until Stop closes the listener or stops the server.
	_ = srv.Serve(listener)
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

// snapshot builds the initial subscribe event carrying the full state of all
// four panels, so the TUI seeds everything from one message. It builds fresh
// proto values and never mutates the live zens map.
func (d *Daemon) snapshot() *driftnodepb.Snapshot {
	d.mu.Lock()
	src := make([]zenInfo, 0, len(d.zens))
	for _, z := range d.zens {
		src = append(src, *z)
	}
	tcUp := d.tcListener != nil
	unlocked := d.unlocked != nil
	d.mu.Unlock()
	zens := make([]*driftnodepb.Zen, 0, len(src))
	for i := range src {
		zens = append(zens, d.zenToProto(src[i]))
	}
	feed, _, _ := d.handleFeedPage(0, false, false)
	follows := d.followEntries()
	followers := d.followerEntries()
	return &driftnodepb.Snapshot{
		Feed:      feedItemsToProto(feed),
		Zens:      zens,
		Follows:   identityEntriesToProto(follows),
		Followers: identityEntriesToProto(followers),
		Status: &driftnodepb.StatusDiff{
			Running:   true,
			Zens:      int32(len(zens)),
			Transport: tcUp,
			Unlocked:  unlocked,
		},
	}
}

// snapshotProto is an alias for snapshot so the gRPC Subscribe handler
// reads as a typed builder.
func (d *Daemon) snapshotProto() *driftnodepb.Snapshot { return d.snapshot() }

// zenToProto copies one zenInfo into a fresh proto Zen, enriching it with
// the name/identity/verified/pinned state bound to its token. It takes the
// zenInfo by value so the caller can copy it under d.mu and then enrich
// without the lock, avoiding a race on fields a concurrent setZenStatus may
// mutate.
func (d *Daemon) zenToProto(z zenInfo) *driftnodepb.Zen {
	out := zenToProtoPlain(z)
	out.Verified = false
	out.Pinned = false
	if id, ok, _ := d.store.RoutingByToken(z.ID); ok {
		out.Identity = string(id)
		if name, _ := d.store.DisplayName(id); name != "" {
			out.Name = name
		}
		if v, _ := d.store.IsVerified(id); v {
			out.Verified = true
		}
		if p, _ := d.store.IsPinned(id); p {
			out.Pinned = true
		}
	}
	return out
}

// zenToProtoPlain copies a zenInfo value into a proto Zen with no enrichment.
func zenToProtoPlain(z zenInfo) *driftnodepb.Zen {
	return &driftnodepb.Zen{
		Id:       z.ID,
		Identity: z.Identity,
		Name:     z.Name,
		Kind:     z.Kind,
		Status:   z.Status,
		Verified: z.Verified,
		Pinned:   z.Pinned,
	}
}

// feedItemToProto converts one feedItem to a proto FeedItem.
func feedItemToProto(it feedItem) *driftnodepb.FeedItem {
	return &driftnodepb.FeedItem{
		Id:        it.ID,
		Timestamp: it.Timestamp,
		Author:    it.Author,
		Name:      it.Name,
		Text:      it.Text,
		LikeCount: int32(it.LikeCount),
		Likers:    it.Likers,
	}
}

// feedItemsToProto converts a feedItem slice to proto.
func feedItemsToProto(items []feedItem) []*driftnodepb.FeedItem {
	out := make([]*driftnodepb.FeedItem, 0, len(items))
	for _, it := range items {
		out = append(out, feedItemToProto(it))
	}
	return out
}

// identityEntryToProto converts one map[string]string identity row to proto.
func identityEntryToProto(r map[string]string) *driftnodepb.Identity {
	return &driftnodepb.Identity{Identity: r["identity"], Name: r["name"]}
}

// identityEntriesToProto converts a map[string]string identity slice to proto.
func identityEntriesToProto(rows []map[string]string) []*driftnodepb.Identity {
	out := make([]*driftnodepb.Identity, 0, len(rows))
	for _, r := range rows {
		out = append(out, identityEntryToProto(r))
	}
	return out
}

// addSubscriberProto registers a typed subscriber and returns it.
func (d *Daemon) addSubscriberProto() *subscriber {
	sub := newSubscriber()
	d.addSubscriber(sub)
	return sub
}

// feedItem is one post in the merged feed, serialized to the CLI.
type feedItem struct {
	ID        string   `json:"id"`
	Timestamp int64    `json:"timestamp"`
	Author    string   `json:"author"`
	Name      string   `json:"name"`
	Text      string   `json:"text"`
	LikeCount int      `json:"like_count"`
	Likers    []string `json:"likers"`
}

// feedPageLimit is the fixed page size shared by the subscribe snapshot and
// FeedPage. The snapshot is page 0; FeedPage serves page N.
const feedPageLimit = 200

// handleFeedPage returns one page of the merged timeline, newest-first. Page
// 0 is the snapshot. hasMore reports whether older posts remain. When
// allLikes is set, the daemon fetches likes from connected zens for the
// user's own posts in the page before populating like counts.
func (d *Daemon) handleFeedPage(page int, mine, allLikes bool) ([]feedItem, bool, error) {
	posts := d.cachedPosts()
	ownID, _, _ := d.store.Identity()
	start := page * feedPageLimit
	end := start + feedPageLimit

	// cachedPosts is already newest-first, so a page is a slice of the
	// filtered view. Filtering mine once per call is O(n); the unfiltered
	// path is O(1) slicing. Re-scanning from index 0 each page would be
	// O(n^2) over a full fetch and hang the CLI on a large feed.
	var view []core.SignedEvent
	if mine {
		view = make([]core.SignedEvent, 0, len(posts))
		for _, p := range posts {
			if p.Author == ownID {
				view = append(view, p)
			}
		}
	} else {
		view = posts
	}

	if start >= len(view) {
		return nil, false, nil
	}
	if end > len(view) {
		end = len(view)
	}
	items := make([]feedItem, 0, end-start)
	for _, p := range view[start:end] {
		items = append(items, d.feedItemFor(p))
	}
	if allLikes {
		ids := make([]core.EventID, 0, len(items))
		for _, it := range items {
			if it.Author == ownID.String() {
				if id, err := core.ParseEventID(it.ID); err == nil {
					ids = append(ids, id)
				}
			}
		}
		if len(ids) > 0 {
			_, _ = d.handleFetchLikes(ids)
		}
	}
	d.populateLikes(items)
	return items, end < len(view), nil
}

// feedItemFor builds a feedItem from one signed post event. A post with a
// non-zero ParentID is a reply; the feed prefixes it so replies are visually
// distinguishable from top-level posts.
func (d *Daemon) feedItemFor(p core.SignedEvent) feedItem {
	text := p.Event.Post.Text
	if p.Event.Post != nil && !p.Event.Post.ParentID.IsZero() {
		text = "(reply) " + text
	}
	name := d.displayName(p.Author)
	id, _ := p.ID()
	return feedItem{
		ID:        id.String(),
		Timestamp: p.Event.Timestamp,
		Author:    p.Author.String(),
		Name:      name,
		Text:      text,
	}
}

// feedItemsFor builds feedItems for a batch of merged post/reply events,
// used to push a feed diff after a sync round.
func (d *Daemon) feedItemsFor(posts []core.SignedEvent) []feedItem {
	items := make([]feedItem, 0, len(posts))
	for _, p := range posts {
		items = append(items, d.feedItemFor(p))
	}
	return items
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
	d.emitStatus()
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
// local PostLog. When parentID is non-empty, the post is a reply to the
// referenced event.
func (d *Daemon) handlePost(text, parentID string, kp *core.KeyPair) Response {
	var parent core.EventID
	if parentID != "" {
		p, err := core.ParseEventID(parentID)
		if err != nil {
			return Response{Error: fmt.Sprintf("parent id: %s", err)}
		}
		parent = p
	}
	post := &core.Post{Text: text}
	if !parent.IsZero() {
		post.ParentID = parent
	}
	se, id, err := d.store.SignAndAppend(kp, core.PostLog, core.Event{
		Kind: core.KindPost,
		Post: post,
	})
	if err != nil {
		return Response{Error: err.Error()}
	}
	d.invalidateFeed()
	d.touchSignAt()
	d.triggerSyncNow()
	ownID, _, _ := d.store.Identity()
	name := d.displayName(ownID)
	d.emitFeedAdd([]feedItem{{
		ID:        id.String(),
		Timestamp: se.Event.Timestamp,
		Author:    ownID.String(),
		Name:      name,
		Text:      text,
	}})
	return Response{Result: map[string]string{"event_id": id.String()}}
}

// handleLike signs a Like event targeting the given post event ID and
// appends it to the local PostLog. The like is recorded in the liker's own
// log, never the target's (section 7.2): nobody can write into someone
// else's log without a valid signature from that identity's key.
func (d *Daemon) handleLike(postID string, kp *core.KeyPair) Response {
	target, err := core.ParseEventID(postID)
	if err != nil {
		return Response{Error: fmt.Sprintf("post id: %s", err)}
	}
	if target.IsZero() {
		return Response{Error: "post id required"}
	}
	_, id, err := d.store.SignAndAppend(kp, core.PostLog, core.Event{
		Kind: core.KindLike,
		Like: &core.Like{TargetID: target},
	})
	if err != nil {
		return Response{Error: err.Error()}
	}
	d.invalidateFeed()
	d.touchSignAt()
	d.triggerSyncNow()
	return Response{Result: map[string]string{"event_id": id.String()}}
}

// handleFetchLikes dials each known zen and requests Like events targeting
// the given post IDs. Best-effort: only zens currently reachable respond,
// and each returns only what it happens to hold. Received like events are
// stored as crawled events (merged into the feed's source set by dedup).
// Returns all newly-received like events.
func (d *Daemon) handleFetchLikes(targetIDs []core.EventID) ([]core.SignedEvent, error) {
	d.mu.Lock()
	transport := d.transport
	kp := d.unlocked
	d.mu.Unlock()
	refs := d.knownRefsList()
	if transport == nil {
		return nil, errors.New("no transport configured")
	}
	if kp == nil {
		return nil, errors.New("key is locked; run 'driftnode daemon unlock'")
	}
	type result struct {
		events []core.SignedEvent
		err    error
	}
	results := make(chan result, len(refs))
	for _, ref := range refs {
		go func(ref syncproto.ZenRef) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			conn, err := transport.Dial(ctx, ref.Token)
			if err != nil {
				results <- result{err: err}
				return
			}
			defer conn.Close()
			sess := syncproto.NewSession(d.store, d.logger)
			sess.SetKey(kp)
			if _, err := sess.Handshake(conn, conn); err != nil {
				results <- result{err: err}
				return
			}
			cl := syncproto.NewClient(d.store, d.logger)
			likes, err := cl.FetchLikes(conn, conn, targetIDs)
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{events: likes}
		}(ref)
	}
	var all []core.SignedEvent
	for range refs {
		r := <-results
		if r.err != nil {
			d.logger.Debug("fetch likes: zen error", "err", r.err)
			continue
		}
		for _, se := range r.events {
			inserted, err := d.store.PutCrawledEvent(&se, uint64(se.Event.Sequence))
			if err != nil {
				d.logger.Warn("fetch likes: store", "err", err)
				continue
			}
			if inserted {
				all = append(all, se)
			}
		}
	}
	if len(all) > 0 {
		d.invalidateFeed()
	}
	return all, nil
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
	_, id, err := d.store.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:    core.KindProfile,
		Profile: &core.Profile{DisplayName: name},
	})
	if err != nil {
		return Response{Error: err.Error()}
	}
	d.invalidateNames()
	d.touchSignAt()
	d.triggerSyncNow()
	return Response{Result: map[string]string{"event_id": id.String()}}
}

// handleDetail signs a Detail event (bio, first/last name, location) and
// appends it to the local DetailLog. The DetailLog is never crawled and never
// cached durably by peers; it is fetched on demand for display and
// discarded. The owner's own DetailLog is backup-critical.
func (d *Daemon) handleDetail(bio, firstName, lastName, location string, kp *core.KeyPair) Response {
	_, id, err := d.store.SignAndAppend(kp, core.DetailLog, core.Event{
		Kind:   core.KindDetail,
		Detail: &core.Detail{Bio: bio, FirstName: firstName, LastName: lastName, Location: location},
	})
	if err != nil {
		return Response{Error: err.Error()}
	}
	d.touchSignAt()
	d.triggerSyncNow()
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
	if target, err := core.ResolvePubkey(targetStr); err == nil {
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
	if _, _, err := d.store.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:   core.KindFollow,
		Follow: &core.Follow{TargetPubkey: target},
	}); err != nil {
		return Response{Error: err.Error()}
	}
	followedID := core.IdentityFromPubkey(ed25519.PublicKey(target[:]))
	d.touchSignAt()
	d.triggerSyncNow()
	name, _ := d.store.DisplayName(followedID)
	d.emitFollowsAdd(map[string]string{"identity": followedID.String(), "name": name})
	return Response{Result: map[string]string{"followed": followedID.String()}}
}

// handleUnfollow signs an Unfollow event, removes the routing binding for
// the target, and drops the zen from the zens map. This is the single
// "remove a zen" gesture: unfollowing stops the daemon from dialing the
// identity on future sync rounds. The shared store.Unfollow clears the pin
// on the target so the clear-on-unfollow invariant is defined once.
func (d *Daemon) handleUnfollow(targetStr string, kp *core.KeyPair) Response {
	target, err := core.ResolvePubkey(targetStr)
	if err != nil {
		return Response{Error: fmt.Sprintf("resolve target: %s", err)}
	}
	_, _, id, err := d.store.Unfollow(kp, target)
	if err != nil {
		return Response{Error: err.Error()}
	}
	token, hadToken, _ := d.store.Routing(id)
	if err := d.store.DeleteRouting(id); err != nil {
		d.logger.Warn("unfollow: delete routing", "zen", id, "err", err)
	}
	if hadToken {
		d.removeZen(token)
	} else {
		d.removeZen(string(id))
	}
	d.emitFollowsRemove(id.String())
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
	z := zenInfo{ID: id, Kind: kind, Status: status}
	d.mu.Lock()
	d.zens[id] = &z
	snap := z
	d.mu.Unlock()
	d.emitZenUpsert(snap)
}

// removeZen removes a zen record.
func (d *Daemon) removeZen(id string) {
	d.mu.Lock()
	delete(d.zens, id)
	d.mu.Unlock()
	d.emitZenRemove(id)
}

// setZenStatus updates a recorded zen's status.
func (d *Daemon) setZenStatus(id, status string) {
	d.mu.Lock()
	p, ok := d.zens[id]
	var snap zenInfo
	if ok {
		p.Status = status
		snap = *p
	}
	d.mu.Unlock()
	if ok {
		d.emitZenUpsert(snap)
	}
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
	// Collect Post/Reply events and inbound Follows merged during this
	// session so the daemon can push feed and follower diffs to subscribers
	// once the round completes.
	var mergedPosts []core.SignedEvent
	var newFollowers []core.Identity
	ownID, _, _ := d.store.Identity()
	var ownPub [32]byte
	if ownID != "" {
		if pub, err := ownID.PubkeyBytes(); err == nil {
			copy(ownPub[:], pub)
		}
	}
	sess.SetOnMerge(func(se *core.SignedEvent, inserted bool) {
		if !inserted {
			return
		}
		switch se.Event.Log {
		case core.PostLog:
			if se.Event.Kind != core.KindPost {
				return
			}
			mergedPosts = append(mergedPosts, *se)
		case core.ProfileLog:
			if se.Event.Kind == core.KindFollow && se.Event.Follow != nil &&
				ownPub != [32]byte{} && se.Event.Follow.TargetPubkey == ownPub {
				newFollowers = append(newFollowers, se.Author)
			}
		}
	})
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
	var n int
	var err error
	if initiator {
		n, err = sess.RunInitiator(conn, conn)
	} else {
		n, err = sess.RunListener(conn, conn)
	}
	if len(mergedPosts) > 0 {
		d.invalidateFeed()
		d.emitFeedAdd(d.feedItemsFor(mergedPosts))
	}
	for _, fid := range newFollowers {
		name := d.displayName(fid)
		d.emitFollowersAdd(map[string]string{"identity": fid.String(), "name": name})
	}
	return n, err
}

// Dial probes the daemon's control socket and returns ErrNotRunning if no
// daemon is listening. The socket now speaks gRPC; callers that need to talk
// to the daemon should use DialClient for a typed client.
func Dial(socketPath string) (net.Conn, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, ErrNotRunning
	}
	return conn, nil
}

// SendRequest is a test convenience that dials the gRPC daemon and maps a
// legacy method name to a typed RPC, returning a Response shaped like the old
// JSON-lines protocol so tests can drive the daemon with map[string]any
// params. Production callers use DialClient and the typed client directly.
func SendRequest(socketPath, method string, params map[string]any) (*Response, error) {
	cl, cc, err := DialClient(socketPath)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	ctx := context.Background()
	switch method {
	case "whoami":
		r, err := cl.Whoami(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"identity": r.Identity, "token": r.Token, "stable": r.Stable, "display_name": r.DisplayName, "has_avatar": r.HasAvatar, "bio": r.Bio, "first_name": r.FirstName, "last_name": r.LastName, "location": r.Location}}, nil
	case "follows":
		r, err := cl.Follows(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"follows": identitiesFromProto(r.Identities)}}, nil
	case "followers":
		r, err := cl.Followers(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"followers": identitiesFromProto(r.Identities)}}, nil
	case "feed":
		mine := false
		if v, ok := params["mine"]; ok {
			if b, ok := v.(bool); ok {
				mine = b
			}
		}
		r, err := cl.FeedPage(ctx, &driftnodepb.FeedPageReq{Page: 0, Mine: mine})
		if err != nil {
			return nil, err
		}
		return &Response{Result: feedItemsFromProto(r.Items)}, nil
	case "post":
		r, err := cl.Post(ctx, &driftnodepb.PostReq{Text: paramString(params, "text"), Passphrase: paramString(params, "passphrase")})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"event_id": r.EventId}}, nil
	case "profile":
		if _, err := cl.Profile(ctx, &driftnodepb.ProfileReq{Name: paramString(params, "name"), Passphrase: paramString(params, "passphrase")}); err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"status": "ok"}}, nil
	case "detail":
		if _, err := cl.Detail(ctx, &driftnodepb.DetailReq{Bio: paramString(params, "bio"), FirstName: paramString(params, "first_name"), LastName: paramString(params, "last_name"), Location: paramString(params, "location"), Passphrase: paramString(params, "passphrase")}); err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"status": "ok"}}, nil
	case "zens":
		r, err := cl.Zens(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: zensFromProto(r.Zens)}, nil
	case "verify":
		r, err := cl.Verify(ctx, &driftnodepb.IdentityReq{Identity: paramString(params, "identity")})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"verified": r.Identity}}, nil
	case "unverify":
		r, err := cl.Unverify(ctx, &driftnodepb.IdentityReq{Identity: paramString(params, "identity")})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"unverified": r.Identity}}, nil
	case "follow":
		r, err := cl.Follow(ctx, &driftnodepb.FollowReq{Target: paramString(params, "target"), Passphrase: paramString(params, "passphrase")})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"followed": r.Followed}}, nil
	case "unfollow":
		r, err := cl.Unfollow(ctx, &driftnodepb.UnfollowReq{Target: paramString(params, "target"), Passphrase: paramString(params, "passphrase")})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"unfollowed": r.Unfollowed}}, nil
	case "unlock":
		r, err := cl.Unlock(ctx, &driftnodepb.UnlockReq{Passphrase: paramString(params, "passphrase")})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"status": r.Status, "identity": r.Identity}}, nil
	case "lock":
		r, err := cl.Lock(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"status": r.Status}}, nil
	case "sync":
		r, err := cl.Sync(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"status": r.Status}}, nil
	case "stop":
		r, err := cl.Stop(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"status": r.Status}}, nil
	case "status":
		r, err := cl.Status(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"running": r.Running, "zens": int(r.Zens), "socket": r.Socket, "transport": r.Transport, "unlocked": r.Unlocked}}, nil
	case "rotate-key":
		r, err := cl.RotateKey(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"token": r.Token}}, nil
	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	}
}

// paramString reads a string parameter from a params map.
func paramString(params map[string]any, key string) string {
	if params == nil {
		return ""
	}
	if v, ok := params[key].(string); ok {
		return v
	}
	return ""
}

// identitiesFromProto converts proto identities back to the map slice shape
// the legacy Response.Result carried.
func identitiesFromProto(ids []*driftnodepb.Identity) []map[string]string {
	out := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, map[string]string{"identity": id.Identity, "name": id.Name})
	}
	return out
}

// feedItemsFromProto converts proto feed items back to the feedItem slice
// shape the legacy Response.Result carried.
func feedItemsFromProto(items []*driftnodepb.FeedItem) []feedItem {
	out := make([]feedItem, 0, len(items))
	for _, it := range items {
		out = append(out, feedItem{
			ID:        it.Id,
			Timestamp: it.Timestamp,
			Author:    it.Author,
			Name:      it.Name,
			Text:      it.Text,
			LikeCount: int(it.LikeCount),
			Likers:    it.Likers,
		})
	}
	return out
}

// zensFromProto converts proto zens back to the []any shape the legacy
// Response.Result carried, so callers asserting on resp.Result.([]any)
// keep working.
func zensFromProto(zens []*driftnodepb.Zen) []any {
	out := make([]any, 0, len(zens))
	for _, z := range zens {
		out = append(out, map[string]any{
			"id":       z.Id,
			"identity": z.Identity,
			"name":     z.Name,
			"kind":     z.Kind,
			"status":   z.Status,
			"verified": z.Verified,
		})
	}
	return out
}

// tailcatTransport adapts the net.Dialer (which dials a tailcat.Addr) to the
// daemon's Transport interface (which dials a string token).
type tailcatTransport struct {
	d *p2p.Dialer
}

func (t tailcatTransport) Dial(ctx context.Context, token string) (net.Conn, error) {
	return t.d.Dial(ctx, tailcat.Addr(token))
}
