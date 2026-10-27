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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"driftnode/internal/core"
	"driftnode/internal/crawler"
	p2p "driftnode/internal/net"
	"driftnode/internal/proto/driftnodepb"
	"driftnode/internal/store"

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

// Daemon is the long-running process that holds network state and exposes
// the gRPC Driftnode control API over a Unix domain socket. The CLI and TUI
// are thin clients of this socket.
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
	// bgCancel stops all four background routines (crawl loop, assess
	// thread, head lane, tail lane) on Stop. They share one context so a
	// single cancellation tears down in-flight dials promptly.
	bgCancel context.CancelFunc
	// crawlNow triggers an immediate crawl pass out of the periodic
	// cadence, fired by user actions (post, follow, unfollow) and the
	// sync RPC. Buffered with capacity 1 so a pending signal is dropped
	// if one is already queued.
	crawlNow chan struct{}
	// splitMu guards headSet and tailSet, the head/tail split computed once
	// per assess pass and read by both lanes. assess is the sole writer
	// (the only routine that derives weights); it holds splitMu around the
	// weight derivation and the split, never during the resolve dials, so
	// the lanes keep running while pending follows are resolved. A lane
	// holds splitMu only for the slice copy it reads, so it dials against a
	// consistent snapshot even if assess publishes the next split mid-pass.
	// Separate from d.mu so the lanes do not contend with control-socket
	// handlers.
	splitMu sync.Mutex
	headSet []core.Identity
	tailSet []core.Identity
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

	// dialer is the outbound tailcat dialer the daemon owns when it
	// created its own transport (no injected one). Its cached tunnels are
	// torn down on Stop. Guarded by mu.
	dialer *p2p.Dialer

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

	// bootstrapDone is true once the bootstrap seed follow pass has run.
	// Cleared while the key is locked, so the next unlock retries the
	// bootstrap auto-follow if it ran before the key was available.
	bootstrapDone bool
	// bootstrapMu is held for the duration of a seed follow pass; TryLock
	// rejects a second concurrent pass (Start's and unlock's can overlap).
	bootstrapMu sync.Mutex
	// bootstrapRetryDelay is the pause between retry passes of failed
	// seeds. A field so tests can shorten it; zero means the default.
	bootstrapRetryDelay time.Duration

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

// New creates a Daemon backed by the given store.
func New(s *store.Store, logger *slog.Logger) *Daemon {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return &Daemon{
		store:     s,
		logger:    logger,
		zens:      make(map[string]*zenInfo),
		crawlNow:  make(chan struct{}, 1),
		done:      make(chan struct{}),
		nameCache: make(map[core.Identity]string),
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
	// Serve the typed Driftnode control API as gRPC over the Unix
	// socket; the CLI and TUI are gRPC clients.
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
		d.dialer = p2p.NewDialer(d.logger)
		d.transport = tailcatTransport{d.dialer}
	}
	if err := d.startTransport(); err != nil {
		d.logger.Warn("tailcat listener unavailable; inbound zen sync disabled", "err", err)
	} else {
		d.emitStatus()
	}

	// Background routines, all sharing one context cancelled on Stop:
	// the crawl loop (periodic + on-demand Profile-log walks), the assess
	// thread (weight recomputation and head/tail derivation), and the
	// head and tail lanes that continuously dial their sets with fast-skip.
	// The lanes deliver snappy sync of the top-K follows, so a reattached
	// daemon pulls the logs it missed while down without waiting on the
	// crawl interval.
	ctx, cancel := context.WithCancel(context.Background())
	d.bgCancel = cancel
	// Seed the head/tail split before launching the lanes so their first
	// pass reads a populated split instead of empty sets, then waits for
	// the first assess pass. Read-only: no dials, just the persisted
	// weights and routing table.
	d.seedSplit()
	go d.crawlLoop(ctx)
	go d.assessLoop(ctx)
	go d.headLane(ctx)
	go d.tailLane(ctx)
	// Trigger an immediate first crawl so a reattached daemon discovers
	// any new follows' Profile logs right away.
	d.triggerCrawlNow()

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
// background routines (crawl loop, assess thread, head/tail lanes), then
// removes the socket file. It is safe to call from the control-socket
// handler (self-stop) or from a signal handler.
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
	syncCancel := d.bgCancel
	d.bgCancel = nil
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
	if d.dialer != nil {
		d.dialer.Close()
	}
	if socket != "" {
		os.Remove(socket)
	}
	// Prune stale remote PostLog events so the on-disk store does not
	// grow without bound across restarts. Own logs are retained in full.
	if n, err := d.store.PruneRemotePosts(); err != nil {
		d.logger.Warn("prune remote posts", "err", err)
	} else if n > 0 {
		d.logger.Info("pruned stale remote posts", "count", n)
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
		Follows:   follows,
		Followers: followers,
		Status: &driftnodepb.StatusDiff{
			Running:   true,
			Zens:      int32(len(zens)),
			Transport: tcUp,
			Unlocked:  unlocked,
		},
	}
}

// addSubscriberProto registers a typed subscriber and returns it.
func (d *Daemon) addSubscriberProto() *subscriber {
	sub := newSubscriber()
	d.addSubscriber(sub)
	return sub
}

// feedPageLimit is the fixed page size shared by the subscribe snapshot and
// FeedPage. The snapshot is page 0; FeedPage serves page N.
const feedPageLimit = 200

// Scheduler constants. All source-code constants, not user-facing config.
// See docs/weighted-follow-graph-plan.md phase 6 for grounding.
const (
	// crawlInterval is the ticker period between crawl passes (walk the
	// follow graph, fetch Profile logs, section 9.3).
	crawlInterval = 10 * time.Minute
	// assessPeriod is the ticker period for the assess thread: how fast
	// weights are recomputed and head/tail re-derived.
	assessPeriod = 60 * time.Second
	// maxHeadSize is the ceiling on K, the head-set size. K scales with
	// the bound follows (about a third, Dunbar's layer ratio) up to this
	// cap.
	maxHeadSize = 150
	// headDialTimeout is the head lane's fast-skip per-dial timeout: a
	// non-answering head follow releases its slot in seconds.
	headDialTimeout = 5 * time.Second
	// headFanout is the head lane's parallel dials per pass.
	headFanout = 32
	// tailDialTimeout is the tail lane's per-dial timeout: latency is OK
	// here (liveness), but a never-answering follow still releases its slot.
	tailDialTimeout = 10 * time.Second
	// tailFanout is the tail lane's small fixed concurrency.
	tailFanout = 16
	// laneBackoff is the sleep after each lane pass so a fast pass does
	// not busy spin (shared by head and tail).
	laneBackoff = 5 * time.Second
	// onDemandDialTimeout is the per-dial timeout for the on-demand dial
	// path (follow-by-token, bootstrap, pending-resolution), derived from
	// the caller's context.
	onDemandDialTimeout = 30 * time.Second
	// bootstrapSeedAttempts is the number of follow attempts per seed.
	bootstrapSeedAttempts = 3
	// bootstrapSeedRetryDelay is the default pause between follow
	// attempts; the daemon field of the shorter name overrides it.
	bootstrapSeedRetryDelay = 5 * time.Second
)

// tailcatTransport adapts the net.Dialer (which dials a tailcat.Addr) to the
// daemon's Transport interface (which dials a string token).
type tailcatTransport struct {
	d *p2p.Dialer
}

func (t tailcatTransport) Dial(ctx context.Context, token string) (net.Conn, error) {
	return t.d.Dial(ctx, tailcat.Addr(token))
}
