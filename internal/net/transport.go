package net

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/wgengine/filter"
)

// SyncPort is the TCP port the sync protocol listens on inside a tailcat
// tunnel. The tunnel carries a WireGuard-encrypted byte stream; this is the
// application-level port for event-log sync (section 7.3).
const SyncPort uint16 = 7421

// Listener listens for inbound zen connections over tailcat and runs the
// sync protocol on each (section 5.1).
type Listener struct {
	tcServer *tailcat.Server
	addr     tailcat.Addr
	priv     *tailcat.PrivateKey
	logger   *slog.Logger
}

// KeyConfig supplies a pre-existing tailcat private key to a listener. A nil
// KeyConfig generates a fresh ephemeral key. The daemon passes a KeyConfig
// loaded from the local store so the address token stays stable across
// restarts (section 5.2).
type KeyConfig struct {
	// KeyBytes is a tailcat private key serialized as JSON. When set, the
	// listener loads its identity from these bytes instead of generating a
	// fresh key.
	KeyBytes []byte
}

// NewListenerWithKey creates a tailcat listener that accepts inbound
// connections and dispatches them to handler. The handler receives a duplex
// byte stream over which the sync protocol runs. OnTCP returns a fresh
// handler per inbound connection, so one listener handles many concurrent
// zens (section 5.1). A nil keyCfg generates a fresh ephemeral key.
func NewListenerWithKey(handler func(net.Conn), logger *slog.Logger, keyCfg *KeyConfig) (*Listener, error) {
	priv := tailcat.NewPrivateKey()
	if keyCfg != nil && len(keyCfg.KeyBytes) > 0 {
		var loaded tailcat.PrivateKey
		if err := json.Unmarshal(keyCfg.KeyBytes, &loaded); err != nil {
			return nil, fmt.Errorf("parse key: %w", err)
		}
		priv = &loaded
	}
	s := &tailcat.Server{
		Key:          priv.Private,
		PresharedKey: priv.Public.PresharedKey,
		Logf: func(format string, args ...any) {
			logger.Debug(fmt.Sprintf(format, args...))
		},
		OnTCP: func(port uint16) func(net.Conn) {
			if port != SyncPort {
				return nil
			}
			return handler
		},
		ServedTCPPorts: []filter.PortRange{{First: SyncPort, Last: SyncPort}},
	}
	if err := s.Start(); err != nil {
		return nil, fmt.Errorf("tailcat start: %w", err)
	}
	return &Listener{
		tcServer: s,
		addr:     s.TailcatAddr(),
		priv:     priv,
		logger:   logger,
	}, nil
}

// KeyBytes returns the listener's tailcat private key serialized as JSON.
// Used by the daemon to persist the key in the local store so the node's
// token stays stable across restarts.
func (l *Listener) KeyBytes() ([]byte, error) {
	return json.MarshalIndent(l.priv, "", "\t")
}

// Addr returns the tailcat address (tc<base64>) that zens dial to reach this
// listener.
func (l *Listener) Addr() tailcat.Addr { return l.addr }

// AddrFromKeyBytes derives the tailcat address from a persisted private key
// (the JSON form KeyBytes produces), without starting a listener.
// Returns empty if no key is set.
func AddrFromKeyBytes(keyBytes []byte) (string, error) {
	if len(keyBytes) == 0 {
		return "", nil
	}
	var pk tailcat.PrivateKey
	if err := json.Unmarshal(keyBytes, &pk); err != nil {
		return "", fmt.Errorf("parse key: %w", err)
	}
	return string(pk.Public.Addr()), nil
}

// Close stops the listener.
func (l *Listener) Close() error { return l.tcServer.Close() }

// defaultCachedClients is the dialer's default cache bound. Each client
// holds a userspace WireGuard engine, netstack, and DERP connection, so
// lingering ones dominate memory. The protocol caps follows at
// core.MaxFollows; the bound is a backstop against pathological token
// sets, not the working bound: dials to the same address reuse one
// client, so only distinct dialed addresses occupy entries.
const defaultCachedClients = 64

// Dialer dials remote zen tailcat addresses. It caches one tailcat client
// per address: a client owns a userspace tunnel (engine, netstack, DERP
// connection) that is expensive to build and holds real resources, so dials
// to the same zen reuse it instead of building a fresh tunnel per sync
// round. Entries leave the cache on dial failure (a suspect tunnel is torn
// down, not reused), on LRU eviction over the cap, and on Close.
type Dialer struct {
	logger *slog.Logger

	// maxClients bounds the cache size; default defaultCachedClients.
	maxClients int

	mu       sync.Mutex
	clients  map[tailcat.Addr]*tailcat.Client
	lastUsed map[tailcat.Addr]int64
}

// NewDialer creates a dialer for outbound zen connections.
func NewDialer(logger *slog.Logger) *Dialer {
	return &Dialer{
		logger:     logger,
		maxClients: defaultCachedClients,
		clients:    make(map[tailcat.Addr]*tailcat.Client),
		lastUsed:   make(map[tailcat.Addr]int64),
	}
}

// clientFor returns the tailcat client for addr, building a fresh one when
// none is cached. Builds run outside d.mu; when two cold dials race, the
// loser's fresh build is closed so the race never leaks a client.
func (d *Dialer) clientFor(addr tailcat.Addr) *tailcat.Client {
	d.mu.Lock()
	if c, ok := d.clients[addr]; ok {
		d.lastUsed[addr] = time.Now().UnixNano()
		d.mu.Unlock()
		return c
	}
	d.mu.Unlock()

	c := d.buildClient(addr)

	d.mu.Lock()
	if existing, ok := d.clients[addr]; ok {
		// Lost the race: reuse the winner, close our fresh build after
		// releasing the lock.
		d.lastUsed[addr] = time.Now().UnixNano()
		d.mu.Unlock()
		go c.Close()
		return existing
	}
	d.evictLocked()
	d.clients[addr] = c
	d.lastUsed[addr] = time.Now().UnixNano()
	d.mu.Unlock()
	return c
}

// buildClient constructs a tailcat client for addr with the dialer's logger.
func (d *Dialer) buildClient(addr tailcat.Addr) *tailcat.Client {
	c := tailcat.NewClient(addr)
	c.Logf = func(format string, args ...any) {
		d.logger.Debug(fmt.Sprintf(format, args...))
	}
	return c
}

// evictLocked enforces the cache cap, closing evicted clients inline. Runs
// on insert only, so the cache never exceeds the cap. LRU order comes from
// lastUsed timestamps. Caller must hold d.mu.
func (d *Dialer) evictLocked() {
	for len(d.clients) >= d.maxClients {
		var coldest tailcat.Addr
		coldestTS := int64(1<<62 - 1)
		for a, ts := range d.lastUsed {
			if ts < coldestTS {
				coldest = a
				coldestTS = ts
			}
		}
		c, ok := d.clients[coldest]
		if ok {
			delete(d.clients, coldest)
			delete(d.lastUsed, coldest)
			go c.Close()
		}
	}
}

// Dial connects to a zen's tailcat address and returns a duplex stream over
// which the sync protocol runs. The stream rides the cached client for
// that address. A failed dial closes and evicts that address's client so a
// wedged tunnel is never reused.
func (d *Dialer) Dial(ctx context.Context, addr tailcat.Addr) (net.Conn, error) {
	c := d.clientFor(addr)
	conn, err := c.DialTCPPort(ctx, SyncPort)
	if err != nil {
		d.evictAddr(addr)
		return nil, fmt.Errorf("tailcat dial: %w", err)
	}
	return conn, nil
}

// evictAddr closes and removes the cached client for one address.
func (d *Dialer) evictAddr(addr tailcat.Addr) {
	d.mu.Lock()
	c, ok := d.clients[addr]
	if ok {
		delete(d.clients, addr)
		delete(d.lastUsed, addr)
	}
	d.mu.Unlock()
	if ok {
		c.Close()
	}
}

// Close tears down every cached client, freeing the tunnel resources the
// dialer holds. Safe to call more than once.
func (d *Dialer) Close() error {
	d.mu.Lock()
	clients := d.clients
	d.clients = make(map[tailcat.Addr]*tailcat.Client)
	d.lastUsed = make(map[tailcat.Addr]int64)
	d.mu.Unlock()
	for _, c := range clients {
		c.Close()
	}
	return nil
}
