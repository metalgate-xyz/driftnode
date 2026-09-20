package net

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"

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

// Dialer dials a remote zen's tailcat address and returns a connected stream.
type Dialer struct {
	logger *slog.Logger
}

// NewDialer creates a dialer for outbound zen connections.
func NewDialer(logger *slog.Logger) *Dialer {
	return &Dialer{logger: logger}
}

// Dial connects to a zen's tailcat address and returns a duplex stream over
// which the sync protocol runs.
func (d *Dialer) Dial(ctx context.Context, addr tailcat.Addr) (net.Conn, error) {
	c := tailcat.NewClient(addr)
	c.Logf = func(format string, args ...any) {
		d.logger.Debug(fmt.Sprintf(format, args...))
	}
	conn, err := c.DialTCPPort(ctx, SyncPort)
	if err != nil {
		return nil, fmt.Errorf("tailcat dial: %w", err)
	}
	return conn, nil
}
