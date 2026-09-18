package net

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"

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

// KeyConfig controls the tailcat private key used by a listener. A nil
// KeyConfig generates an ephemeral key (the default for non-seed nodes). A
// KeyConfig with KeyFile set loads a persistent key so the node's token stays
// stable across restarts, which is what seed nodes listed in bootstrap.yaml
// need (section 5.2).
type KeyConfig struct {
	// KeyFile is the path to a tailcat *.private.json key file. If set,
	// the listener loads its identity from this file instead of generating
	// a fresh ephemeral key.
	KeyFile string
}

// NewListener creates a tailcat listener that accepts inbound connections and
// dispatches them to handler. The handler receives a duplex byte stream over
// which the sync protocol runs. OnTCP returns a fresh handler per inbound
// connection, so one listener handles many concurrent zens (section 5.1).
func NewListener(handler func(net.Conn), logger *slog.Logger) (*Listener, error) {
	return NewListenerWithKey(handler, logger, nil)
}

// NewListenerWithKey is like NewListener but allows a persistent key to be
// loaded from a file. If the key file does not exist, a new key is generated,
// the listener starts with it, and the key is saved to the file for the next
// run. If the file exists, the key is loaded so the node's address token
// stays stable across restarts. A nil keyCfg generates an ephemeral key.
func NewListenerWithKey(handler func(net.Conn), logger *slog.Logger, keyCfg *KeyConfig) (*Listener, error) {
	priv := tailcat.NewPrivateKey()
	if keyCfg != nil && keyCfg.KeyFile != "" {
		data, err := os.ReadFile(keyCfg.KeyFile)
		if err == nil {
			var loaded tailcat.PrivateKey
			if err := json.Unmarshal(data, &loaded); err != nil {
				return nil, fmt.Errorf("parse key file: %w", err)
			}
			priv = &loaded
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read key file: %w", err)
		}
		// If the file doesn't exist, priv stays as the freshly generated key.
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

// SaveKeyFile writes the listener's tailcat private key to path in the JSON
// format tailcat uses, so it can be loaded by NewListenerWithKey on a future
// run to preserve the node's token.
func (l *Listener) SaveKeyFile(path string) error {
	data, err := json.MarshalIndent(l.priv, "", "\t")
	if err != nil {
		return fmt.Errorf("marshal key: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write key file: %w", err)
	}
	return nil
}

// Addr returns the tailcat address (tc<base64>) that zens dial to reach this
// listener.
func (l *Listener) Addr() tailcat.Addr { return l.addr }

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
