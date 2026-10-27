package net

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"
)

// skipIfNoTailcat starts and immediately stops a listener to check whether
// the environment supports tailcat's network monitor. The sandbox may not.
func skipIfNoTailcat(t *testing.T) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	l, err := NewListenerWithKey(func(net.Conn) {}, logger, nil)
	if err != nil {
		t.Skipf("tailcat not available in this environment: %v", err)
	}
	l.Close()
}

func TestTransportRoundTrip(t *testing.T) {
	skipIfNoTailcat(t)
	received := make(chan []byte, 1)
	handler := func(conn net.Conn) {
		defer conn.Close()
		buf := make([]byte, 1024)
		n, _ := conn.Read(buf)
		received <- buf[:n]
		conn.Write([]byte("ack"))
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	listener, err := NewListenerWithKey(handler, logger, nil)
	if err != nil {
		t.Fatalf("NewListenerWithKey: %v", err)
	}
	defer listener.Close()

	dialer := NewDialer(logger)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := dialer.Dial(ctx, listener.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}

	select {
	case data := <-received:
		if string(data) != "hello" {
			t.Fatalf("received: want %q, got %q", "hello", string(data))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for server to receive")
	}

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if string(buf[:n]) != "ack" {
		t.Fatalf("ack: want %q, got %q", "ack", string(buf[:n]))
	}
}

func TestListenerAddr(t *testing.T) {
	skipIfNoTailcat(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	listener, err := NewListenerWithKey(func(net.Conn) {}, logger, nil)
	if err != nil {
		t.Fatalf("NewListenerWithKey: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr()
	if len(addr) < 3 || string(addr[:2]) != "tc" {
		t.Fatalf("addr should start with 'tc', got %q", addr)
	}
}

// dialCountFor makes cache size observable to tests without exposing the
// map outside the package.
func dialerCacheSize(d *Dialer) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.clients)
}

// TestDialerCachesClients asserts consecutive dials to the same address
// reuse one tailcat client: the tunnel must not be rebuilt per dial, or
// every sync round leaks the previous engine and netstack.
func TestDialerCachesClients(t *testing.T) {
	skipIfNoTailcat(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	listener, err := NewListenerWithKey(func(net.Conn) {}, logger, nil)
	if err != nil {
		t.Fatalf("NewListenerWithKey: %v", err)
	}
	defer listener.Close()

	d := NewDialer(logger)
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := range 2 {
		conn, err := d.Dial(ctx, listener.Addr())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conn.Close()
	}
	if n := dialerCacheSize(d); n != 1 {
		t.Fatalf("want exactly one cached client after two dials, got %d", n)
	}
}

// TestDialerCloseTearsDownClients asserts Close releases every cached
// client, so a stopped daemon leaves no tunnel state behind.
func TestDialerCloseTearsDownClients(t *testing.T) {
	skipIfNoTailcat(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	listener, err := NewListenerWithKey(func(net.Conn) {}, logger, nil)
	if err != nil {
		t.Fatalf("NewListenerWithKey: %v", err)
	}
	defer listener.Close()

	d := NewDialer(logger)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := d.Dial(ctx, listener.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Close()

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := dialerCacheSize(d); n != 0 {
		t.Fatalf("want no cached clients after Close, got %d", n)
	}
}

// TestDialerCacheEvictsColdest asserts the cache stays within its cap:
// after dialing more distinct addresses than the cap allows, the cache
// holds at most cap entries, and the coldest (evicted) address still
// dials again successfully, proving eviction tore the old client down
// rather than leaving wedged state. The cap is lowered for the test so
// the assertion needs a handful of real tunnels, not dozens.
func TestDialerCacheEvictsColdest(t *testing.T) {
	skipIfNoTailcat(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	const cap = 2
	want := cap + 2
	listeners := make([]*Listener, 0, want)
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()
	for i := range want {
		l, err := NewListenerWithKey(func(net.Conn) {}, logger, nil)
		if err != nil {
			t.Fatalf("listener %d: %v", i, err)
		}
		listeners = append(listeners, l)
	}

	d := NewDialer(logger)
	d.maxClients = cap
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	for i, l := range listeners {
		conn, err := d.Dial(ctx, l.Addr())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conn.Close()
	}
	if n := dialerCacheSize(d); n > cap {
		t.Fatalf("cache should stay within cap %d, got %d", cap, n)
	}
	// The first dialed address was the coldest and was evicted; dialing
	// it again must build a fresh client and succeed.
	conn, err := d.Dial(ctx, listeners[0].Addr())
	if err != nil {
		t.Fatalf("re-dial evicted coldest: %v", err)
	}
	conn.Close()
}

// TestDialerFailedDialEvictsClient asserts a dial failure leaves nothing
// cached for that address, so a wedged tunnel is not reused by the next
// dial attempt.
func TestDialerFailedDialEvictsClient(t *testing.T) {
	skipIfNoTailcat(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	d := NewDialer(logger)
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// An address with no listener behind it fails to dial. It may also be
	// unparseable, failing before any client is cached; either way the
	// cache must end up empty.
	if _, err := d.Dial(ctx, "tcpnowhere"); err == nil {
		t.Fatal("dial to a dead address should fail")
	}
	if n := dialerCacheSize(d); n != 0 {
		t.Fatalf("want empty cache after failed dial, got %d", n)
	}
}

// TestDialerConcurrentColdDialsKeepOneClient asserts two dials racing to
// the same cold address converge on a single cached client and both see
// a working connection, so the loser of the race is closed rather than
// leaked.
func TestDialerConcurrentColdDialsKeepOneClient(t *testing.T) {
	skipIfNoTailcat(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	listener, err := NewListenerWithKey(func(net.Conn) {}, logger, nil)
	if err != nil {
		t.Fatalf("NewListenerWithKey: %v", err)
	}
	defer listener.Close()

	d := NewDialer(logger)
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const dials = 2
	var wg sync.WaitGroup
	errCh := make(chan error, dials)
	for range dials {
		wg.Go(func() {
			conn, err := d.Dial(ctx, listener.Addr())
			if err != nil {
				errCh <- err
				return
			}
			conn.Close()
		})
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent dial: %v", err)
	}
	if n := dialerCacheSize(d); n != 1 {
		t.Fatalf("want one cached client after concurrent cold dials, got %d", n)
	}
}
