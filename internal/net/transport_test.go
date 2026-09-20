package net

import (
	"context"
	"io"
	"log/slog"
	"net"
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
