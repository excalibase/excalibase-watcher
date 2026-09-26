package nats

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/nats-io/nats.go"
)

// unreachableConn is a connection that is retrying against a port nothing
// listens on, so it never becomes connected.
func unreachableConn(t *testing.T) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect("nats://127.0.0.1:1",
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Hour),
		nats.ReconnectBufSize(-1),
	)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(conn.Close)
	return conn
}

func TestPublishWaitsForTheConnectionInsteadOfRetryingBlindly(t *testing.T) {
	var attempts atomic.Int32
	p := newTestPublisher(func(context.Context, string, []byte, string) error {
		attempts.Add(1)
		return nats.ErrReconnectBufExceeded
	})
	p.conn.Store(unreachableConn(t))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := p.publish(ctx, cdc.NewEvent(cdc.Insert, "public", "users", `{"id":1}`, "INSERT", "0/1"))

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("publish err = %v, want context deadline", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts while disconnected = %d, want 1", got)
	}
}

func TestPublishLoopStopsOnceTheConnectionIsClosed(t *testing.T) {
	p := newTestPublisher(func(context.Context, string, []byte, string) error {
		return nats.ErrConnectionClosed
	})
	conn := unreachableConn(t)
	conn.Close()
	p.conn.Store(conn)
	var acked atomic.Int32
	p.SetOnPublished(func(cdc.Event) { acked.Add(1) })

	ch := make(chan cdc.Event, 2)
	ch <- cdc.NewEvent(cdc.Insert, "public", "users", `{"id":1}`, "INSERT", "0/1")
	ch <- cdc.NewEvent(cdc.Insert, "public", "users", `{"id":2}`, "INSERT", "0/2")
	done := make(chan struct{})
	go func() {
		p.publishLoop(context.Background(), ch)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publish loop kept going on a closed connection")
	}
	if acked.Load() != 0 || p.LastAckedLSN() != "" {
		t.Error("nothing may be acked on a closed connection")
	}
	if len(ch) != 1 {
		t.Errorf("loop consumed %d events past the failure, want 0", 1-len(ch))
	}
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

// The bus has no authentication, so an auth error is a misconfigured server and must be visible.
func TestAuthErrorIsLogged(t *testing.T) {
	logs := captureLogs(t)
	logAsyncError(unreachableConn(t), nil, nats.ErrAuthorization)
	if logs.Len() == 0 {
		t.Error("an authorization error was silenced")
	}
}

func TestRetryStepLogsOnlyWithAConnectionError(t *testing.T) {
	logs := captureLogs(t)
	p := newTestPublisher(nil)
	if got := p.nextReconnectDelay(2); got <= 0 || got > reconnectMaxDelay {
		t.Errorf("delay = %v", got)
	}
	if logs.Len() != 0 {
		t.Errorf("logged without a connection error: %s", logs.String())
	}
}

func TestClosedOutsideStopReportsFailure(t *testing.T) {
	p := newTestPublisher(nil)
	conn := unreachableConn(t)
	conn.Close()
	p.onClosed(conn)

	select {
	case err := <-p.Failed():
		if !errors.Is(err, errConnectionClosed) {
			t.Errorf("failure = %v", err)
		}
	default:
		t.Fatal("a connection closed for good must fail the publisher")
	}
}

func TestClosedDuringStopIsNotAFailure(t *testing.T) {
	p := newTestPublisher(nil)
	p.Stop()
	conn := unreachableConn(t)
	conn.Close()
	p.onClosed(conn)

	select {
	case err := <-p.Failed():
		t.Fatalf("Stop reported as failure: %v", err)
	default:
	}
}
