package nats

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	reconnectBaseDelay    = 500 * time.Millisecond
	reconnectMaxDelay     = 30 * time.Second
	connectedPollInterval = 200 * time.Millisecond
)

var errConnectionClosed = errors.New("NATS connection closed")

// backoff is exponential with equal jitter, so watchers that lost the server
// together do not all retry in the same instant.
type backoff struct {
	base, max  time.Duration
	randInt63n func(int64) int64
}

func defaultBackoff() backoff {
	return backoff{base: reconnectBaseDelay, max: reconnectMaxDelay, randInt63n: rand.Int64N}
}

// delay returns a duration in [step/2, step] for the 1-based attempt.
func (b backoff) delay(attempt int) time.Duration {
	step := b.base
	for i := 1; i < attempt && step < b.max; i++ {
		step *= 2
	}
	step = min(step, b.max)
	half := step / 2
	return half + time.Duration(b.randInt63n(int64(step-half)+1))
}

// connectOptions never lets the client give up on an unreachable server, at
// boot or later. An auth error means a misconfigured server and is not
// retried forever. Publishes are not buffered while disconnected; they fail
// and are retried by publishWithRetry with the same message id.
func (p *Publisher) connectOptions() []nats.Option {
	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
		nats.ReconnectBufSize(-1),
		nats.CustomReconnectDelay(p.nextReconnectDelay),
		nats.ReconnectErrHandler(func(_ *nats.Conn, err error) {
			slog.Warn("NATS connection attempt failed", "error", err)
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if !p.stopping.Load() {
				slog.Warn("NATS disconnected", "error", err)
			}
		}),
		nats.ConnectHandler(func(conn *nats.Conn) {
			slog.Info("NATS connected", "url", conn.ConnectedUrlRedacted())
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			slog.Info("NATS reconnected", "url", conn.ConnectedUrlRedacted())
		}),
		nats.ClosedHandler(p.onClosed),
		nats.ErrorHandler(logAsyncError),
	}
	if p.cfg.Username != "" {
		opts = append(opts, nats.UserInfo(p.cfg.Username, p.cfg.Password))
	}
	if p.cfg.InboxPrefix != "" {
		opts = append(opts, nats.CustomInboxPrefix(p.cfg.InboxPrefix))
	}
	return opts
}

// nextReconnectDelay runs once per failed attempt. A socket-level failure is
// already reported by ReconnectErrHandler; a failed handshake is only visible
// here, as the connection's last error.
func (p *Publisher) nextReconnectDelay(attempt int) time.Duration {
	delay := p.reconnect.delay(attempt)
	if conn := p.conn.Load(); conn != nil {
		if err := conn.LastError(); err != nil {
			slog.Warn("NATS handshake failed, retrying", "attempt", attempt, "retry_in", delay, "error", err)
		}
	}
	return delay
}

// logAsyncError replaces the client's default stderr print.
func logAsyncError(_ *nats.Conn, _ *nats.Subscription, err error) {
	slog.Warn("NATS error", "error", err)
}

// connectionLost reports a connection that exists but is down; with no
// connection (unit tests with a fake publish) there is nothing to wait for.
func (p *Publisher) connectionLost() bool {
	conn := p.conn.Load()
	return conn != nil && !conn.IsConnected()
}

// onClosed fires when the client stops reconnecting for good. Outside Stop
// that means nothing will ever be published again, so the watcher must exit.
func (p *Publisher) onClosed(conn *nats.Conn) {
	if p.stopping.Load() {
		return
	}
	err := errConnectionClosed
	if last := conn.LastError(); last != nil {
		err = errors.Join(errConnectionClosed, last)
	}
	p.fail(err)
}

// waitConnected blocks until the connection is up, so stream verification
// does not fail (and log) on every attempt while the server is unreachable.
func (p *Publisher) waitConnected(ctx context.Context, conn *nats.Conn) error {
	ticker := time.NewTicker(connectedPollInterval)
	defer ticker.Stop()
	for !conn.IsConnected() {
		if conn.IsClosed() {
			return errConnectionClosed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
