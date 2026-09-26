package nats

import (
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/nats-io/nats.go"
)

func resolvedOptions(t *testing.T, cfg config.NATSConfig) nats.Options {
	t.Helper()
	opts := nats.GetDefaultOptions()
	for _, apply := range NewPublisher(cfg, cdc.NewService()).connectOptions() {
		if err := apply(&opts); err != nil {
			t.Fatalf("applying option: %v", err)
		}
	}
	return opts
}

// An unreachable server at boot and a server that goes away are both
// transient: the client must keep going.
func TestConnectOptionsNeverGiveUp(t *testing.T) {
	opts := resolvedOptions(t, config.NATSConfig{URL: "nats://localhost:4222"})

	if !opts.RetryOnFailedConnect {
		t.Error("a failed first connect must be retried, not returned")
	}
	if opts.IgnoreAuthErrorAbort {
		t.Error("an auth error means the server is misconfigured; it must not be retried forever")
	}
	if opts.MaxReconnect >= 0 {
		t.Errorf("MaxReconnect = %d, want unlimited", opts.MaxReconnect)
	}
	if opts.CustomReconnectDelayCB == nil {
		t.Error("reconnect delay must be the capped, jittered backoff")
	}
	if opts.ReconnectBufSize != -1 {
		t.Errorf("ReconnectBufSize = %d, want -1 so publishes fail fast instead of piling up while disconnected", opts.ReconnectBufSize)
	}
	for name, cb := range map[string]bool{
		"ReconnectErrCB":    opts.ReconnectErrCB != nil,
		"DisconnectedErrCB": opts.DisconnectedErrCB != nil,
		"ClosedCB":          opts.ClosedCB != nil,
		"AsyncErrorCB":      opts.AsyncErrorCB != nil,
	} {
		if !cb {
			t.Errorf("%s not set", name)
		}
	}
}

// A watcher with no credential keeps connecting anonymously, so an
// unauthenticated local/dev NATS is unaffected.
func TestConnectOptionsWithoutCredential(t *testing.T) {
	opts := resolvedOptions(t, config.NATSConfig{URL: "nats://localhost:4222"})
	if opts.User != "" || opts.Password != "" {
		t.Errorf("credential set without configuration: user=%q", opts.User)
	}
	if opts.InboxPrefix != "" {
		t.Errorf("InboxPrefix = %q, want client default", opts.InboxPrefix)
	}
}

func TestConnectOptionsCarriesCredentialAndInboxPrefix(t *testing.T) {
	opts := resolvedOptions(t, config.NATSConfig{
		Username:    "tenant-watcher:proj-a",
		Password:    "pw",
		InboxPrefix: "_INBOX_tw_proj-a",
	})
	if opts.User != "tenant-watcher:proj-a" || opts.Password != "pw" {
		t.Errorf("credential not carried: user=%q", opts.User)
	}
	if opts.InboxPrefix != "_INBOX_tw_proj-a" {
		t.Errorf("InboxPrefix = %q", opts.InboxPrefix)
	}
}

func TestReconnectBackoffIsExponentialJitteredAndCapped(t *testing.T) {
	low := backoff{base: time.Second, max: 30 * time.Second, randInt63n: func(int64) int64 { return 0 }}
	high := backoff{base: time.Second, max: 30 * time.Second, randInt63n: func(n int64) int64 { return n - 1 }}

	cases := []struct {
		attempt int
		step    time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{5, 16 * time.Second},
		{6, 30 * time.Second},
		{1000, 30 * time.Second},
	}
	for _, c := range cases {
		if got := low.delay(c.attempt); got != c.step/2 {
			t.Errorf("attempt %d lowest delay = %v, want %v", c.attempt, got, c.step/2)
		}
		if got := high.delay(c.attempt); got != c.step {
			t.Errorf("attempt %d highest delay = %v, want %v", c.attempt, got, c.step)
		}
	}
}

func TestDefaultReconnectBackoffStaysWithinBounds(t *testing.T) {
	retry := defaultBackoff()
	for attempt := 1; attempt < 64; attempt++ {
		got := retry.delay(attempt)
		if got <= 0 || got > reconnectMaxDelay {
			t.Fatalf("attempt %d delay = %v, want (0, %v]", attempt, got, reconnectMaxDelay)
		}
	}
}

func TestNotReadyBeforeStart(t *testing.T) {
	p := NewPublisher(config.NATSConfig{}, cdc.NewService())
	if p.Ready() {
		t.Error("a publisher that never connected must not report ready")
	}
}
