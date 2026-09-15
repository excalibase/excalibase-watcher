package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
)

func newTestPublisher(publishFn func(ctx context.Context, subject string, data []byte) error) *Publisher {
	p := NewPublisher(config.NATSConfig{SubjectPrefix: "cdc"}, cdc.NewService())
	p.publishFn = publishFn
	p.retryBaseDelay = time.Millisecond
	return p
}

func TestPublishRetriesUntilSuccessAndAcksOnlyOnce(t *testing.T) {
	var attempts atomic.Int32
	p := newTestPublisher(func(context.Context, string, []byte) error {
		if attempts.Add(1) < 3 {
			return errors.New("nats: timeout")
		}
		return nil
	})
	var acked atomic.Int32
	p.SetOnPublished(func(cdc.Event) { acked.Add(1) })

	event := cdc.NewEvent(cdc.Insert, "public", "users", `{"id":1}`, "INSERT", "0/10")
	if err := p.publish(context.Background(), event); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if attempts.Load() != 3 {
		t.Errorf("attempts = %d, want 3", attempts.Load())
	}
	if acked.Load() != 1 {
		t.Errorf("onPublished calls = %d, want 1", acked.Load())
	}
	if got := p.LastAckedLSN(); got != "0/10" {
		t.Errorf("LastAckedLSN = %q, want 0/10", got)
	}
}

func TestPublishDoesNotAckWhileFailing(t *testing.T) {
	p := newTestPublisher(func(context.Context, string, []byte) error {
		return errors.New("nats: no responders")
	})
	var acked atomic.Int32
	p.SetOnPublished(func(cdc.Event) { acked.Add(1) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	event := cdc.NewEvent(cdc.Insert, "public", "users", `{"id":1}`, "INSERT", "0/20")
	err := p.publish(ctx, event)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("publish err = %v, want context deadline", err)
	}
	if acked.Load() != 0 {
		t.Errorf("onPublished must not be called for an unpublished event, got %d", acked.Load())
	}
	if got := p.LastAckedLSN(); got != "" {
		t.Errorf("LastAckedLSN = %q, want empty", got)
	}
}
