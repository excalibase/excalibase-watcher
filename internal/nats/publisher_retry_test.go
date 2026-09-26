package nats

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
)

type publishFunc = func(ctx context.Context, subject string, data []byte, msgID string) error

func newTestPublisher(publishFn publishFunc) *Publisher {
	p := NewPublisher(config.NATSConfig{SubjectPrefix: "cdc"}, cdc.NewService())
	p.publishFn = publishFn
	p.retryBaseDelay = time.Millisecond
	return p
}

func TestPublishRetriesUntilSuccessAndAcksOnlyOnce(t *testing.T) {
	var attempts atomic.Int32
	p := newTestPublisher(func(context.Context, string, []byte, string) error {
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

// A publish whose ack was lost may still have been stored. Every retry of one
// event carries the same message id so the stream drops the second copy.
func TestRetriesOfOneEventShareAMessageID(t *testing.T) {
	var mu sync.Mutex
	var ids []string
	p := newTestPublisher(func(_ context.Context, _ string, _ []byte, msgID string) error {
		mu.Lock()
		defer mu.Unlock()
		ids = append(ids, msgID)
		if len(ids) < 3 {
			return errors.New("nats: timeout")
		}
		return nil
	})

	first := cdc.NewEvent(cdc.Insert, "public", "users", `{"id":1}`, "INSERT", "0/10")
	second := cdc.NewEvent(cdc.Insert, "public", "users", `{"id":1}`, "INSERT", "0/10")
	if err := p.publish(context.Background(), first); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if err := p.publish(context.Background(), second); err != nil {
		t.Fatalf("publish second: %v", err)
	}

	if len(ids) != 4 {
		t.Fatalf("attempts = %d, want 4", len(ids))
	}
	if ids[0] == "" || ids[0] != ids[1] || ids[1] != ids[2] {
		t.Errorf("retries of one event used ids %q, want one non-empty id", ids[:3])
	}
	if ids[3] == ids[0] {
		t.Error("two distinct events must not share a message id, or the stream would drop a real change")
	}
}

func TestPublishDoesNotAckWhileFailing(t *testing.T) {
	p := newTestPublisher(func(context.Context, string, []byte, string) error {
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

// An event with a source position gets the same id however often it is
// published (retry, lost ack, re-stream after restart), scoped to this
// watcher's subject prefix because tenants share one stream.
func TestMessageIDComesFromTheSourcePosition(t *testing.T) {
	p := NewPublisher(config.NATSConfig{SubjectPrefix: "cdc.proj-a"}, cdc.NewService())
	other := NewPublisher(config.NATSConfig{SubjectPrefix: "cdc.proj-b"}, cdc.NewService())
	event := cdc.NewEvent(cdc.Insert, "public", "users", `{"id":1}`, "INSERT", "0/10")
	event.SourceID = "pg:0/1000:2"

	if got := p.messageID(event); got != "cdc.proj-a/pg:0/1000:2" {
		t.Errorf("message id = %q", got)
	}
	if p.messageID(event) != NewPublisher(config.NATSConfig{SubjectPrefix: "cdc.proj-a"}, cdc.NewService()).messageID(event) {
		t.Error("a restarted watcher must derive the same id")
	}
	if other.messageID(event) == p.messageID(event) {
		t.Error("two tenants with the same source position must not dedupe each other")
	}
}

func TestEventWithoutASourcePositionGetsAUniqueID(t *testing.T) {
	p := NewPublisher(config.NATSConfig{SubjectPrefix: "cdc"}, cdc.NewService())
	event := cdc.NewEvent(cdc.Insert, "public", "users", `{"id":1}`, "INSERT", "")
	first, second := p.messageID(event), p.messageID(event)
	if first == "" || first == second {
		t.Errorf("snapshot ids %q and %q must be non-empty and distinct", first, second)
	}
}
