//go:build integration

package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/excalibase/watcher-go/internal/natstest"
	"github.com/nats-io/nats.go/jetstream"
)

// outage spans several reconnect attempts, more than the two after which a
// default NATS client could give up.
const outage = 4 * time.Second

func natsConfig(url string) config.NATSConfig {
	return config.NATSConfig{
		Enabled:       true,
		URL:           url,
		StreamName:    "CDC_RECONNECT",
		SubjectPrefix: "cdc.proj",
	}
}

// provisionStream creates the stream on file storage so it survives the
// server being taken down.
func provisionFileStream(t *testing.T, env *natstest.Server, cfg config.NATSConfig) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(env.Connect(t))
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     cfg.StreamName,
		Subjects: []string{cfg.SubjectPrefix + ".>"},
		Storage:  jetstream.FileStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return js
}

func emitInserts(svc *cdc.Service, from, to int) {
	for id := from; id <= to; id++ {
		svc.HandleEvent(cdc.NewEvent(cdc.Insert, "public", "users",
			fmt.Sprintf(`{"id":%d}`, id), "INSERT", fmt.Sprintf("0/%X", id)))
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

// assertStreamHoldsExactly reads the whole stream and requires ids 1..count,
// each once and in order.
func assertStreamHoldsExactly(t *testing.T, js jetstream.JetStream, stream string, count int) {
	t.Helper()
	ctx := context.Background()
	waitFor(t, 30*time.Second, fmt.Sprintf("%d messages in %s", count, stream), func() bool {
		info, err := js.Stream(ctx, stream)
		return err == nil && info.CachedInfo().State.Msgs >= uint64(count)
	})
	consumer, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("ordered consumer: %v", err)
	}
	batch, err := consumer.Fetch(count+10, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	var ids []int
	for msg := range batch.Messages() {
		var event cdc.Event
		var row struct {
			ID int `json:"id"`
		}
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			t.Fatalf("decode %s: %v", msg.Data(), err)
		}
		if err := json.Unmarshal([]byte(event.Data), &row); err != nil {
			t.Fatalf("decode row %q: %v", event.Data, err)
		}
		ids = append(ids, row.ID)
	}
	if len(ids) != count {
		t.Fatalf("stream holds %d events %v, want exactly %d", len(ids), ids, count)
	}
	for i, id := range ids {
		if id != i+1 {
			t.Fatalf("stream order %v, want 1..%d once each", ids, count)
		}
	}
}

func startPublisher(t *testing.T, cfg config.NATSConfig, svc *cdc.Service) *Publisher {
	t.Helper()
	pub := NewPublisher(cfg, svc)
	if err := pub.Start(context.Background()); err != nil {
		t.Fatalf("Start must not fail while NATS is down: %v", err)
	}
	t.Cleanup(pub.Stop)
	return pub
}

func TestFirstConnectKeepsRetryingWhileNATSIsDown(t *testing.T) {
	env := natstest.Start(t)
	cfg := natsConfig(env.URL)
	js := provisionFileStream(t, env, cfg)
	env.Stop()

	svc := cdc.NewService()
	defer svc.Shutdown()
	pub := startPublisher(t, cfg, svc)
	emitInserts(svc, 1, 5)
	time.Sleep(outage)
	if pub.Ready() {
		t.Fatal("ready while the server is down")
	}

	env.Start(t)
	waitFor(t, 60*time.Second, "publisher to connect once the server is back", pub.Ready)
	assertStreamHoldsExactly(t, js, cfg.StreamName, 5)
	assertNoFailure(t, pub)
}

func TestReconnectKeepsRetryingThroughAnOutage(t *testing.T) {
	env := natstest.Start(t)
	cfg := natsConfig(env.URL)
	js := provisionFileStream(t, env, cfg)

	svc := cdc.NewService()
	defer svc.Shutdown()
	pub := startPublisher(t, cfg, svc)
	waitFor(t, 30*time.Second, "publisher to connect", pub.Ready)
	emitInserts(svc, 1, 2)
	waitFor(t, 30*time.Second, "first events acked", func() bool { return pub.LastAckedLSN() == "0/2" })

	env.Stop()
	waitFor(t, 10*time.Second, "not ready after the server went away", func() bool { return !pub.Ready() })
	emitInserts(svc, 3, 8)
	time.Sleep(outage)

	env.Start(t)
	waitFor(t, 60*time.Second, "publisher to reconnect once the server is back", pub.Ready)
	assertStreamHoldsExactly(t, js, cfg.StreamName, 8)
	assertNoFailure(t, pub)
}

func TestMissingStreamFailsOnceConnected(t *testing.T) {
	env := natstest.Reserve(t)
	cfg := natsConfig(env.URL)

	svc := cdc.NewService()
	defer svc.Shutdown()
	pub := startPublisher(t, cfg, svc)
	env.Start(t)

	select {
	case err := <-pub.Failed():
		if err == nil {
			t.Fatal("nil failure")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("a missing stream must still fail the watcher once connected")
	}
	if pub.Ready() {
		t.Error("ready without a stream")
	}
}

func assertNoFailure(t *testing.T, pub *Publisher) {
	t.Helper()
	select {
	case err := <-pub.Failed():
		t.Fatalf("publisher reported failure: %v", err)
	default:
	}
}

// With NATS reachable at boot, a missing stream fails Start itself, before the
// database listeners create a slot that would then be left behind.
func TestMissingStreamFailsStartWhenNATSIsUp(t *testing.T) {
	env := natstest.Start(t)
	svc := cdc.NewService()
	defer svc.Shutdown()

	pub := NewPublisher(natsConfig(env.URL), svc)
	err := pub.Start(context.Background())
	defer pub.Stop()
	if !errors.Is(err, errStreamMissing) {
		t.Fatalf("Start err = %v, want missing stream", err)
	}
}

// A restarted watcher re-publishing an event the stream already holds (the
// slot had not been confirmed past it) must not store it twice.
func TestRePublishAfterRestartIsDeduplicated(t *testing.T) {
	env := natstest.Start(t)
	cfg := natsConfig(env.URL)
	js := provisionFileStream(t, env, cfg)

	event := cdc.NewEvent(cdc.Insert, "public", "users", `{"id":1}`, "INSERT", "0/1")
	event.SourceID = "pg:0/1000:0"
	for run := 0; run < 2; run++ {
		svc := cdc.NewService()
		pub := NewPublisher(cfg, svc)
		if err := pub.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		svc.HandleEvent(event)
		waitFor(t, 30*time.Second, "event acked", func() bool { return pub.LastAckedLSN() == "0/1" })
		pub.Stop()
		svc.Shutdown()
	}
	assertStreamHoldsExactly(t, js, cfg.StreamName, 1)
}
