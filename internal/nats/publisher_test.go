//go:build integration

package nats

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
)

func setupNATS(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()

	container, err := tcnats.Run(ctx, "nats:2.10")
	if err != nil {
		t.Fatalf("starting NATS container: %v", err)
	}

	url, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		container.Terminate(ctx)
	}
	return url, cleanup
}

// provisionStream pre-creates the JetStream stream the publisher expects to
// verify. The watcher is a pure publisher (never creates streams) — tests must
// mirror infra behavior and provision the stream first.
func provisionStream(t *testing.T, url string, cfg config.NATSConfig) {
	t.Helper()
	ctx := context.Background()
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.StreamName,
		Subjects: []string{cfg.SubjectPrefix + ".>"},
		Storage:  jetstream.MemoryStorage,
		MaxAge:   time.Duration(cfg.MaxAgeMinutes) * time.Minute,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
}

func makeNATSConfig(url string) config.NATSConfig {
	return config.NATSConfig{
		Enabled:       true,
		URL:           url,
		StreamName:    "TEST_CDC",
		SubjectPrefix: "cdc",
		MaxAgeMinutes: 5,
		Storage:       "memory",
	}
}

func TestStreamCreation(t *testing.T) {
	url, cleanup := setupNATS(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeNATSConfig(url)
	provisionStream(t, url, cfg)
	pub := NewPublisher(cfg, svc)
	if err := pub.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer pub.Stop()

	// Verify stream exists
	conn, _ := nats.Connect(url)
	defer conn.Close()
	js, _ := jetstream.New(conn)

	stream, err := js.Stream(ctx, "TEST_CDC")
	if err != nil {
		t.Fatalf("stream not found: %v", err)
	}
	info, _ := stream.Info(ctx)
	if info.Config.Name != "TEST_CDC" {
		t.Errorf("stream name = %q", info.Config.Name)
	}
}

func TestPublishInsertEvent(t *testing.T) {
	url, cleanup := setupNATS(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeNATSConfig(url)
	provisionStream(t, url, cfg)
	pub := NewPublisher(cfg, svc)
	if err := pub.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer pub.Stop()

	// Create a consumer to read messages
	conn, _ := nats.Connect(url)
	defer conn.Close()
	js, _ := jetstream.New(conn)
	consumer, err := js.CreateConsumer(ctx, "TEST_CDC", jetstream.ConsumerConfig{
		FilterSubject: "cdc.public.users",
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Emit an event
	event := cdc.NewEvent(cdc.Insert, "public", "users", `{"id":1, "name":"Alice"}`, "INSERT", "0/1")
	svc.HandleEvent(event)

	// Read from NATS
	msgs, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	count := 0
	for msg := range msgs.Messages() {
		count++
		var received cdc.Event
		if err := json.Unmarshal(msg.Data(), &received); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if received.Table != "users" {
			t.Errorf("table = %q", received.Table)
		}
		if received.Schema != "public" {
			t.Errorf("schema = %q", received.Schema)
		}
		msg.Ack()
	}
	if count != 1 {
		t.Errorf("expected 1 message, got %d", count)
	}
}

func TestSubjectFormat(t *testing.T) {
	url, cleanup := setupNATS(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeNATSConfig(url)
	provisionStream(t, url, cfg)
	pub := NewPublisher(cfg, svc)
	if err := pub.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer pub.Stop()

	conn, _ := nats.Connect(url)
	defer conn.Close()
	js, _ := jetstream.New(conn)

	// Subscribe to wildcard
	consumer, _ := js.CreateConsumer(ctx, "TEST_CDC", jetstream.ConsumerConfig{
		FilterSubject: "cdc.>",
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})

	time.Sleep(200 * time.Millisecond)

	svc.HandleEvent(cdc.NewEvent(cdc.Insert, "shop", "orders", `{"id":1}`, "INSERT", ""))

	msgs, _ := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	for msg := range msgs.Messages() {
		if msg.Subject() != "cdc.shop.orders" {
			t.Errorf("subject = %q, want cdc.shop.orders", msg.Subject())
		}
		msg.Ack()
	}
}

func TestDDLSubjectUsesFallback(t *testing.T) {
	url, cleanup := setupNATS(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeNATSConfig(url)
	provisionStream(t, url, cfg)
	pub := NewPublisher(cfg, svc)
	if err := pub.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer pub.Stop()

	conn, _ := nats.Connect(url)
	defer conn.Close()
	js, _ := jetstream.New(conn)
	consumer, _ := js.CreateConsumer(ctx, "TEST_CDC", jetstream.ConsumerConfig{
		FilterSubject: "cdc.>",
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})

	time.Sleep(200 * time.Millisecond)

	// DDL event with empty table
	svc.HandleEvent(cdc.NewEvent(cdc.DDL, "public", "", `{"query":"ALTER TABLE"}`, "DDL", ""))

	msgs, _ := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	for msg := range msgs.Messages() {
		if msg.Subject() != "cdc.public._ddl" {
			t.Errorf("subject = %q, want cdc.public._ddl", msg.Subject())
		}
		msg.Ack()
	}
}

func TestFilterExcludesBeginCommitHeartbeat(t *testing.T) {
	url, cleanup := setupNATS(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeNATSConfig(url)
	provisionStream(t, url, cfg)
	pub := NewPublisher(cfg, svc)
	if err := pub.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer pub.Stop()

	conn, _ := nats.Connect(url)
	defer conn.Close()
	js, _ := jetstream.New(conn)
	consumer, _ := js.CreateConsumer(ctx, "TEST_CDC", jetstream.ConsumerConfig{
		FilterSubject: "cdc.>",
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})

	time.Sleep(200 * time.Millisecond)

	// These should NOT be published
	svc.HandleEvent(cdc.NewEvent(cdc.Begin, "", "", "", "BEGIN", ""))
	svc.HandleEvent(cdc.NewEvent(cdc.Commit, "", "", "", "COMMIT", ""))
	svc.HandleEvent(cdc.NewEvent(cdc.Heartbeat, "", "", "", "HEARTBEAT", ""))

	// This SHOULD be published
	svc.HandleEvent(cdc.NewEvent(cdc.Insert, "public", "users", `{"id":1}`, "INSERT", ""))

	time.Sleep(500 * time.Millisecond)

	msgs, _ := consumer.Fetch(10, jetstream.FetchMaxWait(2*time.Second))
	count := 0
	for msg := range msgs.Messages() {
		count++
		var event cdc.Event
		json.Unmarshal(msg.Data(), &event)
		if event.Type == cdc.Begin || event.Type == cdc.Commit || event.Type == cdc.Heartbeat {
			t.Errorf("should not publish %v events", event.Type)
		}
		msg.Ack()
	}
	if count != 1 {
		t.Errorf("expected 1 publishable event, got %d", count)
	}
}
