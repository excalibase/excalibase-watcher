//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	natspub "github.com/excalibase/watcher-go/internal/nats"
	"github.com/excalibase/watcher-go/internal/natstest"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
)

func readStreamEvents(t *testing.T, js jetstream.JetStream, stream string, count int) []cdc.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	consumer, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("ordered consumer: %v", err)
	}
	events := make([]cdc.Event, 0, count)
	for range count {
		msg, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
		if err != nil {
			t.Fatalf("reading message: %v", err)
		}
		var event cdc.Event
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			t.Fatalf("decode: %v", err)
		}
		events = append(events, event)
	}
	return events
}

// A row larger than NATS accepts (1 MB max_payload by default) is published
// with its key only, and CDC carries on with the next change.
func TestOversizedRowIsPublishedTruncatedAndCDCContinues(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	env := natstest.Start(t)
	natsCfg := config.NATSConfig{URL: env.URL, StreamName: "CDC_OVERSIZE", SubjectPrefix: "cdc.proj"}
	js, err := jetstream.New(env.Connect(t))
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: natsCfg.StreamName, Subjects: []string{natsCfg.SubjectPrefix + ".>"}, Storage: jetstream.FileStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	svc := cdc.NewService()
	defer svc.Shutdown()
	pub := natspub.NewPublisher(natsCfg, svc)
	pgCfg := makeConfig(connStr)
	listener, err := NewListener(pgCfg, svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	pub.SetOnPublished(listener.PublishedObserver())
	if err := pub.Start(ctx); err != nil {
		t.Fatalf("publisher Start: %v", err)
	}
	defer pub.Stop()
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("listener Start: %v", err)
	}
	defer listener.Stop()
	time.Sleep(500 * time.Millisecond)

	conn, err := pgx.Connect(ctx, stripReplicationParam(connStr))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var bigID int
	if err := conn.QueryRow(ctx, "INSERT INTO cdc_test_users (name) VALUES ($1) RETURNING id",
		strings.Repeat("x", 2<<20)).Scan(&bigID); err != nil {
		t.Fatalf("insert big row: %v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO cdc_test_users (name) VALUES ('small')"); err != nil {
		t.Fatalf("insert small row: %v", err)
	}
	var afterWrites string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&afterWrites); err != nil {
		t.Fatalf("wal lsn: %v", err)
	}
	afterLSN, _ := pglogrepl.ParseLSN(afterWrites)

	waitUntil(t, 60*time.Second, "both rows in the stream", func() bool {
		stream, err := js.Stream(ctx, natsCfg.StreamName)
		return err == nil && stream.CachedInfo().State.Msgs >= 2
	})
	events := readStreamEvents(t, js, natsCfg.StreamName, 2)

	var key map[string]any
	if err := json.Unmarshal([]byte(events[0].Data), &key); err != nil {
		t.Fatalf("truncated data %q: %v", events[0].Data, err)
	}
	if !events[0].DataTruncated || len(key) != 1 || key["id"] != float64(bigID) {
		t.Errorf("first event = %+v, want truncated to id %d", events[0], bigID)
	}
	if events[1].DataTruncated || !strings.Contains(events[1].Data, "small") {
		t.Errorf("second event = %+v, want the small row in full", events[1])
	}
	waitUntil(t, 30*time.Second, "the slot to confirm past both rows", func() bool {
		return querySlotStatsDirect(t, connStr, pgCfg.SlotName).ConfirmedFlushLSN >= uint64(afterLSN)
	})
}
