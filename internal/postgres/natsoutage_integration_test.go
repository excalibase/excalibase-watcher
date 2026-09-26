//go:build integration

package postgres

import (
	"context"
	"fmt"
	"sync"
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

// insertLSNs records the WAL position of every insert the listener hands to
// the bus, independently of whether NATS ever acknowledges it.
type insertLSNs struct {
	mu   sync.Mutex
	lsns []pglogrepl.LSN
}

func (r *insertLSNs) record(t *testing.T, ch <-chan cdc.Event) {
	for event := range ch {
		if event.Type != cdc.Insert {
			continue
		}
		lsn, err := pglogrepl.ParseLSN(event.LSN)
		if err != nil {
			t.Errorf("insert LSN %q: %v", event.LSN, err)
			continue
		}
		r.mu.Lock()
		r.lsns = append(r.lsns, lsn)
		r.mu.Unlock()
	}
}

func (r *insertLSNs) snapshot() []pglogrepl.LSN {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]pglogrepl.LSN(nil), r.lsns...)
}

// While NATS is down nothing is published, so the slot must not be confirmed
// past any of the inserts; once it is back they are all published exactly
// once and the slot catches up.
func TestSlotHeldWhileNATSIsDown(t *testing.T) {
	const rows = 5
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	env := natstest.Start(t)
	natsCfg := config.NATSConfig{
		URL:           env.URL,
		StreamName:    "CDC_SLOT_HELD",
		SubjectPrefix: "cdc.proj",
	}
	js, err := jetstream.New(env.Connect(t))
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: natsCfg.StreamName, Subjects: []string{natsCfg.SubjectPrefix + ".>"}, Storage: jetstream.FileStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	env.Stop()

	svc := cdc.NewService()
	defer svc.Shutdown()
	pub := natspub.NewPublisher(natsCfg, svc)
	cfg := makeConfig(connStr)
	listener, err := NewListener(cfg, svc)
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

	observed := &insertLSNs{}
	ch, unsub := svc.SubscribeAll()
	defer unsub()
	go observed.record(t, ch)
	time.Sleep(500 * time.Millisecond)

	conn, err := pgx.Connect(ctx, stripReplicationParam(connStr))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	for i := 1; i <= rows; i++ {
		if _, err := conn.Exec(ctx, "INSERT INTO cdc_test_users (name) VALUES ($1)", fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}
	waitUntil(t, 30*time.Second, "listener to hand every insert to the bus", func() bool {
		return len(observed.snapshot()) == rows
	})
	first := observed.snapshot()[0]

	// Longer than the status interval, so a confirm would have been sent.
	time.Sleep(statusInterval + 3*time.Second)
	if pub.Ready() {
		t.Fatal("publisher ready while NATS is down")
	}
	if got := querySlotStatsDirect(t, connStr, cfg.SlotName).ConfirmedFlushLSN; got >= uint64(first) {
		t.Fatalf("confirmed_flush_lsn %s advanced past unpublished insert %s", pglogrepl.LSN(got), first)
	}

	env.Start(t)
	waitUntil(t, 60*time.Second, "publisher to connect once NATS is back", pub.Ready)
	waitUntil(t, 30*time.Second, "every insert in the stream", func() bool {
		stream, err := js.Stream(ctx, natsCfg.StreamName)
		return err == nil && stream.CachedInfo().State.Msgs >= rows
	})
	stream, err := js.Stream(ctx, natsCfg.StreamName)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if got := stream.CachedInfo().State.Msgs; got != rows {
		t.Fatalf("stream holds %d messages, want exactly %d", got, rows)
	}
	last := observed.snapshot()[rows-1]
	waitUntil(t, 30*time.Second, "confirmed_flush_lsn to reach the last published insert", func() bool {
		return querySlotStatsDirect(t, connStr, cfg.SlotName).ConfirmedFlushLSN >= uint64(last)
	})
}

func insertThreeRows(t *testing.T, connStr string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, stripReplicationParam(connStr))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "INSERT INTO cdc_test_users (name) VALUES ('a'), ('b'), ('c')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
}

// captureInsertSourceIDs runs a gated listener that never acks, optionally
// writes three rows, and returns the source ids of the first three inserts.
func captureInsertSourceIDs(t *testing.T, cfg config.PostgresConfig, write func()) []string {
	t.Helper()
	svc := cdc.NewService()
	defer svc.Shutdown()
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	listener.PublishedObserver()
	ch, unsub := svc.SubscribeAll()
	defer unsub()
	if err := listener.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()
	if write != nil {
		time.Sleep(500 * time.Millisecond)
		write()
	}
	var ids []string
	for len(ids) < 3 {
		ids = append(ids, drainUntilType(t, ch, cdc.Insert, 30*time.Second).SourceID)
	}
	return ids
}

// A transaction Postgres streams again after a restart (the slot was never
// confirmed past it) carries the same source ids, which is what lets the
// stream drop the re-published copies.
func TestReStreamedTransactionKeepsItsSourceIDs(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	cfg := makeConfig(connStr)

	first := captureInsertSourceIDs(t, cfg, func() { insertThreeRows(t, connStr) })
	again := captureInsertSourceIDs(t, cfg, nil)
	for i := range first {
		if first[i] == "" || again[i] != first[i] {
			t.Fatalf("source ids first run %v, after restart %v", first, again)
		}
	}
	if first[0] == first[1] || first[1] == first[2] {
		t.Fatalf("rows of one statement share an id: %v", first)
	}
}
