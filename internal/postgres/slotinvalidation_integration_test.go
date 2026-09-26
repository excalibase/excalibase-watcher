//go:build integration

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// A watcher that never gets its events acked holds the slot. Once the WAL it
// pins passes max_slot_wal_keep_size, a checkpoint invalidates the slot. The
// watcher recreates it at the current position, counts the gap, and streams
// new changes again instead of staying stuck.
func TestInvalidatedSlotIsRecreatedAndStreamingResumes(t *testing.T) {
	connStr, cleanup := setupPostgresWith(t, "max_slot_wal_keep_size=32MB")
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()
	cfg := makeConfig(connStr)
	cfg.SlotStatsIntervalSeconds = 1
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	listener.PublishedObserver()
	ch, unsub := svc.SubscribeAll()
	defer unsub()
	resumed := signalOnInsert(ch, "after-recovery")
	recreatedBefore := testutil.ToFloat64(metrics.SlotsRecreated)
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()
	waitUntil(t, 30*time.Second, "the first slot sample", func() bool {
		return testutil.ToFloat64(metrics.SlotWALStatus.WithLabelValues("reserved")) == 1
	})
	if !listener.SlotUsable() {
		t.Fatal("a fresh slot must be usable")
	}

	conn, err := pgx.Connect(ctx, stripReplicationParam(connStr))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	writePastTheWALCap(t, ctx, conn)

	waitUntil(t, 90*time.Second, "the watcher to recreate the invalidated slot", func() bool {
		return testutil.ToFloat64(metrics.SlotsRecreated) > recreatedBefore
	})
	if got := querySlotStatsDirect(t, connStr, cfg.SlotName).WALStatus; got == walStatusLost {
		t.Fatalf("slot still %s after recreation", got)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO cdc_test_users (name) VALUES ('after-recovery')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	select {
	case <-resumed:
	case <-time.After(60 * time.Second):
		t.Fatal("no change streamed after the slot was recreated")
	}
	waitUntil(t, 30*time.Second, "readiness to recover", listener.SlotUsable)
}

func signalOnInsert(ch <-chan cdc.Event, marker string) <-chan struct{} {
	seen := make(chan struct{})
	go func() {
		for event := range ch {
			if event.Type == cdc.Insert && strings.Contains(event.Data, marker) {
				close(seen)
				return
			}
		}
	}()
	return seen
}

// writePastTheWALCap writes well over 32MB of WAL and checkpoints, which
// invalidates a slot that pins it.
func writePastTheWALCap(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	for range 5 {
		if _, err := conn.Exec(ctx,
			"INSERT INTO cdc_test_users (name) SELECT repeat('x', 1000) FROM generate_series(1, 20000)"); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if _, err := conn.Exec(ctx, "SELECT pg_switch_wal()"); err != nil {
			t.Fatalf("switch wal: %v", err)
		}
	}
	if _, err := conn.Exec(ctx, "CHECKPOINT"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
}
