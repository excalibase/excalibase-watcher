//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/metrics"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func querySlotStatsDirect(t *testing.T, connStr, slot string) SlotStats {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, stripReplicationParam(connStr))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	stats, err := querySlotStats(ctx, conn, slot)
	if err != nil {
		t.Fatalf("querySlotStats: %v", err)
	}
	return stats
}

func resetSlotGauges() {
	metrics.SetSlotStats(0, 0, 0)
	metrics.SetLagSeconds(0)
	metrics.SetLastEventTimestamp(time.Unix(0, 0))
}

func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestSlotGaugesMoveAfterInsert(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()
	resetSlotGauges() // gauges are process-global; earlier tests leave values from other containers

	cfg := makeConfig(connStr)
	cfg.SlotStatsIntervalSeconds = 1
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()
	svc.MarkRunning()

	ch, unsub := svc.SubscribeAll()
	defer unsub()

	waitUntil(t, 10*time.Second, "first slot sample", func() bool {
		return testutil.ToFloat64(metrics.SlotRestartLSN) > 0
	})
	confirmedBefore := testutil.ToFloat64(metrics.SlotConfirmedFlushLSN)

	conn, _ := pgx.Connect(ctx, stripReplicationParam(connStr))
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "INSERT INTO cdc_test_users (name) VALUES ('lag')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	event := drainUntilType(t, ch, cdc.Insert, 10*time.Second)
	eventLSN, err := pglogrepl.ParseLSN(event.LSN)
	if err != nil {
		t.Fatalf("event LSN %q: %v", event.LSN, err)
	}

	if got := testutil.ToFloat64(metrics.LastEventTimestampSeconds); time.Since(time.Unix(int64(got), 0)) > time.Minute {
		t.Errorf("cdc_last_event_timestamp_seconds = %v, want recent", got)
	}
	waitUntil(t, 30*time.Second, "confirmed_flush_lsn to advance past the insert", func() bool {
		return testutil.ToFloat64(metrics.SlotConfirmedFlushLSN) >= float64(eventLSN)
	})
	if got := testutil.ToFloat64(metrics.SlotConfirmedFlushLSN); got <= confirmedBefore {
		t.Errorf("cdc_slot_confirmed_flush_lsn did not move: before=%v after=%v", confirmedBefore, got)
	}
	waitUntil(t, 30*time.Second, "lag to reset to 0 once caught up", func() bool {
		return testutil.ToFloat64(metrics.LagSeconds) == 0
	})
}

func TestConfirmedFlushWaitsForPublishAck(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeConfig(connStr)
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	onPublished := listener.PublishedObserver() // gate acks on a (simulated) NATS publisher
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()
	svc.MarkRunning()

	ch, unsub := svc.SubscribeAll()
	defer unsub()
	time.Sleep(500 * time.Millisecond)

	conn, _ := pgx.Connect(ctx, stripReplicationParam(connStr))
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "INSERT INTO cdc_test_users (name) VALUES ('unacked')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	event := drainUntilType(t, ch, cdc.Insert, 10*time.Second)
	eventLSN, err := pglogrepl.ParseLSN(event.LSN)
	if err != nil {
		t.Fatalf("event LSN %q: %v", event.LSN, err)
	}

	// Longer than the periodic status interval: the watcher must keep the slot
	// behind the insert while the event is not acknowledged by NATS.
	time.Sleep(statusInterval + 3*time.Second)
	if got := querySlotStatsDirect(t, connStr, cfg.SlotName).ConfirmedFlushLSN; got >= uint64(eventLSN) {
		t.Fatalf("confirmed_flush_lsn %s advanced past unacked event %s", pglogrepl.LSN(got), eventLSN)
	}

	onPublished(event)
	waitUntil(t, 30*time.Second, "confirmed_flush_lsn to advance after ack", func() bool {
		return querySlotStatsDirect(t, connStr, cfg.SlotName).ConfirmedFlushLSN >= uint64(eventLSN)
	})
}
