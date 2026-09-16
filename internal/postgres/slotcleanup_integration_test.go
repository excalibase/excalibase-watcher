//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/excalibase/watcher-go/internal/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func slotExists(t *testing.T, conn *pgx.Conn, name string) bool {
	t.Helper()
	var exists bool
	err := conn.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", name).Scan(&exists)
	if err != nil {
		t.Fatalf("slot lookup: %v", err)
	}
	return exists
}

func TestOrphanSlotIsDroppedAndOwnedSlotsSurvive(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, stripReplicationParam(connStr))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	registry := newSlotRegistry("bootstrap", "bootstrap-owner")
	if err := registry.ensureTable(ctx, conn); err != nil {
		t.Fatalf("ensureTable: %v", err)
	}
	for _, slot := range []string{"cdc_orphan", "cdc_kept", "pgl_foreign"} {
		if _, err := conn.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot); err != nil {
			t.Fatalf("create slot %s: %v", slot, err)
		}
	}
	_, err = conn.Exec(ctx, `INSERT INTO `+slotOwnersTable+` (slot_name, owner_id, heartbeat_at) VALUES
		('cdc_orphan', 'dead-pod', now() - interval '2 hours'),
		('cdc_kept',   'live-pod', now())`)
	if err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	svc := cdc.NewService()
	defer svc.Shutdown()
	cfg := makeConfig(connStr)
	cfg.OwnerID = "this-pod"
	cfg.SlotCleanup = config.SlotCleanupConfig{
		Enabled: true, IntervalMinutes: 10, StaleAfterMinutes: 30, SlotPattern: "^(cdc_|test_)",
	}
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	before := testutil.ToFloat64(metrics.SlotsDropped)
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitUntil(t, 15*time.Second, "orphan slot to be dropped", func() bool {
		return !slotExists(t, conn, "cdc_orphan")
	})
	for _, slot := range []string{"cdc_kept", "pgl_foreign", cfg.SlotName} {
		if !slotExists(t, conn, slot) {
			t.Errorf("slot %s must survive cleanup", slot)
		}
	}
	if got := testutil.ToFloat64(metrics.SlotsDropped) - before; got != 1 {
		t.Errorf("cdc_slots_dropped_total delta = %v, want 1", got)
	}

	var ownerID string
	var releasedAt *time.Time
	err = conn.QueryRow(ctx, "SELECT owner_id, released_at FROM "+slotOwnersTable+" WHERE slot_name = $1",
		cfg.SlotName).Scan(&ownerID, &releasedAt)
	if err != nil {
		t.Fatalf("own registry row: %v", err)
	}
	if ownerID != "this-pod" || releasedAt != nil {
		t.Errorf("own row = (%s, released %v), want (this-pod, not released)", ownerID, releasedAt)
	}

	listener.Stop()
	err = conn.QueryRow(ctx, "SELECT released_at FROM "+slotOwnersTable+" WHERE slot_name = $1",
		cfg.SlotName).Scan(&releasedAt)
	if err != nil {
		t.Fatalf("own registry row after stop: %v", err)
	}
	if releasedAt == nil {
		t.Error("Stop must mark the own registry row released_at")
	}
	if !slotExists(t, conn, cfg.SlotName) {
		t.Error("a watcher never drops its own slot on shutdown")
	}
}

func TestRegistryHeartbeatIsNotPublishedAsEvent(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()
	cfg := makeConfig(connStr)
	cfg.SlotCleanup = config.SlotCleanupConfig{Enabled: true, IntervalMinutes: 10, StaleAfterMinutes: 30,
		SlotPattern: "^cdc_", HeartbeatSeconds: 1}
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	ch, unsub := svc.SubscribeAll()
	defer unsub()
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()

	deadline := time.After(3 * time.Second)
	for {
		select {
		case e := <-ch:
			if e.Table == slotOwnersTable {
				t.Fatalf("registry heartbeat leaked as %s event", e.Type)
			}
		case <-deadline:
			return
		}
	}
}
