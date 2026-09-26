//go:build integration

package postgres

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// selectiveAcker acknowledges every publishable event except those whose data
// contains held, which it keeps until release.
type selectiveAcker struct {
	held    string
	observe func(cdc.Event)
	mu      sync.Mutex
	kept    []cdc.Event
}

func (a *selectiveAcker) run(ch <-chan cdc.Event) {
	for event := range ch {
		if !cdc.IsPublishable(event.Type) {
			continue
		}
		if strings.Contains(event.Data, a.held) {
			a.mu.Lock()
			a.kept = append(a.kept, event)
			a.mu.Unlock()
			continue
		}
		a.observe(event)
	}
}

func (a *selectiveAcker) keptCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.kept)
}

func (a *selectiveAcker) release() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, event := range a.kept {
		a.observe(event)
	}
}

func beginAndInsert(t *testing.T, ctx context.Context, conn *pgx.Conn, name string) pgx.Tx {
	t.Helper()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO cdc_test_users (name) VALUES ($1)", name); err != nil {
		t.Fatalf("insert %s: %v", name, err)
	}
	return tx
}

func currentWALLSN(t *testing.T, ctx context.Context, conn *pgx.Conn) pglogrepl.LSN {
	t.Helper()
	var text string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&text); err != nil {
		t.Fatalf("pg_current_wal_lsn: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(text)
	if err != nil {
		t.Fatalf("parse lsn: %v", err)
	}
	return lsn
}

// A transaction that started first but committed last is streamed after the
// other one, with changes at lower WAL positions than changes already acked.
// Its commit must stay unconfirmed until its own changes are acked, or a
// crash in between skips it on restart.
func TestInterleavedTransactionHeldUntilItsChangesAreAcked(t *testing.T) {
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
	acker := &selectiveAcker{held: "early", observe: listener.PublishedObserver()}
	ch, unsub := svc.SubscribeAll()
	defer unsub()
	go acker.run(ch)
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()
	time.Sleep(500 * time.Millisecond)

	first, err := pgx.Connect(ctx, stripReplicationParam(connStr))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer first.Close(ctx)
	second, err := pgx.Connect(ctx, stripReplicationParam(connStr))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer second.Close(ctx)

	early := beginAndInsert(t, ctx, first, "early")
	late := beginAndInsert(t, ctx, second, "late")
	if err := late.Commit(ctx); err != nil {
		t.Fatalf("commit late: %v", err)
	}
	beforeEarlyCommit := currentWALLSN(t, ctx, second)
	if err := early.Commit(ctx); err != nil {
		t.Fatalf("commit early: %v", err)
	}

	waitUntil(t, 30*time.Second, "the early insert to reach the bus", func() bool {
		return acker.keptCount() == 1
	})
	time.Sleep(statusInterval + 3*time.Second)
	if got := querySlotStatsDirect(t, connStr, cfg.SlotName).ConfirmedFlushLSN; got > uint64(beforeEarlyCommit) {
		t.Fatalf("confirmed_flush_lsn %s passed the commit of the unacked transaction (at or after %s)",
			pglogrepl.LSN(got), beforeEarlyCommit)
	}

	acker.release()
	waitUntil(t, 30*time.Second, "confirmed_flush_lsn to pass the acked commit", func() bool {
		return querySlotStatsDirect(t, connStr, cfg.SlotName).ConfirmedFlushLSN > uint64(beforeEarlyCommit)
	})
}
