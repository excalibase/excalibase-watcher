//go:build integration

package postgres

import (
	"context"
	"io"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/jackc/pgx/v5"
)

// provisionWatcherRole grants exactly what provisioning grants the watcher:
// REPLICATION, and write access only to its own registry schema.
func provisionWatcherRole(t *testing.T, connStr string) string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `
CREATE ROLE cdc_watcher WITH LOGIN REPLICATION PASSWORD 'wP';
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA excalibase_cdc;
REVOKE ALL ON SCHEMA excalibase_cdc FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA excalibase_cdc TO cdc_watcher;
ALTER ROLE cdc_watcher IN DATABASE testdb SET search_path = excalibase_cdc;
CREATE PUBLICATION cdc_watcher_pub FOR TABLE public.cdc_test_users;`)
	if err != nil {
		t.Fatalf("provision watcher role: %v", err)
	}
	u, err := url.Parse(connStr)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	u.User = url.UserPassword("cdc_watcher", "wP")
	return u.String()
}

func TestListenerRunsWithTheProvisionedWatcherRole(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	cfg := makeConfig(provisionWatcherRole(t, connStr))
	cfg.SlotName = "cdc_watcher"
	cfg.PublicationName = "cdc_watcher_pub"
	cfg.CreatePublication = false
	cfg.OwnerID = "pod-1"
	cfg.SlotCleanup = config.SlotCleanupConfig{Enabled: true, IntervalMinutes: 10, StaleAfterMinutes: 30, SlotPattern: "^cdc_"}

	svc := cdc.NewService()
	defer svc.Shutdown()
	ch, unsub := svc.SubscribeAll()
	defer unsub()
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()

	if listener.registry == nil {
		t.Fatal("slot owner registry unavailable with the provisioned grants")
	}
	admin, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close(ctx)
	var owner string
	err = admin.QueryRow(ctx, `SELECT owner_id FROM excalibase_cdc.excalibase_cdc_slot_owners WHERE slot_name = 'cdc_watcher'`).Scan(&owner)
	if err != nil || owner != "pod-1" {
		t.Fatalf("registry row = (%q, %v), want pod-1 in excalibase_cdc", owner, err)
	}
	if _, err := admin.Exec(ctx, `INSERT INTO cdc_test_users (name, email) VALUES ('Ada', 'ada@example.com')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if e := drainUntilType(t, ch, cdc.Insert, 10*time.Second); e.Table != "cdc_test_users" {
		t.Errorf("insert event for %s, want cdc_test_users", e.Table)
	}
}

// forwardWhenUp listens on addr and relays every connection to target.
func forwardWhenUp(t *testing.T, addr, target string) {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			go relay(client, target)
		}
	}()
}

func relay(client net.Conn, target string) {
	defer client.Close()
	server, err := net.Dial("tcp", target)
	if err != nil {
		return
	}
	defer server.Close()
	go func() { _, _ = io.Copy(server, client) }()
	_, _ = io.Copy(client, server)
}

func unusedAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func TestListenerWaitsForADatabaseThatComesUpLate(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	real, err := url.Parse(connStr)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	late := *real
	late.Host = unusedAddress(t)

	svc := cdc.NewService()
	defer svc.Shutdown()
	ch, unsub := svc.SubscribeAll()
	defer unsub()
	listener, err := NewListener(makeConfig(late.String()), svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	started := make(chan error, 1)
	go func() { started <- listener.Start(ctx) }()

	select {
	case err := <-started:
		t.Fatalf("Start returned %v while the database was still unreachable", err)
	case <-time.After(3 * time.Second):
	}

	forwardWhenUp(t, late.Host, real.Host)
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("Start after the database came up: %v", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("listener never connected once the database came up")
	}
	defer listener.Stop()

	admin, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, `INSERT INTO cdc_test_users (name, email) VALUES ('Late', 'late@example.com')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	drainUntilType(t, ch, cdc.Insert, 10*time.Second)
}
