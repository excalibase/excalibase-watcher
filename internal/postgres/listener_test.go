//go:build integration

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupPostgres(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("testuser"),
		tcpostgres.WithPassword("testpass"),
		tcpostgres.WithInitScripts(), // no init scripts
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_replication_slots=5",
					"-c", "max_wal_senders=5",
				},
			},
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("starting postgres container: %v", err)
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("getting connection string: %v", err)
	}

	// Create test table
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS cdc_test_users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			active BOOLEAN DEFAULT true
		)
	`)
	if err != nil {
		t.Fatalf("creating table: %v", err)
	}
	conn.Close(ctx)

	cleanup := func() {
		container.Terminate(ctx)
	}
	return connStr, cleanup
}

func makeConfig(connStr string) config.PostgresConfig {
	return config.PostgresConfig{
		Enabled:           true,
		URL:               connStr,
		SlotName:          "test_slot",
		PublicationName:   "test_pub",
		CreateSlot:        true,
		CreatePublication: true,
		SnapshotMode:      "none",
		SnapshotChunkSize: 10000,
	}
}

func waitForEvents(t *testing.T, ch <-chan cdc.Event, count int, timeout time.Duration) []cdc.Event {
	t.Helper()
	var events []cdc.Event
	deadline := time.After(timeout)
	for len(events) < count {
		select {
		case e := <-ch:
			events = append(events, e)
		case <-deadline:
			t.Fatalf("timeout waiting for events: got %d, want %d", len(events), count)
		}
	}
	return events
}

func drainUntilType(t *testing.T, ch <-chan cdc.Event, typ cdc.EventType, timeout time.Duration) cdc.Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case e := <-ch:
			if e.Type == typ {
				return e
			}
		case <-deadline:
			t.Fatalf("timeout waiting for event type %v", typ)
		}
	}
	return cdc.Event{} // unreachable
}

func TestCaptureInsert(t *testing.T) {
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

	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()
	svc.MarkRunning()

	ch, unsub := svc.SubscribeAll()
	defer unsub()

	time.Sleep(500 * time.Millisecond) // let replication connect

	// Insert a row
	conn, _ := pgx.Connect(ctx, stripReplicationParam(connStr))
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "INSERT INTO cdc_test_users (name, email) VALUES ('Alice', 'alice@test.com')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	event := drainUntilType(t, ch, cdc.Insert, 10*time.Second)
	if event.Table != "cdc_test_users" {
		t.Errorf("table = %q, want cdc_test_users", event.Table)
	}
	if event.Schema != "public" {
		t.Errorf("schema = %q, want public", event.Schema)
	}
}

func TestCaptureUpdate(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	// Need REPLICA IDENTITY FULL for old values in UPDATE
	conn, _ := pgx.Connect(ctx, stripReplicationParam(connStr))
	_, _ = conn.Exec(ctx, "ALTER TABLE cdc_test_users REPLICA IDENTITY FULL")
	_, _ = conn.Exec(ctx, "INSERT INTO cdc_test_users (name, email) VALUES ('Alice', 'alice@test.com')")
	conn.Close(ctx)

	cfg := makeConfig(connStr)
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
	time.Sleep(500 * time.Millisecond)

	conn, _ = pgx.Connect(ctx, stripReplicationParam(connStr))
	defer conn.Close(ctx)
	_, _ = conn.Exec(ctx, "UPDATE cdc_test_users SET name = 'Bob' WHERE name = 'Alice'")

	event := drainUntilType(t, ch, cdc.Update, 10*time.Second)
	if event.Table != "cdc_test_users" {
		t.Errorf("table = %q", event.Table)
	}
	// Should contain old and new data
	if event.Data == "" {
		t.Error("UPDATE data should not be empty")
	}
}

func TestCaptureDelete(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	conn, _ := pgx.Connect(ctx, stripReplicationParam(connStr))
	_, _ = conn.Exec(ctx, "ALTER TABLE cdc_test_users REPLICA IDENTITY FULL")
	_, _ = conn.Exec(ctx, "INSERT INTO cdc_test_users (name, email) VALUES ('Alice', 'alice@test.com')")
	conn.Close(ctx)

	cfg := makeConfig(connStr)
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
	time.Sleep(500 * time.Millisecond)

	conn, _ = pgx.Connect(ctx, stripReplicationParam(connStr))
	defer conn.Close(ctx)
	_, _ = conn.Exec(ctx, "DELETE FROM cdc_test_users WHERE name = 'Alice'")

	event := drainUntilType(t, ch, cdc.Delete, 10*time.Second)
	if event.Table != "cdc_test_users" {
		t.Errorf("table = %q", event.Table)
	}
}

func TestCaptureTruncate(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	conn, _ := pgx.Connect(ctx, stripReplicationParam(connStr))
	_, _ = conn.Exec(ctx, "INSERT INTO cdc_test_users (name) VALUES ('Alice')")
	conn.Close(ctx)

	cfg := makeConfig(connStr)
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
	time.Sleep(500 * time.Millisecond)

	conn, _ = pgx.Connect(ctx, stripReplicationParam(connStr))
	defer conn.Close(ctx)
	_, _ = conn.Exec(ctx, "TRUNCATE cdc_test_users")

	event := drainUntilType(t, ch, cdc.Truncate, 10*time.Second)
	if event.Table != "cdc_test_users" {
		t.Errorf("table = %q", event.Table)
	}
}

func TestBeginAndCommitEvents(t *testing.T) {
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
	_, _ = conn.Exec(ctx, "INSERT INTO cdc_test_users (name) VALUES ('Alice')")

	// Should see BEGIN, INSERT, COMMIT
	var types []cdc.EventType
	deadline := time.After(10 * time.Second)
	for len(types) < 3 {
		select {
		case e := <-ch:
			types = append(types, e.Type)
		case <-deadline:
			t.Fatalf("timeout: got types %v", types)
		}
	}

	if types[0] != cdc.Begin {
		t.Errorf("first event = %v, want BEGIN", types[0])
	}
	// INSERT may not be second if relation message comes first
	foundInsert := false
	foundCommit := false
	for _, typ := range types {
		if typ == cdc.Insert {
			foundInsert = true
		}
		if typ == cdc.Commit {
			foundCommit = true
		}
	}
	if !foundInsert {
		t.Error("expected INSERT event")
	}
	if !foundCommit {
		t.Error("expected COMMIT event")
	}
}

func TestTableFiltering(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	// Create another table
	conn, _ := pgx.Connect(ctx, stripReplicationParam(connStr))
	_, _ = conn.Exec(ctx, "CREATE TABLE other_table (id SERIAL PRIMARY KEY, value TEXT)")
	conn.Close(ctx)

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeConfig(connStr)
	cfg.Tables = []string{"cdc_test_users"} // Only watch this table

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
	time.Sleep(500 * time.Millisecond)

	conn, _ = pgx.Connect(ctx, stripReplicationParam(connStr))
	defer conn.Close(ctx)

	// Insert into filtered-out table
	_, _ = conn.Exec(ctx, "INSERT INTO other_table (value) VALUES ('should_be_filtered')")
	// Insert into watched table
	_, _ = conn.Exec(ctx, "INSERT INTO cdc_test_users (name) VALUES ('watched')")

	event := drainUntilType(t, ch, cdc.Insert, 10*time.Second)
	if event.Table != "cdc_test_users" {
		t.Errorf("got event for filtered table: %q", event.Table)
	}
}

func TestChunkedSnapshot(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate rows
	conn, _ := pgx.Connect(ctx, stripReplicationParam(connStr))
	for i := 0; i < 5; i++ {
		_, _ = conn.Exec(ctx, fmt.Sprintf("INSERT INTO cdc_test_users (name) VALUES ('user_%d')", i))
	}
	conn.Close(ctx)

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeConfig(connStr)
	cfg.SnapshotMode = "chunked"
	cfg.SnapshotChunkSize = 2 // small chunks to test pagination

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
	svc.MarkRunning()

	// Should receive 5 snapshot INSERT events
	events := waitForEvents(t, ch, 5, 10*time.Second)
	for _, e := range events {
		if e.Type != cdc.Insert {
			t.Errorf("snapshot event type = %v, want INSERT", e.Type)
		}
		if e.Table != "cdc_test_users" {
			t.Errorf("snapshot table = %q", e.Table)
		}
	}
}

func TestDDLCapture(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeConfig(connStr)
	cfg.CaptureDDL = true

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
	time.Sleep(500 * time.Millisecond)

	// Execute DDL
	conn, _ := pgx.Connect(ctx, stripReplicationParam(connStr))
	defer conn.Close(ctx)
	_, _ = conn.Exec(ctx, "ALTER TABLE cdc_test_users ADD COLUMN age INT")

	event := drainUntilType(t, ch, cdc.DDL, 10*time.Second)
	if event.Type != cdc.DDL {
		t.Errorf("type = %v, want DDL", event.Type)
	}
}
