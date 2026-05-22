//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	_ "github.com/go-mysql-org/go-mysql/driver"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupMySQL(t *testing.T) (string, int, func()) {
	t.Helper()
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mysql:8.0",
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": "testpass",
				"MYSQL_DATABASE":      "testdb",
			},
			Cmd: []string{
				"--log-bin=mysql-bin",
				"--binlog-format=ROW",
				"--binlog-row-image=FULL",
				"--server-id=1",
			},
			WaitingFor: wait.ForLog("ready for connections").
				WithOccurrence(2).
				WithStartupTimeout(180 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("starting mysql container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatal(err)
	}

	addr := fmt.Sprintf("%s:%s", host, port.Port())

	// Create test table using go-mysql driver
	dsn := fmt.Sprintf("root:testpass@%s:%s/testdb", host, port.Port())
	var db *sql.DB
	for i := 0; i < 30; i++ {
		db, err = sql.Open("mysql", dsn)
		if err == nil {
			if err = db.Ping(); err == nil {
				break
			}
			db.Close()
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		t.Fatalf("connecting after retries: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS cdc_test_users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(200),
			active BOOLEAN DEFAULT true
		)
	`)
	if err != nil {
		t.Fatalf("creating table: %v", err)
	}

	cleanup := func() {
		container.Terminate(ctx)
	}
	return addr, port.Int(), cleanup
}

func makeMySQLConfig(addr string) config.MySQLConfig {
	return config.MySQLConfig{
		Enabled:           true,
		Host:              addr,
		Username:          "root",
		Password:          "testpass",
		Schema:            "testdb",
		SnapshotMode:      "none",
		SnapshotChunkSize: 10000,
	}
}

func execMySQL(t *testing.T, addr, query string) {
	t.Helper()
	dsn := fmt.Sprintf("root:testpass@%s/testdb", addr)
	var db *sql.DB
	var err error
	for i := 0; i < 5; i++ {
		db, err = sql.Open("mysql", dsn)
		if err == nil {
			if err = db.Ping(); err == nil {
				break
			}
			db.Close()
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer db.Close()
	_, err = db.Exec(query)
	if err != nil {
		t.Fatalf("executing %q: %v", query, err)
	}
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
	return cdc.Event{}
}

func TestMySQLCaptureInsert(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeMySQLConfig(addr)
	cfg.Tables = []string{"cdc_test_users"} // filter to our table only
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}

	// Subscribe to specific table to avoid system event noise
	ch, unsub := svc.Subscribe("cdc_test_users")
	defer unsub()

	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()
	svc.MarkRunning()

	time.Sleep(3 * time.Second) // let canal connect and sync

	execMySQL(t, addr, "INSERT INTO cdc_test_users (name, email) VALUES ('Alice', 'alice@test.com')")

	event := drainUntilType(t, ch, cdc.Insert, 15*time.Second)
	if event.Table != "cdc_test_users" {
		t.Errorf("table = %q, want cdc_test_users", event.Table)
	}
	if event.Schema != "testdb" {
		t.Errorf("schema = %q, want testdb", event.Schema)
	}
}

func TestMySQLCaptureUpdate(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()
	ctx := context.Background()

	execMySQL(t, addr, "INSERT INTO cdc_test_users (name, email) VALUES ('Alice', 'alice@test.com')")

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeMySQLConfig(addr)
	cfg.Tables = []string{"cdc_test_users"}
	ch, unsub := svc.Subscribe("cdc_test_users")
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
	time.Sleep(3 * time.Second)

	execMySQL(t, addr, "UPDATE cdc_test_users SET name = 'Bob' WHERE name = 'Alice'")

	event := drainUntilType(t, ch, cdc.Update, 15*time.Second)
	if event.Table != "cdc_test_users" {
		t.Errorf("table = %q", event.Table)
	}
	if event.Data == "" {
		t.Error("UPDATE data should not be empty")
	}
}

func TestMySQLCaptureDelete(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()
	ctx := context.Background()

	execMySQL(t, addr, "INSERT INTO cdc_test_users (name, email) VALUES ('Alice', 'alice@test.com')")

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeMySQLConfig(addr)
	cfg.Tables = []string{"cdc_test_users"}
	ch, unsub := svc.Subscribe("cdc_test_users")
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
	time.Sleep(3 * time.Second)

	execMySQL(t, addr, "DELETE FROM cdc_test_users WHERE name = 'Alice'")

	event := drainUntilType(t, ch, cdc.Delete, 15*time.Second)
	if event.Table != "cdc_test_users" {
		t.Errorf("table = %q", event.Table)
	}
}

func TestMySQLCaptureXid(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeMySQLConfig(addr)
	cfg.Tables = []string{"cdc_test_users"}

	// XID (COMMIT) goes to global channel
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
	time.Sleep(3 * time.Second)

	execMySQL(t, addr, "INSERT INTO cdc_test_users (name) VALUES ('Xid test')")

	event := drainUntilType(t, ch, cdc.Commit, 15*time.Second)
	if event.Type != cdc.Commit {
		t.Errorf("type = %v, want COMMIT", event.Type)
	}
}

func TestMySQLDDLCapture(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()
	ctx := context.Background()

	svc := cdc.NewService()
	defer svc.Shutdown()

	cfg := makeMySQLConfig(addr)
	cfg.Tables = []string{"cdc_test_users"}

	// DDL goes to global channel
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
	time.Sleep(3 * time.Second)

	execMySQL(t, addr, "ALTER TABLE cdc_test_users ADD COLUMN age INT")

	event := drainUntilType(t, ch, cdc.DDL, 15*time.Second)
	if event.Type != cdc.DDL {
		t.Errorf("type = %v, want DDL", event.Type)
	}
}
