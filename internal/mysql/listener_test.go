//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/go-mysql-org/go-mysql/driver"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupMySQL(t *testing.T) (string, int, func()) {
	t.Helper()
	return setupMySQLWithFlags(t)
}

// mysqlCAs is the CA each started server's certificate is signed by, by
// address, so clients verify it as the platform's do (EXC-589).
var mysqlCAs sync.Map

const mysqlTLSDir = "/etc/mysql-tls"

// setupMySQLWithFlags starts MySQL with extra server flags on top of the
// row-based binlog ones every test needs. It serves a certificate from its
// own CA for localhost, refuses plaintext, and root must use TLS.
func setupMySQLWithFlags(t *testing.T, flags ...string) (string, int, func()) {
	t.Helper()
	ctx := context.Background()
	ca := newTestCA(t)
	certPEM, keyPEM := ca.issue(t, "localhost")
	files := []testcontainers.ContainerFile{
		{Reader: strings.NewReader(ca.pem), ContainerFilePath: mysqlTLSDir + "/ca.pem", FileMode: 0o644},
		{Reader: strings.NewReader(certPEM), ContainerFilePath: mysqlTLSDir + "/server.pem", FileMode: 0o644},
		{Reader: strings.NewReader(keyPEM), ContainerFilePath: mysqlTLSDir + "/server-key.pem", FileMode: 0o644},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mysql:8.0",
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": "testpass",
				"MYSQL_DATABASE":      "testdb",
			},
			Files: files,
			Cmd: append([]string{
				"--log-bin=mysql-bin",
				"--binlog-format=ROW",
				"--binlog-row-image=FULL",
				"--server-id=1",
				"--ssl-ca=" + mysqlTLSDir + "/ca.pem",
				"--ssl-cert=" + mysqlTLSDir + "/server.pem",
				"--ssl-key=" + mysqlTLSDir + "/server-key.pem",
				"--require-secure-transport=ON",
			}, flags...),
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
	mysqlCAs.Store(addr, ca.pem)
	if err := driver.SetCustomTLSConfig("mysql://"+addr, []byte(ca.pem), nil, nil, false, host); err != nil {
		t.Fatal(err)
	}

	db := openTestDB(t, addr, 30, 2*time.Second)
	defer db.Close()
	for _, statement := range []string{
		"ALTER USER 'root'@'%' REQUIRE SSL",
		`CREATE TABLE IF NOT EXISTS cdc_test_users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(200),
			active BOOLEAN DEFAULT true
		)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	cleanup := func() {
		container.Terminate(ctx)
	}
	return addr, port.Int(), cleanup
}

// openTestDB opens addr as root over TLS verified against its CA, retrying
// while the server starts.
func openTestDB(t *testing.T, addr string, attempts int, pause time.Duration) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("root:testpass@%s/testdb?ssl=custom", addr)
	var db *sql.DB
	var err error
	for i := 0; i < attempts; i++ {
		db, err = sql.Open("mysql", dsn)
		if err == nil {
			if err = db.Ping(); err == nil {
				return db
			}
			db.Close()
		}
		time.Sleep(pause)
	}
	t.Fatalf("connecting after retries: %v", err)
	return nil
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
		CACert:            caOf(addr),
	}
}

func caOf(addr string) string {
	ca, _ := mysqlCAs.Load(addr)
	caPEM, _ := ca.(string)
	return caPEM
}

func execMySQL(t *testing.T, addr, query string) {
	t.Helper()
	db := openTestDB(t, addr, 5, time.Second)
	defer db.Close()
	if _, err := db.Exec(query); err != nil {
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
			return cdc.Event{}
		}
	}
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

// A binlog stream that stops while the listener should be running must
// surface, so the watcher exits instead of idling with CDC stalled.
func TestStreamingErrorIsReportedAsAFailure(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()

	offsetFile := filepath.Join(t.TempDir(), "binlog.offset")
	if err := NewFileOffsetStore(offsetFile).Save(BinlogPosition{File: "mysql-bin.999999", Position: 4}); err != nil {
		t.Fatal(err)
	}
	cfg := makeMySQLConfig(addr)
	cfg.OffsetFile = offsetFile
	svc := cdc.NewService()
	defer svc.Shutdown()
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	if err := listener.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()

	select {
	case err := <-listener.Failed():
		if err == nil {
			t.Fatal("failure delivered without an error")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("streaming from a missing binlog file was never reported")
	}
}
