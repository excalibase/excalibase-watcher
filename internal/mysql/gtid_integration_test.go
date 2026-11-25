//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/excalibase/watcher-go/internal/natstest"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go/network"
)

// streamUsers starts a listener over cfg and returns the names of the rows it
// delivers; stop ends it the way a clean shutdown does.
func streamUsers(t *testing.T, cfg config.MySQLConfig) (names <-chan string, stop func(), err error) {
	t.Helper()
	svc := cdc.NewService()
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatal(err)
	}
	events, unsub := svc.Subscribe("cdc_test_users")
	if err := listener.Start(context.Background()); err != nil {
		unsub()
		svc.Shutdown()
		return nil, nil, err
	}
	svc.MarkRunning()
	out := make(chan string, 1000)
	go func() {
		for event := range events {
			if event.Type == cdc.Insert {
				out <- nameOf(t, event.Data)
			}
		}
	}()
	return out, func() { listener.Stop(); unsub(); svc.Shutdown() }, nil
}

func nameOf(t *testing.T, data string) string {
	t.Helper()
	const key = `"name":"`
	i := strings.Index(data, key)
	if i < 0 {
		t.Errorf("no name in %s", data)
		return ""
	}
	rest := data[i+len(key):]
	return rest[:strings.Index(rest, `"`)]
}

func expectNames(t *testing.T, names <-chan string, want ...string) {
	t.Helper()
	got := map[string]int{}
	deadline := time.After(60 * time.Second)
	for len(got) < len(want) {
		select {
		case name := <-names:
			got[name]++
		case <-deadline:
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	time.Sleep(2 * time.Second) // a repeated delivery would land by now
	for {
		select {
		case name := <-names:
			got[name]++
		default:
			for _, name := range want {
				if got[name] != 1 {
					t.Errorf("%s delivered %d times, want once (got %v)", name, got[name], got)
				}
			}
			if len(got) != len(want) {
				t.Errorf("delivered %v, want exactly %v", got, want)
			}
			return
		}
	}
}

func TestStartRefusesAServerWithoutGTID(t *testing.T) {
	addr, _, cleanup := setupMySQLWithFlags(t, "--gtid-mode=OFF", "--enforce-gtid-consistency=OFF", "--binlog-row-image=MINIMAL")
	defer cleanup()
	cfg := makeMySQLConfig(addr)
	cfg.OffsetFile = filepath.Join(t.TempDir(), "o.json")
	if _, _, err := streamUsers(t, cfg); !errors.Is(err, ErrServerNotReady) {
		t.Fatalf("Start err = %v, want ErrServerNotReady", err)
	} else {
		for _, want := range []string{"gtid_mode", "enforce_gtid_consistency", "binlog_row_image"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q lacks %s", err, want)
			}
		}
	}
}

func TestStartNeedsReplicationGrantsAndStreamsOnceTheyAreGiven(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()
	for _, statement := range []string{
		"CREATE USER 'cdc'@'%' IDENTIFIED BY 'cdcpass' REQUIRE SSL",
		"GRANT SELECT ON testdb.* TO 'cdc'@'%'",
	} {
		execMySQL(t, addr, statement)
	}
	cfg := makeMySQLConfig(addr)
	cfg.Username, cfg.Password = "cdc", "cdcpass"
	cfg.Tables = []string{"cdc_test_users"}

	_, _, err := streamUsers(t, cfg)
	if !errors.Is(err, ErrServerNotReady) || !strings.Contains(err.Error(), "REPLICATION SLAVE") ||
		!strings.Contains(err.Error(), "REPLICATION CLIENT") {
		t.Fatalf("Start err = %v, want ErrServerNotReady naming the replication grants", err)
	}

	execMySQL(t, addr, "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%'")
	names, stop, err := streamUsers(t, cfg)
	if err != nil {
		t.Fatalf("Start with the least privileges: %v", err)
	}
	defer stop()
	insertRows(t, addr, "least", 1)
	expectNames(t, names, "least-1")
}

// A restart resumes after the last saved set: what happened while the watcher
// was down arrives once, what it already delivered does not arrive again.
func TestRestartResumesByGTIDWithoutLossOrRepeat(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()
	cfg := makeMySQLConfig(addr)
	cfg.OffsetFile = filepath.Join(t.TempDir(), "o.json")

	names, stop, err := streamUsers(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	insertRows(t, addr, "before", 2)
	expectNames(t, names, "before-1", "before-2")
	stop()

	insertRows(t, addr, "while-down", 2)
	names, stop, err = streamUsers(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	insertRows(t, addr, "after", 1)
	expectNames(t, names, "while-down-1", "while-down-2", "after-1")
}

// First start begins at the server's present, not at the start of its binlog.
func TestFirstStartDoesNotReplayHistory(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()
	insertRows(t, addr, "history", 3)
	cfg := makeMySQLConfig(addr)
	cfg.OffsetFile = filepath.Join(t.TempDir(), "o.json")

	names, stop, err := streamUsers(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	insertRows(t, addr, "new", 1)
	expectNames(t, names, "new-1")
	if saved, _ := NewFileOffsetStore(cfg.OffsetFile).Load(); saved.GTIDSet == "" {
		t.Error("the first start saved no offset")
	}
}

func TestPurgedOffsetFailsLoudlyAndResetOffsetIsTheWayOut(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()
	cfg := makeMySQLConfig(addr)
	cfg.OffsetFile = filepath.Join(t.TempDir(), "o.json")

	names, stop, err := streamUsers(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	insertRows(t, addr, "old", 1)
	expectNames(t, names, "old-1")
	stop()

	insertRows(t, addr, "lost", 1)
	execMySQL(t, addr, "FLUSH BINARY LOGS")
	execMySQL(t, addr, "PURGE BINARY LOGS BEFORE NOW() + INTERVAL 1 DAY")

	if _, _, err := streamUsers(t, cfg); !errors.Is(err, ErrGTIDPurged) {
		t.Fatalf("Start err = %v, want ErrGTIDPurged", err)
	}
	if kept, _ := NewFileOffsetStore(cfg.OffsetFile).Load(); kept.GTIDSet == "" {
		t.Error("a refused start must keep the saved offset, so the operator still sees what was lost")
	}

	cfg.ResetOffset = "after-purge"
	names, stop, err = streamUsers(t, cfg)
	if err != nil {
		t.Fatalf("Start with the reset token: %v", err)
	}
	defer stop()
	insertRows(t, addr, "fresh", 1)
	expectNames(t, names, "fresh-1")
}

// A saved set from some other database is refused, not silently adopted.
func TestOffsetOfAnotherServerIsRefused(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()
	cfg := makeMySQLConfig(addr)
	cfg.OffsetFile = filepath.Join(t.TempDir(), "o.json")
	if err := NewFileOffsetStore(cfg.OffsetFile).Save(OffsetRecord{GTIDSet: serverB + ":1-50"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := streamUsers(t, cfg); !errors.Is(err, ErrOffsetAheadOfServer) {
		t.Fatalf("Start err = %v, want ErrOffsetAheadOfServer", err)
	}
}

// MOCO failover: the primary goes away and the watcher is pointed at the
// replica that took over. The saved GTID set means the same there, so the
// watcher continues where it was, with no loss and no repeat.
func TestFailoverToAReplicaKeepsTheWatchersPlace(t *testing.T) {
	ctx := context.Background()
	net, err := network.New(ctx)
	if err != nil {
		t.Fatalf("network: %v", err)
	}
	t.Cleanup(func() { _ = net.Remove(ctx) })

	primary, _, stopPrimary := setupMySQLOn(t, mysqlNode{network: net.Name, alias: "primary"}, "--server-id=1")
	defer stopPrimary()
	replica, _, stopReplica := setupMySQLOn(t, mysqlNode{network: net.Name, alias: "replica", bare: true}, "--server-id=2")
	defer stopReplica()
	execMySQL(t, primary, "CREATE USER 'repl'@'%' IDENTIFIED BY 'replpass'")
	execMySQL(t, primary, "GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%'")
	execMySQL(t, replica, "CHANGE REPLICATION SOURCE TO SOURCE_HOST='primary', SOURCE_USER='repl', "+
		"SOURCE_PASSWORD='replpass', SOURCE_AUTO_POSITION=1, SOURCE_SSL=1")
	execMySQL(t, replica, "START REPLICA")

	cfg := makeMySQLConfig(primary)
	cfg.OffsetFile = filepath.Join(t.TempDir(), "o.json")
	names, stop, err := streamUsers(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	insertRows(t, primary, "p", 2)
	expectNames(t, names, "p-1", "p-2")
	stop()
	insertRows(t, primary, "gap", 2)
	waitReplicaCaughtUp(t, primary, replica)
	stopPrimary()

	cfg.Host, cfg.CACert = replica, caOf(replica)
	names, stop, err = streamUsers(t, cfg)
	if err != nil {
		t.Fatalf("Start on the replica: %v", err)
	}
	defer stop()
	expectNames(t, names, "gap-1", "gap-2")
}

func waitReplicaCaughtUp(t *testing.T, primary, replica string) {
	t.Helper()
	db := openTestDB(t, primary, 5, time.Second)
	var executed string
	if err := db.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		t.Fatal(err)
	}
	db.Close()
	replicaDB := openTestDB(t, replica, 5, time.Second)
	defer replicaDB.Close()
	var reached sql.NullInt64
	if err := replicaDB.QueryRow("SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 60)", strings.Join(strings.Fields(executed), "")).
		Scan(&reached); err != nil || reached.Int64 != 0 {
		t.Fatalf("replica did not catch up: %v %v", reached, err)
	}
}

func TestKVOffsetStore(t *testing.T) {
	env := natstest.Start(t)
	js, err := jetstream.New(env.Connect(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Three replicas on a one-node NATS: the bucket falls back to one.
	a, err := NewKVOffsetStore(ctx, js, "OFFSETS", "cdc.a.shop", 3)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := a.Load(); err != nil || got != (OffsetRecord{}) {
		t.Fatalf("empty Load = %+v, %v", got, err)
	}
	want := OffsetRecord{GTIDSet: fmt.Sprintf("%s:1-5", serverA), ResetToken: "t1"}
	if err := a.Save(want); err != nil {
		t.Fatal(err)
	}
	again, err := NewKVOffsetStore(ctx, js, "OFFSETS", "cdc.a.shop", 3)
	if err != nil {
		t.Fatalf("opening an existing bucket: %v", err)
	}
	other, _ := NewKVOffsetStore(ctx, js, "OFFSETS", "cdc.b.shop", 3)
	if got, _ := again.Load(); got != want {
		t.Errorf("same key Load = %+v, want %+v", got, want)
	}
	if got, _ := other.Load(); got != (OffsetRecord{}) {
		t.Errorf("another watcher's key sees %+v", got)
	}
}

// A purged key after a previous run is refused, not read as a first start.
func TestKVOffsetLostKeyIsRefused(t *testing.T) {
	env := natstest.Start(t)
	conn := env.Connect(t)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	store, err := NewKVOffsetStore(ctx, js, "OFFSETS", "cdc.proj.shop", 1)
	if err != nil {
		t.Fatal(err)
	}
	mustDo(t, store.Save(OffsetRecord{GTIDSet: serverA + ":1-5"}))
	kv, err := js.KeyValue(ctx, "OFFSETS")
	if err != nil {
		t.Fatal(err)
	}
	mustDo(t, kv.Purge(ctx, "cdc.proj.shop"))

	if _, err := store.Load(); !errors.Is(err, ErrOffsetLost) {
		t.Fatalf("Load = %v, want ErrOffsetLost", err)
	}
}

// The bucket is on file storage, so the offset outlives a NATS restart.
func TestKVOffsetSurvivesANATSRestart(t *testing.T) {
	env := natstest.Start(t)
	js, err := jetstream.New(env.Connect(t))
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewKVOffsetStore(context.Background(), js, "OFFSETS", "cdc.proj.shop", 1)
	if err != nil {
		t.Fatal(err)
	}
	want := OffsetRecord{GTIDSet: serverA + ":1-9"}
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}
	env.Stop()
	env.Start(t)

	js, err = jetstream.New(env.Connect(t))
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewKVOffsetStore(context.Background(), js, "OFFSETS", "cdc.proj.shop", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Load(); err != nil || got != want {
		t.Fatalf("after restart Load = %+v, %v; want %+v", got, err, want)
	}
}

// Streaming that breaks after Start reaches the watcher through Failed(), which
// makes it exit instead of idling. The break here is the server leaving GTID
// mode ON under the running stream: it refuses the re-sync, and an anonymous
// transaction follows. (The anonymous event itself is refused in the handler,
// see TestAnonymousGTIDStopsTheWatcher.)
func TestStreamBreakingAfterStartIsReportedAsAFailure(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()
	cfg := makeMySQLConfig(addr)
	cfg.OffsetFile = filepath.Join(t.TempDir(), "o.json")
	cfg.Tables = []string{"cdc_test_users"}

	svc := cdc.NewService()
	defer svc.Shutdown()
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()

	execMySQL(t, addr, "SET GLOBAL gtid_mode=ON_PERMISSIVE")
	db := openTestDB(t, addr, 5, time.Second)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, statement := range []string{
		"SET gtid_next='ANONYMOUS'",
		"INSERT INTO cdc_test_users (name) VALUES ('anonymous')",
		"SET gtid_next='AUTOMATIC'",
	} {
		if _, err := conn.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	select {
	case err := <-listener.Failed():
		if err == nil {
			t.Fatal("failure delivered without an error")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("a broken stream was never reported")
	}
}

// The reset token applies once: restarting with it still set keeps the offset
// the first start went on to save.
func TestResetTokenIsOneShotAcrossRestarts(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()
	cfg := makeMySQLConfig(addr)
	cfg.OffsetFile = filepath.Join(t.TempDir(), "o.json")
	cfg.ResetOffset = "t1"

	names, stop, err := streamUsers(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	insertRows(t, addr, "one", 1)
	expectNames(t, names, "one-1")
	stop()

	insertRows(t, addr, "while-down", 1)
	names, stop, err = streamUsers(t, cfg) // same token, still set in the environment
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	expectNames(t, names, "while-down-1")
}
