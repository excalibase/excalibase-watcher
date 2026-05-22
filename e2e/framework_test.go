//go:build e2e

package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	_ "github.com/go-mysql-org/go-mysql/driver"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	pgConnStr  = "postgres://e2euser:e2epass@localhost:15432/e2edb?sslmode=disable"
	mysqlAddr  = "localhost:13306"
	mysqlDSN   = "root:e2epass@localhost:13306/e2edb"
	natsURL    = "nats://localhost:14222"
	healthPort = 18080
)

// watcherBin holds the path to the pre-built binary (set in TestMain)
var watcherBin string

func TestMain(m *testing.M) {
	// Build binary once
	binPath := filepath.Join(os.TempDir(), "watcher-e2e")
	cmd := exec.Command("go", "build", "-o", binPath, ".")
	cmd.Dir = projectRoot()
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build watcher: %v\n%s\n", err, out)
		os.Exit(1)
	}
	watcherBin = binPath

	// Clean up stale Postgres replication slots from prior runs
	ctx := context.Background()
	if conn, err := pgx.Connect(ctx, pgConnStr); err == nil {
		conn.Exec(ctx, `SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE active_pid IS NOT NULL`)
		time.Sleep(500 * time.Millisecond)
		conn.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots")
		conn.Close(ctx)
	}

	// Clean up stale NATS streams
	if nc, err := nats.Connect(natsURL); err == nil {
		if js, err := jetstream.New(nc); err == nil {
			// List and delete all streams
			lister := js.ListStreams(ctx)
			for info := range lister.Info() {
				js.DeleteStream(ctx, info.Config.Name)
			}
		}
		nc.Close()
	}

	code := m.Run()

	// Cleanup after all tests
	if conn, err := pgx.Connect(ctx, pgConnStr); err == nil {
		conn.Exec(ctx, `SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE active_pid IS NOT NULL`)
		time.Sleep(500 * time.Millisecond)
		conn.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots")
		conn.Close(ctx)
	}

	os.Remove(binPath)
	os.Exit(code)
}

// watcherProcess manages the real watcher binary subprocess
type watcherProcess struct {
	cmd    *exec.Cmd
	cfgFile string
	cancel context.CancelFunc
}

func projectRoot() string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return dir
}

func startWatcher(t *testing.T, cfgYAML string) *watcherProcess {
	t.Helper()

	// Watcher no longer creates NATS streams — it's a pure publisher and
	// expects the stream to exist. Pre-provision for the test, mirroring
	// what infra does in production (helm hook / nats-box / terraform).
	if stream, prefix := extractStreamConfig(cfgYAML); stream != "" {
		ensureNATSStream(t, stream, prefix)
	}
	return startWatcherRaw(t, cfgYAML, true)
}

// startWatcherWithoutStreamProvision skips pre-creating the NATS stream.
// Used by tests that verify fail-fast behavior when a stream is missing.
// The returned process may exit immediately; callers must not assume health is up.
func startWatcherWithoutStreamProvision(t *testing.T, cfgYAML string) *watcherProcess {
	t.Helper()
	return startWatcherRaw(t, cfgYAML, false)
}

// startWatcherRaw starts the binary; waitForHealthReady toggles whether we
// wait for /healthz to respond 200 (normal tests) or return immediately
// (fail-fast tests).
func startWatcherRaw(t *testing.T, cfgYAML string, waitForHealthReady bool) *watcherProcess {
	t.Helper()

	cfgFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfgFile, []byte(cfgYAML), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, watcherBin, "--config", cfgFile)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start watcher: %v", err)
	}

	wp := &watcherProcess{cmd: cmd, cfgFile: cfgFile, cancel: cancel}

	if waitForHealthReady {
		healthURL := fmt.Sprintf("http://localhost:%d/healthz", healthPort)
		waitForHealth(t, healthURL, 15*time.Second)
	}

	return wp
}

func (wp *watcherProcess) stop(t *testing.T) {
	t.Helper()
	wp.cancel()
	wp.cmd.Wait()
	time.Sleep(500 * time.Millisecond) // let connections fully close
}

// kill sends SIGKILL — simulates a crash (no graceful shutdown, no Stop()).
// Use this to verify at-least-once semantics: restart must re-deliver any
// events that were in-flight at the time of the kill.
func (wp *watcherProcess) kill(t *testing.T) {
	t.Helper()
	if wp.cmd.Process != nil {
		_ = wp.cmd.Process.Kill()
	}
	wp.cmd.Wait()
	wp.cancel()
	time.Sleep(500 * time.Millisecond)
}

func waitForHealth(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("watcher health endpoint not ready after %v", timeout)
}

// --- Postgres helpers ---

func pgExec(t *testing.T, query string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgConnStr)
	if err != nil {
		t.Fatalf("pg connect: %v", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, query)
	if err != nil {
		t.Fatalf("pg exec %q: %v", query, err)
	}
}

func pgSetup(t *testing.T) {
	t.Helper()
	pgExec(t, "DROP TABLE IF EXISTS e2e_users CASCADE")
	pgExec(t, `CREATE TABLE e2e_users (
		id SERIAL PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT,
		active BOOLEAN DEFAULT true,
		score NUMERIC(10,2),
		age INT
	)`)
	pgExec(t, "ALTER TABLE e2e_users REPLICA IDENTITY FULL")
}

func pgCleanupSlot(t *testing.T, slotName string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgConnStr)
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	conn.Exec(ctx, `
		SELECT pg_terminate_backend(active_pid)
		FROM pg_replication_slots
		WHERE slot_name = $1 AND active_pid IS NOT NULL`, slotName)
	time.Sleep(500 * time.Millisecond)
	conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slotName)
}

// --- MySQL helpers ---

func mysqlExec(t *testing.T, query string) {
	t.Helper()
	dsn := mysqlDSN
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
		t.Fatalf("mysql connect: %v", err)
	}
	defer db.Close()
	_, err = db.Exec(query)
	if err != nil {
		t.Fatalf("mysql exec %q: %v", query, err)
	}
}

func mysqlSetup(t *testing.T) {
	t.Helper()
	mysqlExec(t, "DROP TABLE IF EXISTS e2e_users")
	mysqlExec(t, `CREATE TABLE e2e_users (
		id INT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		email VARCHAR(200),
		active BOOLEAN DEFAULT true,
		score DECIMAL(10,2),
		age INT
	)`)
}

// --- NATS helpers ---

type natsConsumer struct {
	conn     *nats.Conn
	js       jetstream.JetStream
	consumer jetstream.Consumer
}

func newNATSConsumer(t *testing.T, stream, subject string) *natsConsumer {
	t.Helper()
	conn, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		t.Fatalf("jetstream: %v", err)
	}

	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, err = js.Stream(ctx, stream)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		conn.Close()
		t.Fatalf("stream %q not found: %v", stream, err)
	}

	consumer, err := js.CreateConsumer(ctx, stream, jetstream.ConsumerConfig{
		FilterSubject: subject,
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})
	if err != nil {
		conn.Close()
		t.Fatalf("create consumer: %v", err)
	}

	return &natsConsumer{conn: conn, js: js, consumer: consumer}
}

func newNATSConsumerAll(t *testing.T, stream, subject string) *natsConsumer {
	t.Helper()
	conn, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		t.Fatalf("jetstream: %v", err)
	}

	ctx := context.Background()
	consumer, err := js.CreateConsumer(ctx, stream, jetstream.ConsumerConfig{
		FilterSubject: subject,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		conn.Close()
		t.Fatalf("create consumer: %v", err)
	}
	return &natsConsumer{conn: conn, js: js, consumer: consumer}
}

func (nc *natsConsumer) close() {
	nc.conn.Close()
}

func (nc *natsConsumer) fetchEvents(t *testing.T, count int, timeout time.Duration) []cdc.Event {
	t.Helper()
	var events []cdc.Event

	deadline := time.Now().Add(timeout)
	for len(events) < count && time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining < time.Second {
			remaining = time.Second
		}
		msgs, err := nc.consumer.Fetch(count-len(events), jetstream.FetchMaxWait(remaining))
		if err != nil {
			continue
		}
		for msg := range msgs.Messages() {
			var event cdc.Event
			if err := json.Unmarshal(msg.Data(), &event); err != nil {
				t.Logf("unmarshal error: %v, data: %s", err, string(msg.Data()))
				msg.Ack()
				continue
			}
			events = append(events, event)
			msg.Ack()
		}
	}
	return events
}

func (nc *natsConsumer) fetchUntilType(t *testing.T, typ cdc.EventType, timeout time.Duration) cdc.Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining < time.Second {
			remaining = time.Second
		}
		msgs, err := nc.consumer.Fetch(1, jetstream.FetchMaxWait(remaining))
		if err != nil {
			continue
		}
		for msg := range msgs.Messages() {
			var event cdc.Event
			if err := json.Unmarshal(msg.Data(), &event); err != nil {
				t.Logf("fetchUntilType: unmarshal error: %v, raw: %s", err, msg.Data())
			}
			msg.Ack()
			if event.Type == typ {
				return event
			}
		}
	}
	t.Fatalf("timeout waiting for event type %v", typ)
	return cdc.Event{}
}

// --- HTTP helpers ---

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("http get %q: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// --- Config templates ---

func pgOnlyConfig(slotName string) string {
	streamName := "S_" + strings.ToUpper(slotName)
	prefix := "p_" + slotName
	return fmt.Sprintf(`
postgres:
  enabled: true
  url: "%s"
  slot_name: "%s"
  publication_name: "e2e_pub_%s"
  create_slot: true
  create_publication: true
  snapshot_mode: none
  snapshot_chunk_size: 10000
  capture_ddl: false

mysql:
  enabled: false

nats:
  enabled: true
  url: "%s"
  stream_name: "%s"
  subject_prefix: "%s"
  max_age_minutes: 5
  storage: memory

health:
  port: %d

metrics:
  enabled: true
`, pgConnStr, slotName, slotName, natsURL, streamName, prefix, healthPort)
}

func pgWithDDLConfig(slotName string) string {
	streamName := "S_" + strings.ToUpper(slotName)
	prefix := "p_" + slotName
	return fmt.Sprintf(`
postgres:
  enabled: true
  url: "%s"
  slot_name: "%s"
  publication_name: "e2e_ddl_pub_%s"
  create_slot: true
  create_publication: true
  snapshot_mode: none
  capture_ddl: true

mysql:
  enabled: false

nats:
  enabled: true
  url: "%s"
  stream_name: "%s"
  subject_prefix: "%s"
  max_age_minutes: 5
  storage: memory

health:
  port: %d

metrics:
  enabled: true
`, pgConnStr, slotName, slotName, natsURL, streamName, prefix, healthPort)
}

func pgWithSnapshotConfig(slotName string, chunkSize int) string {
	streamName := "S_" + strings.ToUpper(slotName)
	prefix := "p_" + slotName
	return fmt.Sprintf(`
postgres:
  enabled: true
  url: "%s"
  slot_name: "%s"
  publication_name: "e2e_snap_pub_%s"
  create_slot: true
  create_publication: true
  snapshot_mode: chunked
  snapshot_chunk_size: %d

mysql:
  enabled: false

nats:
  enabled: true
  url: "%s"
  stream_name: "%s"
  subject_prefix: "%s"
  max_age_minutes: 5
  storage: memory

health:
  port: %d

metrics:
  enabled: true
`, pgConnStr, slotName, slotName, chunkSize, natsURL, streamName, prefix, healthPort)
}

func pgWithTableFilterConfig(slotName string, tables []string) string {
	streamName := "S_" + strings.ToUpper(slotName)
	prefix := "p_" + slotName
	tableYAML := ""
	for _, t := range tables {
		tableYAML += fmt.Sprintf("\n    - %s", t)
	}
	return fmt.Sprintf(`
postgres:
  enabled: true
  url: "%s"
  slot_name: "%s"
  publication_name: "e2e_filter_pub_%s"
  create_slot: true
  create_publication: true
  snapshot_mode: none
  tables: %s

mysql:
  enabled: false

nats:
  enabled: true
  url: "%s"
  stream_name: "%s"
  subject_prefix: "%s"
  max_age_minutes: 5
  storage: memory

health:
  port: %d

metrics:
  enabled: true
`, pgConnStr, slotName, slotName, tableYAML, natsURL, streamName, prefix, healthPort)
}

func mysqlOnlyConfig(streamName string) string {
	prefix := "m_" + strings.ToLower(streamName)
	return fmt.Sprintf(`
postgres:
  enabled: false

mysql:
  enabled: true
  host: "%s"
  username: "root"
  password: "e2epass"
  schema: "e2edb"
  tables:
    - e2e_users

nats:
  enabled: true
  url: "%s"
  stream_name: "%s"
  subject_prefix: "%s"
  max_age_minutes: 5
  storage: memory

health:
  port: %d

metrics:
  enabled: true
`, mysqlAddr, natsURL, streamName, prefix, healthPort)
}

// mysqlWithOffsetConfig returns a MySQL-only config that persists binlog offset to the given path.
func mysqlWithOffsetConfig(streamName, offsetFile string) string {
	prefix := "m_" + strings.ToLower(streamName)
	return fmt.Sprintf(`
postgres:
  enabled: false

mysql:
  enabled: true
  host: "%s"
  username: "root"
  password: "e2epass"
  schema: "e2edb"
  tables:
    - e2e_users
  offset_file: "%s"

nats:
  enabled: true
  url: "%s"
  stream_name: "%s"
  subject_prefix: "%s"
  max_age_minutes: 5
  storage: memory

health:
  port: %d

metrics:
  enabled: true
`, mysqlAddr, offsetFile, natsURL, streamName, prefix, healthPort)
}

// bothDBsConfig returns a config that watches both Postgres and MySQL simultaneously.
func bothDBsConfig(pgSlot, streamName string) string {
	return fmt.Sprintf(`
postgres:
  enabled: true
  url: "%s"
  slot_name: "%s"
  publication_name: "e2e_both_pub_%s"
  create_slot: true
  create_publication: true
  snapshot_mode: none

mysql:
  enabled: true
  host: "%s"
  username: "root"
  password: "e2epass"
  schema: "e2edb"
  tables:
    - e2e_users

nats:
  enabled: true
  url: "%s"
  stream_name: "%s"
  subject_prefix: "both"
  max_age_minutes: 5
  storage: memory

health:
  port: %d

metrics:
  enabled: true
`, pgConnStr, pgSlot, pgSlot, mysqlAddr, natsURL, streamName, healthPort)
}

// streamNameRegex and subjectPrefixRegex extract NATS config from YAML.
// Simple regex parsing avoids pulling a YAML dependency into tests.
var (
	streamNameRegex    = regexp.MustCompile(`(?m)^\s*stream_name:\s*"?([^"\n]+?)"?\s*$`)
	subjectPrefixRegex = regexp.MustCompile(`(?m)^\s*subject_prefix:\s*"?([^"\n]+?)"?\s*$`)
)

// extractStreamConfig pulls stream_name and subject_prefix from a YAML config string.
// Returns empty strings if NATS is disabled or not configured.
func extractStreamConfig(cfgYAML string) (stream, prefix string) {
	if m := streamNameRegex.FindStringSubmatch(cfgYAML); len(m) == 2 {
		stream = m[1]
	}
	if m := subjectPrefixRegex.FindStringSubmatch(cfgYAML); len(m) == 2 {
		prefix = m[1]
	}
	return
}

// ensureNATSStream creates or resets a stream so consumers can be created before the watcher starts
// ensureNATSStream creates the stream if missing. If it already exists, leaves
// it alone — tests that set up a shared stream with a wildcard must not have
// that overwritten by a per-tenant prefix.
func ensureNATSStream(t *testing.T, stream, subjectPrefix string) {
	t.Helper()
	conn, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()
	js, _ := jetstream.New(conn)
	ctx := context.Background()
	if _, err := js.Stream(ctx, stream); err == nil {
		return // exists, don't touch
	}
	_, _ = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     stream,
		Subjects: []string{subjectPrefix + ".>"},
		Storage:  jetstream.MemoryStorage,
	})
}

// deleteNATSStream cleans up a NATS stream
func deleteNATSStream(stream string) {
	conn, err := nats.Connect(natsURL)
	if err != nil {
		return
	}
	defer conn.Close()
	js, err := jetstream.New(conn)
	if err != nil {
		return
	}
	js.DeleteStream(context.Background(), stream)
}

// containsString checks if any event's Data field contains the given string
func containsString(events []cdc.Event, s string) bool {
	for _, e := range events {
		if strings.Contains(e.Data, s) {
			return true
		}
	}
	return false
}
