//go:build e2e

package e2e

// Edge case E2E tests: JSON/JSONB columns, large transactions, binary data,
// schema evolution mid-stream. These exercise type mapping and buffering
// boundaries that CRUD tests miss.

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
)

// TestE2E_PG_JSONBColumn verifies that Postgres JSONB columns are emitted as
// raw JSON (nested object), not a quoted string. Consumers must be able to
// parse event.data.payload.foo directly without double-decoding.
func TestE2E_PG_JSONBColumn(t *testing.T) {
	slot := "e2e_pg_jsonb"
	pgCleanupSlot(t, slot)
	deleteNATSStream(pgStream(slot))

	pgExec(t, "DROP TABLE IF EXISTS e2e_jsonb CASCADE")
	pgExec(t, `CREATE TABLE e2e_jsonb (
		id SERIAL PRIMARY KEY,
		payload JSONB,
		config JSON
	)`)
	pgExec(t, "ALTER TABLE e2e_jsonb REPLICA IDENTITY FULL")

	cfg := strings.Replace(pgOnlyConfig(slot), "e2e_users", "e2e_jsonb", -1)
	wp := startWatcher(t, cfg)
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	nc := newNATSConsumer(t, pgStream(slot), "p_"+slot+".public.e2e_jsonb")
	defer nc.close()
	time.Sleep(time.Second)

	pgExec(t, `INSERT INTO e2e_jsonb (payload, config) VALUES
		('{"foo": "bar", "nested": {"x": 1}}',
		 '[1, 2, 3]')`)

	event := nc.fetchUntilType(t, cdc.Insert, 15*time.Second)

	// Parse the outer event.data as JSON — must succeed
	var row map[string]interface{}
	if err := json.Unmarshal([]byte(event.Data), &row); err != nil {
		t.Fatalf("event.data is not valid JSON: %v\ndata: %s", err, event.Data)
	}

	// payload must be a nested object (not a string)
	payload, ok := row["payload"].(map[string]interface{})
	if !ok {
		t.Fatalf("payload should be a JSON object, got %T: %v", row["payload"], row["payload"])
	}
	if payload["foo"] != "bar" {
		t.Errorf("payload.foo = %v, want bar", payload["foo"])
	}

	// config must be a nested array (not a string)
	config, ok := row["config"].([]interface{})
	if !ok {
		t.Fatalf("config should be a JSON array, got %T: %v", row["config"], row["config"])
	}
	if len(config) != 3 {
		t.Errorf("config length = %d, want 3", len(config))
	}
}

// TestE2E_MySQL_JSONColumn verifies MySQL JSON columns are emitted as raw JSON.
func TestE2E_MySQL_JSONColumn(t *testing.T) {
	stream := "MY_JSON"
	deleteNATSStream(stream)

	mysqlExec(t, "DROP TABLE IF EXISTS e2e_json")
	mysqlExec(t, `CREATE TABLE e2e_json (
		id INT AUTO_INCREMENT PRIMARY KEY,
		payload JSON
	)`)

	cfg := strings.Replace(mysqlOnlyConfig(stream), "e2e_users", "e2e_json", -1)
	wp := startWatcher(t, cfg)
	defer wp.stop(t)

	nc := newNATSConsumer(t, stream, "m_"+strings.ToLower(stream)+".e2edb.e2e_json")
	defer nc.close()
	time.Sleep(3 * time.Second) // canal connect time

	mysqlExec(t, `INSERT INTO e2e_json (payload) VALUES ('{"user": "alice", "tags": ["a","b"]}')`)

	event := nc.fetchUntilType(t, cdc.Insert, 15*time.Second)

	var row map[string]interface{}
	if err := json.Unmarshal([]byte(event.Data), &row); err != nil {
		t.Fatalf("event.data not valid JSON: %v\ndata: %s", err, event.Data)
	}
	payload, ok := row["payload"].(map[string]interface{})
	if !ok {
		t.Fatalf("payload should be nested object, got %T: %v", row["payload"], row["payload"])
	}
	if payload["user"] != "alice" {
		t.Errorf("payload.user = %v, want alice", payload["user"])
	}
}

// TestE2E_PG_LargeTransaction inserts many rows in a single transaction to
// verify the event bus doesn't deadlock or drop events on large XIDs.
func TestE2E_PG_LargeTransaction(t *testing.T) {
	pgSetup(t)
	slot := "e2e_pg_bigtx"
	pgCleanupSlot(t, slot)
	deleteNATSStream(pgStream(slot))

	wp := startWatcher(t, pgOnlyConfig(slot))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	nc := newNATSConsumer(t, pgStream(slot), pgSubject(slot, "e2e_users"))
	defer nc.close()
	time.Sleep(time.Second)

	// 1000 rows in a single transaction
	const rowCount = 1000
	var sb strings.Builder
	sb.WriteString("INSERT INTO e2e_users (name) VALUES ")
	for i := 0; i < rowCount; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "('bigtx_%d')", i)
	}
	pgExec(t, sb.String())

	events := nc.fetchEvents(t, rowCount, 60*time.Second)
	if len(events) != rowCount {
		t.Fatalf("got %d events, want %d (large transaction lost events)", len(events), rowCount)
	}
}

// TestE2E_MySQL_LargeTransaction: same as PG but for MySQL.
func TestE2E_MySQL_LargeTransaction(t *testing.T) {
	mysqlSetup(t)
	stream := "MY_BIGTX"
	deleteNATSStream(stream)

	wp := startWatcher(t, mysqlOnlyConfig(stream))
	defer wp.stop(t)

	nc := newNATSConsumer(t, stream, mysqlSubject(stream, "e2e_users"))
	defer nc.close()
	time.Sleep(3 * time.Second)

	const rowCount = 1000
	var sb strings.Builder
	sb.WriteString("INSERT INTO e2e_users (name) VALUES ")
	for i := 0; i < rowCount; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "('bigtx_%d')", i)
	}
	mysqlExec(t, sb.String())

	events := nc.fetchEvents(t, rowCount, 60*time.Second)
	if len(events) != rowCount {
		t.Fatalf("got %d events, want %d", len(events), rowCount)
	}
}

// TestE2E_PG_BinaryDataWithControlChars verifies that rows containing control
// characters (newline, tab, null bytes in TEXT) don't break JSON output.
func TestE2E_PG_BinaryDataWithControlChars(t *testing.T) {
	pgSetup(t)
	slot := "e2e_pg_binary"
	pgCleanupSlot(t, slot)
	deleteNATSStream(pgStream(slot))

	wp := startWatcher(t, pgOnlyConfig(slot))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	nc := newNATSConsumer(t, pgStream(slot), pgSubject(slot, "e2e_users"))
	defer nc.close()
	time.Sleep(time.Second)

	// Insert a row with newline, tab, and a backslash-quote sequence
	pgExec(t, `INSERT INTO e2e_users (name) VALUES (E'line1\nline2\ttab\\back\"quote\x01control')`)

	event := nc.fetchUntilType(t, cdc.Insert, 15*time.Second)

	// The event.data must be valid JSON
	var row map[string]interface{}
	if err := json.Unmarshal([]byte(event.Data), &row); err != nil {
		t.Fatalf("event.data not valid JSON after control chars: %v\ndata: %s", err, event.Data)
	}
	name, ok := row["name"].(string)
	if !ok {
		t.Fatalf("name is not a string: %T", row["name"])
	}
	if !strings.Contains(name, "line1") || !strings.Contains(name, "line2") {
		t.Errorf("lost data in name: %q", name)
	}
}

// TestE2E_MySQL_SchemaEvolutionMidStream alters a table while the watcher is
// streaming. Subsequent inserts must use the new schema, not stale column list.
func TestE2E_MySQL_SchemaEvolutionMidStream(t *testing.T) {
	mysqlSetup(t)
	stream := "MY_EVOLVE"
	deleteNATSStream(stream)

	wp := startWatcher(t, mysqlOnlyConfig(stream))
	defer wp.stop(t)

	nc := newNATSConsumer(t, stream, mysqlSubject(stream, "e2e_users"))
	defer nc.close()
	time.Sleep(3 * time.Second)

	// Insert before alter
	mysqlExec(t, "INSERT INTO e2e_users (name) VALUES ('before_alter')")
	before := nc.fetchUntilType(t, cdc.Insert, 15*time.Second)
	if !strings.Contains(before.Data, "before_alter") {
		t.Fatalf("before_alter missing: %s", before.Data)
	}

	// Add a new column
	mysqlExec(t, "ALTER TABLE e2e_users ADD COLUMN nickname VARCHAR(50)")
	time.Sleep(2 * time.Second) // canal needs to pick up DDL

	// Insert with the new column
	mysqlExec(t, "INSERT INTO e2e_users (name, nickname) VALUES ('after_alter', 'ally')")
	after := nc.fetchUntilType(t, cdc.Insert, 15*time.Second)

	var row map[string]interface{}
	if err := json.Unmarshal([]byte(after.Data), &row); err != nil {
		t.Fatalf("after_alter event not valid JSON: %v\ndata: %s", err, after.Data)
	}
	if row["name"] != "after_alter" {
		t.Errorf("name = %v, want after_alter", row["name"])
	}
	if _, hasNickname := row["nickname"]; !hasNickname {
		t.Errorf("post-alter event missing 'nickname' column: %s", after.Data)
	}
}
