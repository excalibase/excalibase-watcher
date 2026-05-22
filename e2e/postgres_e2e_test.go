//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
)

func pgStream(slotName string) string  { return "S_" + strings.ToUpper(slotName) }
func pgSubject(slotName, table string) string { return "p_" + slotName + ".public." + table }
func pgSubjectWild(slotName string) string    { return "p_" + slotName + ".>" }

func TestE2E_PG_Insert(t *testing.T) {
pgSetup(t)
	slot := "e2e_pg_ins"
	deleteNATSStream(pgStream(slot))

	wp := startWatcher(t,pgOnlyConfig(slot))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	nc := newNATSConsumer(t, pgStream(slot), pgSubject(slot, "e2e_users"))
	defer nc.close()
	time.Sleep(time.Second)

	pgExec(t, "INSERT INTO e2e_users (name, email, score) VALUES ('Alice', 'alice@test.com', 99.50)")

	event := nc.fetchUntilType(t, cdc.Insert, 15*time.Second)
	if event.Table != "e2e_users" {
		t.Errorf("table = %q", event.Table)
	}
	if event.Schema != "public" {
		t.Errorf("schema = %q", event.Schema)
	}
	if !containsString([]cdc.Event{event}, "Alice") {
		t.Errorf("data missing Alice: %s", event.Data)
	}
}

func TestE2E_PG_Update(t *testing.T) {
pgSetup(t)
	slot := "e2e_pg_upd"
	deleteNATSStream(pgStream(slot))
	pgExec(t, "INSERT INTO e2e_users (name, email) VALUES ('Alice', 'alice@test.com')")

	wp := startWatcher(t,pgOnlyConfig(slot))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	nc := newNATSConsumer(t, pgStream(slot), pgSubject(slot, "e2e_users"))
	defer nc.close()
	time.Sleep(time.Second)

	pgExec(t, "UPDATE e2e_users SET name = 'Bob' WHERE name = 'Alice'")

	event := nc.fetchUntilType(t, cdc.Update, 15*time.Second)
	if !containsString([]cdc.Event{event}, "old") {
		t.Errorf("UPDATE missing 'old': %s", event.Data)
	}
	if !containsString([]cdc.Event{event}, "new") {
		t.Errorf("UPDATE missing 'new': %s", event.Data)
	}
}

func TestE2E_PG_Delete(t *testing.T) {
pgSetup(t)
	slot := "e2e_pg_del"
	deleteNATSStream(pgStream(slot))
	pgExec(t, "INSERT INTO e2e_users (name) VALUES ('ToDelete')")

	wp := startWatcher(t,pgOnlyConfig(slot))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	nc := newNATSConsumer(t, pgStream(slot), pgSubject(slot, "e2e_users"))
	defer nc.close()
	time.Sleep(time.Second)

	pgExec(t, "DELETE FROM e2e_users WHERE name = 'ToDelete'")

	event := nc.fetchUntilType(t, cdc.Delete, 15*time.Second)
	if event.Table != "e2e_users" {
		t.Errorf("table = %q", event.Table)
	}
}

func TestE2E_PG_Truncate(t *testing.T) {
pgSetup(t)
	slot := "e2e_pg_trunc"
	deleteNATSStream(pgStream(slot))
	pgExec(t, "INSERT INTO e2e_users (name) VALUES ('WillBeTruncated')")

	wp := startWatcher(t,pgOnlyConfig(slot))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	nc := newNATSConsumer(t, pgStream(slot), pgSubject(slot, "e2e_users"))
	defer nc.close()
	time.Sleep(time.Second)

	pgExec(t, "TRUNCATE e2e_users")

	event := nc.fetchUntilType(t, cdc.Truncate, 15*time.Second)
	if event.Table != "e2e_users" {
		t.Errorf("table = %q", event.Table)
	}
}

func TestE2E_PG_DDL(t *testing.T) {
pgSetup(t)
	slot := "e2e_pg_ddl"
	deleteNATSStream(pgStream(slot))
	pgExec(t, "DROP EVENT TRIGGER IF EXISTS cdc_ddl_capture")
	pgExec(t, "DROP EVENT TRIGGER IF EXISTS cdc_ddl_drop_capture")
	pgExec(t, "DROP TABLE IF EXISTS _cdc_ddl_log")

	wp := startWatcher(t,pgWithDDLConfig(slot))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	nc := newNATSConsumer(t, pgStream(slot), pgSubjectWild(slot))
	defer nc.close()
	time.Sleep(2 * time.Second)

	pgExec(t, "ALTER TABLE e2e_users ADD COLUMN bio TEXT")

	event := nc.fetchUntilType(t, cdc.DDL, 15*time.Second)
	if event.Type != cdc.DDL {
		t.Errorf("type = %v", event.Type)
	}
}

func TestE2E_PG_ChunkedSnapshot(t *testing.T) {
pgSetup(t)
	slot := "e2e_pg_snap"
	deleteNATSStream(pgStream(slot))

	for i := 0; i < 5; i++ {
		pgExec(t, fmt.Sprintf("INSERT INTO e2e_users (name) VALUES ('snap_%d')", i))
	}

	wp := startWatcher(t,pgWithSnapshotConfig(slot, 2))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	// Give snapshot time to complete and publish to NATS
	time.Sleep(3 * time.Second)

	// Use DeliverAll to catch snapshot events that were published during startup
	nc := newNATSConsumerAll(t, pgStream(slot), pgSubject(slot, "e2e_users"))
	defer nc.close()

	// Should get 5 snapshot INSERT events
	events := nc.fetchEvents(t, 5, 15*time.Second)
	if len(events) < 5 {
		t.Fatalf("expected 5 snapshot events, got %d", len(events))
	}

	// Insert a live row after snapshot
	pgExec(t, "INSERT INTO e2e_users (name) VALUES ('live_after_snap')")
	live := nc.fetchEvents(t, 1, 15*time.Second)
	if len(live) < 1 {
		t.Error("expected 1 live CDC event after snapshot")
	}
}

func TestE2E_PG_TableFiltering(t *testing.T) {
pgSetup(t)
	pgExec(t, "DROP TABLE IF EXISTS other_table")
	pgExec(t, "CREATE TABLE other_table (id SERIAL PRIMARY KEY, value TEXT)")
	slot := "e2e_pg_filt"
	deleteNATSStream(pgStream(slot))

	wp := startWatcher(t,pgWithTableFilterConfig(slot, []string{"e2e_users"}))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	nc := newNATSConsumer(t, pgStream(slot), pgSubjectWild(slot))
	defer nc.close()
	time.Sleep(time.Second)

	pgExec(t, "INSERT INTO other_table (value) VALUES ('filtered_out')")
	pgExec(t, "INSERT INTO e2e_users (name) VALUES ('should_appear')")

	event := nc.fetchUntilType(t, cdc.Insert, 15*time.Second)
	if event.Table != "e2e_users" {
		t.Errorf("got event for wrong table: %q", event.Table)
	}
}

func TestE2E_PG_TypeMapping(t *testing.T) {
pgSetup(t)
	slot := "e2e_pg_type"
	deleteNATSStream(pgStream(slot))

	wp := startWatcher(t,pgOnlyConfig(slot))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	nc := newNATSConsumer(t, pgStream(slot), pgSubject(slot, "e2e_users"))
	defer nc.close()
	time.Sleep(time.Second)

	pgExec(t, "INSERT INTO e2e_users (name, active, score, age) VALUES ('TypeTest', true, 123.45, 30)")

	event := nc.fetchUntilType(t, cdc.Insert, 15*time.Second)
	if !containsString([]cdc.Event{event}, ":30") {
		t.Errorf("INT should be unquoted: %s", event.Data)
	}
	if !containsString([]cdc.Event{event}, ":true") {
		t.Errorf("BOOL should be true: %s", event.Data)
	}
	if !containsString([]cdc.Event{event}, `"TypeTest"`) {
		t.Errorf("TEXT should be quoted: %s", event.Data)
	}
}
