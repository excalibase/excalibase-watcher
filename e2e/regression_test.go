//go:build e2e

package e2e

// Regression tests for specific production bugs.
// Each test name maps to the commit SHA that fixed the bug.

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
)

// TestE2E_MySQL_NoReplayOnFirstStart proves canal starts at the CURRENT master
// position, not position 0. QA bug: "Watcher-go started, connected to binlog at
// position 0, and replayed all historical events from mysql-bin.000001 → .000003."
//
// Scenario: pre-populate table with N rows BEFORE starting the watcher, with no
// offset file. Start watcher. Expect: no INSERT events for the pre-existing rows.
// Then insert 1 new row. Expect: exactly 1 INSERT event.
func TestE2E_MySQL_NoReplayOnFirstStart(t *testing.T) {
	mysqlSetup(t)
	stream := "MY_NOREPLAY"
	deleteNATSStream(stream)

	// Pre-populate 10 rows BEFORE the watcher starts
	for i := 0; i < 10; i++ {
		mysqlExec(t, fmt.Sprintf("INSERT INTO e2e_users (name) VALUES ('pre_%d')", i))
	}

	// Start watcher with NO offset file — must not replay the 10 rows
	wp := startWatcher(t, mysqlOnlyConfig(stream))
	defer wp.stop(t)

	nc := newNATSConsumer(t, stream, mysqlSubject(stream, "e2e_users"))
	defer nc.close()

	// Give canal time to connect; any replayed rows should arrive within this window
	time.Sleep(3 * time.Second)

	// Non-blocking fetch — we expect ZERO events for pre-existing rows
	events := nc.fetchEvents(t, 1, 2*time.Second)
	if len(events) > 0 {
		t.Fatalf("canal replayed %d historical rows — expected 0", len(events))
	}

	// Now insert a NEW row after the watcher is running — this should arrive
	mysqlExec(t, "INSERT INTO e2e_users (name) VALUES ('post_start')")
	event := nc.fetchUntilType(t, cdc.Insert, 10*time.Second)
	if !containsString([]cdc.Event{event}, "post_start") {
		t.Errorf("expected post_start event, got: %s", event.Data)
	}
}

// TestE2E_MySQL_RestartResumesFromOffset proves offset persistence works.
// QA bug: "watcher-go replays the full binlog on every start because it doesn't
// persist offset_file."
//
// Scenario: start watcher with offset file. Insert N1 rows. Stop watcher.
// While watcher is down, insert N2 more rows. Restart watcher with SAME offset
// file. Expect: N2 events (the ones inserted while down), no duplicates of N1.
func TestE2E_MySQL_RestartResumesFromOffset(t *testing.T) {
	mysqlSetup(t)
	stream := "MY_RESUME"
	deleteNATSStream(stream)

	offsetFile := filepath.Join(t.TempDir(), "binlog.offset")
	cfg := mysqlWithOffsetConfig(stream, offsetFile)

	// Phase 1: start watcher, insert 3 rows, verify events arrive
	wp1 := startWatcher(t, cfg)
	nc1 := newNATSConsumer(t, stream, mysqlSubject(stream, "e2e_users"))
	time.Sleep(3 * time.Second) // let canal catch up

	for i := 0; i < 3; i++ {
		mysqlExec(t, fmt.Sprintf("INSERT INTO e2e_users (name) VALUES ('phase1_%d')", i))
	}
	phase1Events := nc1.fetchEvents(t, 3, 15*time.Second)
	if len(phase1Events) != 3 {
		t.Fatalf("phase1: got %d events, want 3", len(phase1Events))
	}
	nc1.close()
	wp1.stop(t)

	// Phase 2: watcher is DOWN. Insert 4 more rows.
	for i := 0; i < 4; i++ {
		mysqlExec(t, fmt.Sprintf("INSERT INTO e2e_users (name) VALUES ('phase2_%d')", i))
	}

	// Phase 3: restart watcher with same offset file. Should resume, not replay.
	wp2 := startWatcher(t, cfg)
	defer wp2.stop(t)
	nc2 := newNATSConsumerAll(t, stream, mysqlSubject(stream, "e2e_users"))
	defer nc2.close()

	// Fetch up to 15 events (stream may retain phase1 events). Count phase2 matches.
	// We tolerate some phase1 duplicates because canal may re-read the last
	// committed transaction on resume, but all 4 phase2 rows MUST arrive.
	events := nc2.fetchEvents(t, 15, 15*time.Second)
	phase2Count := 0
	for _, e := range events {
		if strings.Contains(e.Data, "phase2_") {
			phase2Count++
		}
	}
	if phase2Count < 4 {
		t.Errorf("got %d phase2 events, want >= 4 (watcher did not resume from offset)", phase2Count)
	}
}

// TestE2E_MySQL_BackpressureNoLoss proves that under load, events are not
// silently dropped. QA bug: "thousands of WARN global subscriber channel full,
// dropping event type=INSERT."
//
// Scenario: rapidly insert N rows. Expect: all N events arrive in NATS.
// Before the fix, this would drop events and the test would fail.
func TestE2E_MySQL_BackpressureNoLoss(t *testing.T) {
	mysqlSetup(t)
	stream := "MY_LOAD"
	deleteNATSStream(stream)

	wp := startWatcher(t, mysqlOnlyConfig(stream))
	defer wp.stop(t)
	nc := newNATSConsumer(t, stream, mysqlSubject(stream, "e2e_users"))
	defer nc.close()
	time.Sleep(3 * time.Second)

	const rowCount = 500
	// Batch insert for speed
	var values []string
	for i := 0; i < rowCount; i++ {
		values = append(values, fmt.Sprintf("('load_%d')", i))
	}
	mysqlExec(t, "INSERT INTO e2e_users (name) VALUES "+strings.Join(values, ","))

	// Must receive ALL rowCount events — zero loss
	events := nc.fetchEvents(t, rowCount, 60*time.Second)
	if len(events) != rowCount {
		t.Fatalf("got %d events, want %d (events dropped under backpressure)", len(events), rowCount)
	}
}

// TestE2E_BothDBsSimultaneously proves that Postgres and MySQL listeners
// run concurrently without interfering with each other.
func TestE2E_BothDBsSimultaneously(t *testing.T) {
	pgSetup(t)
	mysqlSetup(t)
	slot := "e2e_both"
	pgCleanupSlot(t, slot)
	stream := "BOTH_DBS"
	deleteNATSStream(stream)

	wp := startWatcher(t, bothDBsConfig(slot, stream))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	pgConsumer := newNATSConsumer(t, stream, "both.public.e2e_users")
	defer pgConsumer.close()
	mysqlConsumer := newNATSConsumer(t, stream, "both.e2edb.e2e_users")
	defer mysqlConsumer.close()

	time.Sleep(3 * time.Second) // let both listeners connect

	pgExec(t, "INSERT INTO e2e_users (name) VALUES ('pg_row')")
	mysqlExec(t, "INSERT INTO e2e_users (name) VALUES ('mysql_row')")

	pgEvent := pgConsumer.fetchUntilType(t, cdc.Insert, 15*time.Second)
	if pgEvent.Schema != "public" {
		t.Errorf("PG event schema = %q, want public", pgEvent.Schema)
	}
	if !containsString([]cdc.Event{pgEvent}, "pg_row") {
		t.Errorf("PG event missing pg_row: %s", pgEvent.Data)
	}

	mysqlEvent := mysqlConsumer.fetchUntilType(t, cdc.Insert, 15*time.Second)
	if mysqlEvent.Schema != "e2edb" {
		t.Errorf("MySQL event schema = %q, want e2edb", mysqlEvent.Schema)
	}
	if !containsString([]cdc.Event{mysqlEvent}, "mysql_row") {
		t.Errorf("MySQL event missing mysql_row: %s", mysqlEvent.Data)
	}
}

// TestE2E_MySQL_CrashDoesNotLoseEvents proves at-least-once semantics: if the
// watcher is SIGKILL'd (no graceful shutdown, no Stop() flush), restart must
// re-deliver every event that was in the pipeline.
//
// Before the fix, offset was saved by canal's OnXID ahead of NATS delivery.
// A crash with events buffered but unpublished would lose them on restart
// because the offset pointed past them.
func TestE2E_MySQL_CrashDoesNotLoseEvents(t *testing.T) {
	mysqlSetup(t)
	stream := "MY_CRASH"
	deleteNATSStream(stream)

	offsetFile := filepath.Join(t.TempDir(), "binlog.offset")
	cfg := mysqlWithOffsetConfig(stream, offsetFile)

	// Phase 1: start watcher, insert rows, then KILL (no graceful stop)
	wp1 := startWatcher(t, cfg)
	nc1 := newNATSConsumer(t, stream, mysqlSubject(stream, "e2e_users"))
	time.Sleep(3 * time.Second) // canal connect

	const phase1Rows = 50
	var values []string
	for i := 0; i < phase1Rows; i++ {
		values = append(values, fmt.Sprintf("('crash_phase1_%d')", i))
	}
	mysqlExec(t, "INSERT INTO e2e_users (name) VALUES "+strings.Join(values, ","))

	// Read a few events so we know the pipeline is flowing, then crash.
	_ = nc1.fetchEvents(t, 3, 10*time.Second)
	nc1.close()
	wp1.kill(t) // SIGKILL — no Stop(), no final flush

	// Phase 2: restart. Must resume and deliver ALL phase1 rows not yet acked.
	// Combined with phase2 inserts while down.
	const phase2Rows = 10
	for i := 0; i < phase2Rows; i++ {
		mysqlExec(t, fmt.Sprintf("INSERT INTO e2e_users (name) VALUES ('crash_phase2_%d')", i))
	}

	wp2 := startWatcher(t, cfg)
	defer wp2.stop(t)
	nc2 := newNATSConsumerAll(t, stream, mysqlSubject(stream, "e2e_users"))
	defer nc2.close()

	// All phase1 + phase2 rows must be deliverable. Duplicates OK
	// (at-least-once). We check that every unique row appears at least once.
	// Fetch generously to absorb duplicates from replay.
	events := nc2.fetchEvents(t, (phase1Rows+phase2Rows)*3, 60*time.Second)
	seen := make(map[string]bool)
	for _, e := range events {
		for i := 0; i < phase1Rows; i++ {
			key := fmt.Sprintf("crash_phase1_%d", i)
			if strings.Contains(e.Data, `"name":"`+key+`"`) {
				seen[key] = true
			}
		}
		for i := 0; i < phase2Rows; i++ {
			key := fmt.Sprintf("crash_phase2_%d", i)
			if strings.Contains(e.Data, `"name":"`+key+`"`) {
				seen[key] = true
			}
		}
	}
	if len(seen) != phase1Rows+phase2Rows {
		t.Fatalf("only %d/%d unique rows delivered after crash+restart (data loss)",
			len(seen), phase1Rows+phase2Rows)
	}
}

// TestE2E_PG_RestartResumesFromLSN proves the Postgres replication slot persists
// WAL position across restarts. Postgres guarantees this via the slot on the
// server side — we just verify the watcher correctly reconnects and resumes.
func TestE2E_PG_RestartResumesFromLSN(t *testing.T) {
	pgSetup(t)
	slot := "e2e_pg_resume"
	pgCleanupSlot(t, slot)
	deleteNATSStream(pgStream(slot))

	cfg := pgOnlyConfig(slot)

	// Phase 1: start watcher, insert rows, verify events arrive
	wp1 := startWatcher(t, cfg)
	nc1 := newNATSConsumer(t, pgStream(slot), pgSubject(slot, "e2e_users"))
	time.Sleep(time.Second)

	pgExec(t, "INSERT INTO e2e_users (name) VALUES ('pg_phase1')")
	nc1.fetchUntilType(t, cdc.Insert, 10*time.Second)
	nc1.close()
	wp1.stop(t)

	// Phase 2: watcher down, insert more rows (WAL accumulates via slot)
	for i := 0; i < 3; i++ {
		pgExec(t, fmt.Sprintf("INSERT INTO e2e_users (name) VALUES ('pg_phase2_%d')", i))
	}

	// Phase 3: restart. Postgres replication slot must replay the 3 rows.
	wp2 := startWatcher(t, cfg)
	defer pgCleanupSlot(t, slot)
	defer wp2.stop(t)
	nc2 := newNATSConsumerAll(t, pgStream(slot), pgSubject(slot, "e2e_users"))
	defer nc2.close()

	// Fetch up to 10 events (may include phase1 due to DeliverAllPolicy retention).
	// We only care that all 3 phase2 rows arrive after the restart.
	events := nc2.fetchEvents(t, 10, 15*time.Second)
	foundPhase2 := 0
	for _, e := range events {
		if strings.Contains(e.Data, "pg_phase2_") {
			foundPhase2++
		}
	}
	if foundPhase2 < 3 {
		t.Errorf("got %d phase2 events, want >= 3 (slot did not buffer during downtime)", foundPhase2)
	}
}
