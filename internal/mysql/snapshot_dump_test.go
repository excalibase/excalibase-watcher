package mysql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
)

func TestParseMysqlDumpFile(t *testing.T) {
	dump := `-- MySQL dump 10.13  Distrib 8.0.36
--
-- CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000003', MASTER_LOG_POS=154;
SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,
4f22fb58-82db-22f2-af44-d91bb0530673:1-2';

INSERT INTO ` + "`users`" + ` VALUES (1,'Alice','alice@test.com'),(2,'Bob',NULL);

INSERT INTO ` + "`orders`" + ` VALUES (10,1,99.99),(20,2,49.50);
`
	dir := t.TempDir()
	dumpFile := filepath.Join(dir, "dump.sql")
	if err := os.WriteFile(dumpFile, []byte(dump), 0644); err != nil {
		t.Fatal(err)
	}

	var events []cdc.Event
	pos, err := ParseMysqlDumpFile(dumpFile, "mydb", func(e cdc.Event) {
		events = append(events, e)
	})
	if err != nil {
		t.Fatalf("ParseMysqlDumpFile error: %v", err)
	}

	// The dump's GTID set, joined across the lines mysqldump breaks it over.
	want := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,4f22fb58-82db-22f2-af44-d91bb0530673:1-2"
	if pos != want {
		t.Errorf("gtid set = %q, want %q", pos, want)
	}

	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}

	// First event: users row 1
	e := events[0]
	if e.Type != cdc.Insert {
		t.Errorf("event[0] type = %v, want INSERT", e.Type)
	}
	if e.Table != "users" {
		t.Errorf("event[0] table = %q", e.Table)
	}
	if e.Schema != "mydb" {
		t.Errorf("event[0] schema = %q", e.Schema)
	}

	// Second event: users row 2 with NULL
	e = events[1]
	if e.Table != "users" {
		t.Errorf("event[1] table = %q", e.Table)
	}

	// Third event: orders row 1
	e = events[2]
	if e.Table != "orders" {
		t.Errorf("event[2] table = %q", e.Table)
	}
}

func TestParseMysqlDumpEscaping(t *testing.T) {
	dump := `INSERT INTO ` + "`docs`" + ` VALUES (1,'it''s a \"test\"','line1\nline2');
`
	dir := t.TempDir()
	dumpFile := filepath.Join(dir, "dump.sql")
	if err := os.WriteFile(dumpFile, []byte(dump), 0644); err != nil {
		t.Fatal(err)
	}

	var events []cdc.Event
	_, err := ParseMysqlDumpFile(dumpFile, "mydb", func(e cdc.Event) {
		events = append(events, e)
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
}

func TestParseMysqlDumpNoBinlogPos(t *testing.T) {
	dump := `INSERT INTO ` + "`users`" + ` VALUES (1,'Alice','test@test.com');
`
	dir := t.TempDir()
	dumpFile := filepath.Join(dir, "dump.sql")
	if err := os.WriteFile(dumpFile, []byte(dump), 0644); err != nil {
		t.Fatal(err)
	}

	var events []cdc.Event
	pos, err := ParseMysqlDumpFile(dumpFile, "mydb", func(e cdc.Event) {
		events = append(events, e)
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if pos != "" {
		t.Errorf("expected no GTID set, got %q", pos)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
}

func TestParseMysqlDumpSingleLineGTIDSet(t *testing.T) {
	dump := "SET @@GLOBAL.GTID_PURGED='3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9';\n"
	dumpFile := filepath.Join(t.TempDir(), "dump.sql")
	if err := os.WriteFile(dumpFile, []byte(dump), 0644); err != nil {
		t.Fatal(err)
	}
	set, err := ParseMysqlDumpFile(dumpFile, "mydb", func(cdc.Event) {})
	if err != nil {
		t.Fatal(err)
	}
	if set != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9" {
		t.Errorf("gtid set = %q", set)
	}
}

// Only mysqldump's own statement names the dump's GTID set; the same words in
// a comment or inside a row must not become the offset.
func TestParseMysqlDumpIgnoresGTIDPurgedOutsideItsStatement(t *testing.T) {
	dump := "-- note: GTID_PURGED='3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9' was applied\n" +
		"-- SET @@GLOBAL.GTID_PURGED='4f22fb58-82db-22f2-af44-d91bb0530673:1-2';\n"
	dumpFile := filepath.Join(t.TempDir(), "dump.sql")
	if err := os.WriteFile(dumpFile, []byte(dump), 0644); err != nil {
		t.Fatal(err)
	}
	set, err := ParseMysqlDumpFile(dumpFile, "mydb", func(cdc.Event) {})
	if err != nil {
		t.Fatal(err)
	}
	if set != "" {
		t.Errorf("gtid set = %q, want none", set)
	}
}
