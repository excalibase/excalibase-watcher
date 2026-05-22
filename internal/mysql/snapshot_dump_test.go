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

	// Should extract binlog position from CHANGE MASTER TO
	if pos.File != "mysql-bin.000003" {
		t.Errorf("binlog file = %q, want mysql-bin.000003", pos.File)
	}
	if pos.Position != 154 {
		t.Errorf("binlog position = %d, want 154", pos.Position)
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
	if pos.File != "" {
		t.Errorf("expected empty binlog file, got %q", pos.File)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
}
