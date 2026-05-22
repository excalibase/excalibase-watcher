package postgres

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
)

func TestParseDumpFile(t *testing.T) {
	dump := `--
-- PostgreSQL database dump
--

COPY public.users (id, name, email) FROM stdin;
1	Alice	alice@example.com
2	Bob	\N
3	Charlie	charlie@test.com
\.

COPY public.orders (id, user_id, total) FROM stdin;
10	1	99.99
20	2	49.50
\.

`
	dir := t.TempDir()
	dumpFile := filepath.Join(dir, "dump.sql")
	if err := os.WriteFile(dumpFile, []byte(dump), 0644); err != nil {
		t.Fatal(err)
	}

	var events []cdc.Event
	handler := func(e cdc.Event) {
		events = append(events, e)
	}

	err := ParseDumpFile(dumpFile, "0/ABCD", handler)
	if err != nil {
		t.Fatalf("ParseDumpFile error: %v", err)
	}

	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}

	// First event: users row 1
	e := events[0]
	if e.Type != cdc.Insert {
		t.Errorf("event[0] type = %v, want INSERT", e.Type)
	}
	if e.Schema != "public" {
		t.Errorf("event[0] schema = %q", e.Schema)
	}
	if e.Table != "users" {
		t.Errorf("event[0] table = %q", e.Table)
	}
	if e.LSN != "0/ABCD" {
		t.Errorf("event[0] lsn = %q", e.LSN)
	}
	expected := `{"id":"1", "name":"Alice", "email":"alice@example.com"}`
	if e.Data != expected {
		t.Errorf("event[0] data = %q\nwant  %q", e.Data, expected)
	}

	// Second event: users row 2 with NULL email
	e = events[1]
	expected = `{"id":"2", "name":"Bob", "email":null}`
	if e.Data != expected {
		t.Errorf("event[1] data = %q\nwant  %q", e.Data, expected)
	}

	// Fourth event: orders row 1
	e = events[3]
	if e.Table != "orders" {
		t.Errorf("event[3] table = %q", e.Table)
	}
	expected = `{"id":"10", "user_id":"1", "total":"99.99"}`
	if e.Data != expected {
		t.Errorf("event[3] data = %q\nwant  %q", e.Data, expected)
	}
}

func TestParseDumpFileEmpty(t *testing.T) {
	dir := t.TempDir()
	dumpFile := filepath.Join(dir, "empty.sql")
	if err := os.WriteFile(dumpFile, []byte("-- empty dump\n"), 0644); err != nil {
		t.Fatal(err)
	}

	var events []cdc.Event
	err := ParseDumpFile(dumpFile, "0/1", func(e cdc.Event) {
		events = append(events, e)
	})
	if err != nil {
		t.Fatalf("ParseDumpFile error: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}
