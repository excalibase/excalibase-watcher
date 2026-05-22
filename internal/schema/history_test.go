package schema

import (
	"testing"
)

func TestSaveAndGetLatest(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	entry := HistoryEntry{
		Position: "0/1",
		Schema:   "public",
		Table:    "users",
		Columns:  []ColumnDef{{Name: "id", TypeOID: 23}, {Name: "name", TypeOID: 25}},
	}
	if err := store.Save(entry); err != nil {
		t.Fatal(err)
	}

	latest, err := store.GetLatest("public", "users")
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil {
		t.Fatal("GetLatest returned nil")
	}
	if latest.Position != "0/1" {
		t.Errorf("position = %q", latest.Position)
	}
	if len(latest.Columns) != 2 {
		t.Errorf("columns = %d", len(latest.Columns))
	}
}

func TestGetHistory(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	store.Save(HistoryEntry{Position: "0/1", Schema: "public", Table: "users",
		Columns: []ColumnDef{{Name: "id", TypeOID: 23}}})
	store.Save(HistoryEntry{Position: "0/2", Schema: "public", Table: "users",
		Columns: []ColumnDef{{Name: "id", TypeOID: 23}, {Name: "email", TypeOID: 25}}})

	history, err := store.GetHistory("public", "users")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Errorf("history length = %d, want 2", len(history))
	}
}

func TestGetLatestMissing(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	latest, err := store.GetLatest("public", "nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if latest != nil {
		t.Error("expected nil for missing table")
	}
}
