package schema

import (
	"path/filepath"
	"testing"
)

func TestGetHistoryOnEmptyStore(t *testing.T) {
	store, err := NewFileHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	hist, err := store.GetHistory("public", "missing")
	if err != nil {
		t.Fatalf("GetHistory err: %v", err)
	}
	if hist != nil {
		t.Errorf("expected nil history, got %v", hist)
	}
}

func TestLoadEntriesPersistsAcrossInstances(t *testing.T) {
	dir := t.TempDir()
	s1, _ := NewFileHistoryStore(dir)
	s1.Save(HistoryEntry{
		Position: "0/1", Schema: "public", Table: "users",
		Columns: []ColumnDef{{Name: "id", TypeOID: 23}},
	})

	// Fresh store pointing at the same dir — exercises the on-disk load path
	s2, _ := NewFileHistoryStore(dir)
	latest, err := s2.GetLatest("public", "users")
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil || latest.Position != "0/1" {
		t.Errorf("got %v", latest)
	}
}

func TestLoadEntriesBadJSONReturnsError(t *testing.T) {
	dir := t.TempDir()
	// Write invalid JSON into the expected file path
	path := filepath.Join(dir, "bad.schema.json")
	if err := writeFileRaw(path, []byte("not json{{{")); err != nil {
		t.Fatal(err)
	}
	s, _ := NewFileHistoryStore(dir)
	_, err := s.GetLatest("bad", "schema")
	if err == nil {
		t.Fatal("expected error for malformed JSON file")
	}
}

func writeFileRaw(path string, data []byte) error {
	return writeFileViaOS(path, data)
}
