package mysql

import (
	"path/filepath"
	"testing"
)

func TestFileOffsetStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store := NewFileOffsetStore(filepath.Join(dir, "offset.json"))

	pos := BinlogPosition{File: "mysql-bin.000003", Position: 12345}
	if err := store.Save(pos); err != nil {
		t.Fatalf("Save error: %v", err)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if loaded == nil {
		t.Fatal("Load returned nil")
	}
	if loaded.File != "mysql-bin.000003" {
		t.Errorf("file = %q", loaded.File)
	}
	if loaded.Position != 12345 {
		t.Errorf("position = %d", loaded.Position)
	}
}

func TestFileOffsetStoreOverwrite(t *testing.T) {
	dir := t.TempDir()
	store := NewFileOffsetStore(filepath.Join(dir, "offset.json"))

	store.Save(BinlogPosition{File: "mysql-bin.000001", Position: 100})
	store.Save(BinlogPosition{File: "mysql-bin.000002", Position: 200})

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if loaded.File != "mysql-bin.000002" {
		t.Errorf("file = %q, want mysql-bin.000002", loaded.File)
	}
	if loaded.Position != 200 {
		t.Errorf("position = %d, want 200", loaded.Position)
	}
}

func TestFileOffsetStoreLoadMissing(t *testing.T) {
	dir := t.TempDir()
	store := NewFileOffsetStore(filepath.Join(dir, "nonexistent.json"))

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if loaded != nil {
		t.Errorf("expected nil for missing file, got %v", loaded)
	}
}
