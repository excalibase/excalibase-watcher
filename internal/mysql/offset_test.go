package mysql

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileOffsetStoreRoundTripAndOverwrite(t *testing.T) {
	store := NewFileOffsetStore(filepath.Join(t.TempDir(), "offset.json"))
	for _, set := range []string{serverA + ":1-5", serverA + ":1-9"} {
		if err := store.Save(set); err != nil {
			t.Fatal(err)
		}
		got, err := store.Load()
		if err != nil || got != set {
			t.Fatalf("Load = %q, %v; want %q", got, err, set)
		}
	}
}

func TestFileOffsetStoreLoadMissingIsEmpty(t *testing.T) {
	got, err := NewFileOffsetStore(filepath.Join(t.TempDir(), "none.json")).Load()
	if err != nil || got != "" {
		t.Fatalf("Load = %q, %v", got, err)
	}
}

func TestFileOffsetStoreClear(t *testing.T) {
	store := NewFileOffsetStore(filepath.Join(t.TempDir(), "offset.json"))
	if err := store.Clear(); err != nil {
		t.Fatalf("clearing nothing: %v", err)
	}
	if err := store.Save(serverA + ":1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Clear(); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Load(); got != "" {
		t.Fatalf("Load after Clear = %q", got)
	}
}

// A file written before resume by GTID must not be mistaken for "nothing
// saved", which would silently restart from the server's current position.
func TestFileOffsetStoreRefusesALegacyPositionOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	if err := os.WriteFile(path, []byte(`{"file":"mysql-bin.000003","position":154}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileOffsetStore(path).Load(); !errors.Is(err, ErrLegacyOffset) {
		t.Fatalf("err = %v, want ErrLegacyOffset", err)
	}
}

func TestFileOffsetStoreRefusesACorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	if err := os.WriteFile(path, []byte(`{not json`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileOffsetStore(path).Load(); err == nil {
		t.Fatal("a corrupt offset file must be an error, not an empty offset")
	}
}

func TestOffsetKeyIsStableAndAvoidsTheServerAddress(t *testing.T) {
	if got := OffsetKey("excalibase.cdc", "shop"); got != "excalibase.cdc.shop" {
		t.Errorf("key = %q", got)
	}
	if got := OffsetKey("a b", "sh*op"); got != "a_b.sh_op" {
		t.Errorf("key = %q", got)
	}
}
