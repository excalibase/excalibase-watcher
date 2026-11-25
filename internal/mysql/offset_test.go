package mysql

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileOffsetStoreRoundTripAndOverwrite(t *testing.T) {
	store := NewFileOffsetStore(filepath.Join(t.TempDir(), "offset.json"))
	for _, rec := range []OffsetRecord{{GTIDSet: serverA + ":1-5"}, {GTIDSet: serverA + ":1-9", ResetToken: "t1"}} {
		if err := store.Save(rec); err != nil {
			t.Fatal(err)
		}
		got, err := store.Load()
		if err != nil || got != rec {
			t.Fatalf("Load = %+v, %v; want %+v", got, err, rec)
		}
	}
}

func TestFileOffsetStoreLoadMissingIsEmpty(t *testing.T) {
	got, err := NewFileOffsetStore(filepath.Join(t.TempDir(), "none.json")).Load()
	if err != nil || got != (OffsetRecord{}) {
		t.Fatalf("Load = %+v, %v", got, err)
	}
}

// A file written before resume by GTID must not be mistaken for "nothing
// saved", which would silently restart from the server's current position.
func TestFileOffsetStoreMarksALegacyPositionOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	if err := os.WriteFile(path, []byte(`{"file":"mysql-bin.000003","position":154}`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := NewFileOffsetStore(path).Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.GTIDSet != "" || got.File == "" {
		t.Fatalf("legacy record = %+v", got)
	}
	if err := got.usable(); !errors.Is(err, ErrLegacyOffset) {
		t.Fatalf("usable() = %v, want ErrLegacyOffset", err)
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

func TestApplyResetTokenIsOneShot(t *testing.T) {
	saved := OffsetRecord{GTIDSet: serverA + ":1-9"}

	got, applied := applyResetToken(saved, "")
	if applied || got != saved {
		t.Fatalf("no token: %+v applied=%v", got, applied)
	}
	withToken := OffsetRecord{GTIDSet: serverA + ":1-9", ResetToken: "t0"}
	if got, applied := applyResetToken(withToken, ""); applied || got != withToken {
		t.Fatalf("no token must keep an offset that carries an old token: %+v applied=%v", got, applied)
	}
	got, applied = applyResetToken(saved, "t1")
	if !applied || got.GTIDSet != "" || got.ResetToken != "t1" {
		t.Fatalf("new token: %+v applied=%v", got, applied)
	}
	// The record the first start saved, seen by the second start.
	later := OffsetRecord{GTIDSet: serverA + ":1-20", ResetToken: "t1"}
	got, applied = applyResetToken(later, "t1")
	if applied || got != later {
		t.Fatalf("same token again must keep the offset: %+v applied=%v", got, applied)
	}
	got, applied = applyResetToken(later, "t2")
	if !applied || got.GTIDSet != "" || got.ResetToken != "t2" {
		t.Fatalf("a new token resets again: %+v applied=%v", got, applied)
	}
}
