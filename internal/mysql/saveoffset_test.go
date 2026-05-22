package mysql

import (
	"path/filepath"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/go-mysql-org/go-mysql/replication"
)

func TestSaveAckedOffsetEmpty(t *testing.T) {
	svc := cdc.NewService()
	l, _ := NewListener(config.MySQLConfig{OffsetFile: filepath.Join(t.TempDir(), "o.json")}, svc)
	l.SetAckedLSNProvider(func() string { return "" })
	if err := l.saveAckedOffset(); err != nil {
		t.Errorf("empty LSN should be no-op, got %v", err)
	}
}

func TestSaveAckedOffsetMissingColon(t *testing.T) {
	svc := cdc.NewService()
	l, _ := NewListener(config.MySQLConfig{OffsetFile: filepath.Join(t.TempDir(), "o.json")}, svc)
	l.SetAckedLSNProvider(func() string { return "bad-lsn" })
	err := l.saveAckedOffset()
	if err == nil {
		t.Fatal("expected error for LSN without colon")
	}
}

func TestSaveAckedOffsetInvalidPos(t *testing.T) {
	svc := cdc.NewService()
	l, _ := NewListener(config.MySQLConfig{OffsetFile: filepath.Join(t.TempDir(), "o.json")}, svc)
	l.SetAckedLSNProvider(func() string { return "mysql-bin.000001:not-a-number" })
	err := l.saveAckedOffset()
	if err == nil {
		t.Fatal("expected error for non-numeric position")
	}
}

func TestSaveAckedOffsetPersists(t *testing.T) {
	svc := cdc.NewService()
	path := filepath.Join(t.TempDir(), "o.json")
	l, _ := NewListener(config.MySQLConfig{OffsetFile: path}, svc)
	l.SetAckedLSNProvider(func() string { return "mysql-bin.000042:1234" })

	if err := l.saveAckedOffset(); err != nil {
		t.Fatalf("save: %v", err)
	}

	store := NewFileOffsetStore(path)
	pos, err := store.Load()
	if err != nil || pos == nil {
		t.Fatalf("load: err=%v pos=%v", err, pos)
	}
	if pos.File != "mysql-bin.000042" || pos.Position != 1234 {
		t.Errorf("got %+v", pos)
	}
}

func TestOnRotateUpdatesCurrentFile(t *testing.T) {
	h := &eventHandler{}
	ev := &replication.RotateEvent{NextLogName: []byte("mysql-bin.000099")}
	if err := h.OnRotate(nil, ev); err != nil {
		t.Fatal(err)
	}
	if got := h.currentBinlogFile(); got != "mysql-bin.000099" {
		t.Errorf("got %q", got)
	}
}
