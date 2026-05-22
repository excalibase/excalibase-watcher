package mysql

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
)

func TestNewListenerWithoutOffsetFile(t *testing.T) {
	svc := cdc.NewService()
	l, err := NewListener(config.MySQLConfig{Host: "localhost"}, svc)
	if err != nil {
		t.Fatal(err)
	}
	if l.offsetStore != nil {
		t.Error("expected nil offset store when OffsetFile is empty")
	}
	if l.IsRunning() {
		t.Error("new listener should not be running")
	}
}

func TestNewListenerWithOffsetFile(t *testing.T) {
	svc := cdc.NewService()
	path := filepath.Join(t.TempDir(), "offset.json")
	l, err := NewListener(config.MySQLConfig{OffsetFile: path}, svc)
	if err != nil {
		t.Fatal(err)
	}
	if l.offsetStore == nil {
		t.Fatal("expected offset store to be initialized")
	}
}

func TestSetAckedLSNProvider(t *testing.T) {
	svc := cdc.NewService()
	l, _ := NewListener(config.MySQLConfig{}, svc)
	called := false
	l.SetAckedLSNProvider(func() string { called = true; return "foo" })
	if l.ackedLSNProvider == nil {
		t.Fatal("ackedLSNProvider not set")
	}
	_ = l.ackedLSNProvider()
	if !called {
		t.Error("provider not invoked")
	}
}

func TestGetStartPositionNoStore(t *testing.T) {
	svc := cdc.NewService()
	l, _ := NewListener(config.MySQLConfig{}, svc)
	pos, err := l.getStartPosition()
	if err != nil {
		t.Fatal(err)
	}
	if pos != nil {
		t.Errorf("expected nil pos, got %v", pos)
	}
}

func TestGetStartPositionWithSavedOffset(t *testing.T) {
	svc := cdc.NewService()
	path := filepath.Join(t.TempDir(), "offset.json")
	store := NewFileOffsetStore(path)
	if err := store.Save(BinlogPosition{File: "mysql-bin.0005", Position: 9999}); err != nil {
		t.Fatal(err)
	}
	l, _ := NewListener(config.MySQLConfig{OffsetFile: path}, svc)
	pos, err := l.getStartPosition()
	if err != nil {
		t.Fatal(err)
	}
	if pos == nil || pos.File != "mysql-bin.0005" || pos.Position != 9999 {
		t.Errorf("got %v", pos)
	}
}

func TestRunSnapshotNoneMode(t *testing.T) {
	svc := cdc.NewService()
	l, _ := NewListener(config.MySQLConfig{SnapshotMode: ""}, svc)
	if err := l.runSnapshot(); err != nil {
		t.Errorf("none mode should be no-op: %v", err)
	}
}

func TestRunSnapshotBackupFileRequiresPath(t *testing.T) {
	svc := cdc.NewService()
	l, _ := NewListener(config.MySQLConfig{SnapshotMode: "backup_file"}, svc)
	err := l.runSnapshot()
	if err == nil {
		t.Fatal("expected error when backup_file path is empty")
	}
	if !errors.Is(err, err) {
		t.Error("error should be returned")
	}
}

func TestEventHandlerCurrentBinlogFile(t *testing.T) {
	h := &eventHandler{}
	if got := h.currentBinlogFile(); got != "" {
		t.Errorf("empty handler should return empty, got %q", got)
	}
	h.currentFile = "mysql-bin.000001"
	if got := h.currentBinlogFile(); got != "mysql-bin.000001" {
		t.Errorf("got %q", got)
	}
}
