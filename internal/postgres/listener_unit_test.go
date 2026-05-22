package postgres

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
)

func TestNewListenerWithoutFilters(t *testing.T) {
	svc := cdc.NewService()
	l, err := NewListener(config.PostgresConfig{}, svc)
	if err != nil {
		t.Fatal(err)
	}
	if l.IsRunning() {
		t.Error("new listener should not be running")
	}
	if l.schemaStore != nil {
		t.Error("schema store should be nil without SchemaHistoryDir")
	}
}

func TestNewListenerWithTableFilter(t *testing.T) {
	svc := cdc.NewService()
	l, err := NewListener(config.PostgresConfig{
		Tables: []string{"users", "orders"},
	}, svc)
	if err != nil {
		t.Fatal(err)
	}
	if l.parser == nil {
		t.Fatal("parser should be initialized")
	}
}

func TestNewListenerWithSchemaHistory(t *testing.T) {
	svc := cdc.NewService()
	dir := t.TempDir()
	l, err := NewListener(config.PostgresConfig{
		SchemaHistoryDir: filepath.Join(dir, "history"),
	}, svc)
	if err != nil {
		t.Fatal(err)
	}
	if l.schemaStore == nil {
		t.Error("schema store should be initialized")
	}
}

func TestListenerStopWhenNotRunningIsNoop(t *testing.T) {
	svc := cdc.NewService()
	l, _ := NewListener(config.PostgresConfig{}, svc)
	l.Stop()
	if l.IsRunning() {
		t.Error("should remain not-running after Stop")
	}
}

func TestRunSnapshotNoneMode(t *testing.T) {
	svc := cdc.NewService()
	l, _ := NewListener(config.PostgresConfig{SnapshotMode: ""}, svc)
	if err := l.runSnapshot(context.Background()); err != nil {
		t.Errorf("none should be no-op: %v", err)
	}
}

func TestRunSnapshotBackupFileRequiresPath(t *testing.T) {
	svc := cdc.NewService()
	l, _ := NewListener(config.PostgresConfig{SnapshotMode: "backup_file"}, svc)
	if err := l.runSnapshot(context.Background()); err == nil {
		t.Fatal("expected error when backup file path is empty")
	}
}
