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

func TestResetOffsetClearsTheSavedOffsetOnlyWhenAsked(t *testing.T) {
	for _, reset := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "offset.json")
		l, err := NewListener(config.MySQLConfig{OffsetFile: path, ResetOffset: reset}, cdc.NewService())
		if err != nil {
			t.Fatal(err)
		}
		if err := l.offsetStore.Save(serverA + ":1-5"); err != nil {
			t.Fatal(err)
		}
		if err := l.resetOffsetIfAsked(); err != nil {
			t.Fatal(err)
		}
		got, _ := l.offsetStore.Load()
		if reset != (got == "") {
			t.Errorf("reset_offset=%v left offset %q", reset, got)
		}
	}
}

func TestSetOffsetStoreReplacesTheStore(t *testing.T) {
	l, _ := NewListener(config.MySQLConfig{}, cdc.NewService())
	store := NewFileOffsetStore(filepath.Join(t.TempDir(), "o.json"))
	l.SetOffsetStore(store)
	if l.offsetStore != store {
		t.Fatal("store not set")
	}
}

// A stream that ends with an error while the listener should be running must
// reach the watcher, which exits on it instead of idling.
func TestStreamingErrorIsReportedAsAFailure(t *testing.T) {
	l, _ := NewListener(config.MySQLConfig{}, cdc.NewService())
	l.running.Store(true)
	cause := errors.New("binlog stream broke")
	l.reportFailure(cause)
	select {
	case got := <-l.Failed():
		if !errors.Is(got, cause) {
			t.Fatalf("failure = %v", got)
		}
	default:
		t.Fatal("a streaming error was swallowed")
	}
}

func TestStopAndCleanEndsAreNotFailures(t *testing.T) {
	l, _ := NewListener(config.MySQLConfig{}, cdc.NewService())
	l.running.Store(true)
	l.reportFailure(nil)
	l.running.Store(false)
	l.reportFailure(errors.New("closed by Stop"))
	select {
	case got := <-l.Failed():
		t.Fatalf("unexpected failure %v", got)
	default:
	}
}
