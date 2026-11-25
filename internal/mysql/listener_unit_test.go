package mysql

import (
	"errors"
	"os"
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

func newFileListener(t *testing.T, path, token string) *Listener {
	t.Helper()
	l, err := NewListener(config.MySQLConfig{OffsetFile: path, ResetOffset: token}, cdc.NewService())
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// The token discards the saved offset once; a restart with the token still set
// must keep the offset the first start went on to save.
func TestResetTokenDiscardsTheOffsetOnlyOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	if err := NewFileOffsetStore(path).Save(OffsetRecord{GTIDSet: serverA + ":1-5"}); err != nil {
		t.Fatal(err)
	}

	first := newFileListener(t, path, "t1")
	mustDo(t, first.loadOffset())
	if first.record.GTIDSet != "" {
		t.Fatalf("the token did not discard the offset: %+v", first.record)
	}
	mustDo(t, first.saveOffset(serverA+":1-9"))

	second := newFileListener(t, path, "t1")
	mustDo(t, second.loadOffset())
	if second.record.GTIDSet != serverA+":1-9" {
		t.Fatalf("the same token discarded the offset again: %+v", second.record)
	}

	third := newFileListener(t, path, "t2")
	mustDo(t, third.loadOffset())
	if third.record.GTIDSet != "" {
		t.Fatalf("a new token must discard again: %+v", third.record)
	}
}

func TestNoTokenKeepsTheOffsetAndALegacyOffsetIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	mustDo(t, NewFileOffsetStore(path).Save(OffsetRecord{GTIDSet: serverA + ":1-5"}))
	l := newFileListener(t, path, "")
	mustDo(t, l.loadOffset())
	if l.record.GTIDSet != serverA+":1-5" {
		t.Fatalf("record = %+v", l.record)
	}

	legacy := filepath.Join(t.TempDir(), "legacy.json")
	if err := os.WriteFile(legacy, []byte(`{"file":"mysql-bin.000003","position":4}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := newFileListener(t, legacy, "").loadOffset(); !errors.Is(err, ErrLegacyOffset) {
		t.Fatalf("err = %v, want ErrLegacyOffset", err)
	}
	// The token is the way out of it.
	l = newFileListener(t, legacy, "t1")
	mustDo(t, l.loadOffset())
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

type scriptedStore struct {
	loadErr error
	saved   []OffsetRecord
}

func (s *scriptedStore) Load() (OffsetRecord, error) { return OffsetRecord{}, s.loadErr }
func (s *scriptedStore) Save(r OffsetRecord) error   { s.saved = append(s.saved, r); return nil }

// A lost offset stops the watcher; the reset token is the explicit way past it.
func TestLostOffsetNeedsTheResetToken(t *testing.T) {
	store := &scriptedStore{loadErr: ErrOffsetLost}
	l, _ := NewListener(config.MySQLConfig{}, cdc.NewService())
	l.SetOffsetStore(store)
	if err := l.loadOffset(); !errors.Is(err, ErrOffsetLost) {
		t.Fatalf("err = %v, want ErrOffsetLost", err)
	}

	l, _ = NewListener(config.MySQLConfig{ResetOffset: "t1"}, cdc.NewService())
	l.SetOffsetStore(store)
	mustDo(t, l.loadOffset())
	if len(store.saved) != 1 || store.saved[0].ResetToken != "t1" {
		t.Fatalf("the token was not saved: %+v", store.saved)
	}
}
