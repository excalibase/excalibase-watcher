package mysql

import (
	"path/filepath"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

func newGatedListener(t *testing.T, executed string) (*Listener, *eventHandler, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "o.json")
	l, err := NewListener(config.MySQLConfig{OffsetFile: path}, cdc.NewService())
	if err != nil {
		t.Fatal(err)
	}
	l.PublishedObserver()
	return l, &eventHandler{service: l.service, gate: l.gate, executed: gtidSet(t, executed)}, path
}

func loadOffset(t *testing.T, path string) string {
	t.Helper()
	set, err := NewFileOffsetStore(path).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return set
}

func TestSaveConfirmedOffsetWithNothingConfirmedWritesNothing(t *testing.T) {
	l, _, path := newGatedListener(t, "")
	if err := l.saveConfirmedOffset(); err != nil {
		t.Fatal(err)
	}
	if set := loadOffset(t, path); set != "" {
		t.Errorf("offset written with nothing confirmed: %q", set)
	}
}

// A crash after two of three rows of one transaction were acked must resume
// before that transaction, never after it.
func TestOffsetNeverPassesAPartlyAckedTransaction(t *testing.T) {
	l, h, path := newGatedListener(t, serverA+":1-3")
	observe := l.gate.ObservePublished
	ch, unsub := l.service.SubscribeAll()
	defer unsub()

	mustDo(t, h.OnGTID(nil, txnGTID(4)))
	mustDo(t, h.OnXID(nil, gomysql.Position{})) // an empty transaction first
	mustDo(t, h.OnGTID(nil, txnGTID(5)))
	mustDo(t, h.OnRow(rowsEvent(700, 1, 2, 3)))
	mustDo(t, h.OnXID(nil, gomysql.Position{}))
	<-ch // the first COMMIT
	observe(<-ch)
	observe(<-ch)

	mustDo(t, l.saveConfirmedOffset())
	if got := loadOffset(t, path); got != serverA+":1-4" {
		t.Fatalf("offset after 2 of 3 rows acked = %q, want %s:1-4", got, serverA)
	}

	observe(<-ch)
	mustDo(t, l.saveConfirmedOffset())
	if got := loadOffset(t, path); got != serverA+":1-5" {
		t.Fatalf("offset after every row acked = %q, want %s:1-5", got, serverA)
	}
}
