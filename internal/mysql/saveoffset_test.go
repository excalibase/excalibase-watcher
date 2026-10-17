package mysql

import (
	"path/filepath"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

func newGatedListener(t *testing.T) (*Listener, *eventHandler, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "o.json")
	l, err := NewListener(config.MySQLConfig{OffsetFile: path}, cdc.NewService())
	if err != nil {
		t.Fatal(err)
	}
	l.PublishedObserver()
	return l, &eventHandler{service: l.service, gate: l.gate, currentFile: "mysql-bin.000007"}, path
}

func loadOffset(t *testing.T, path string) *BinlogPosition {
	t.Helper()
	pos, err := NewFileOffsetStore(path).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return pos
}

func TestSaveConfirmedOffsetWithNothingConfirmedWritesNothing(t *testing.T) {
	l, _, path := newGatedListener(t)
	if err := l.saveConfirmedOffset(); err != nil {
		t.Fatal(err)
	}
	if pos := loadOffset(t, path); pos != nil {
		t.Errorf("offset written with nothing confirmed: %v", pos)
	}
}

// A crash after two of three rows of one rows event were acked must resume
// before that event, not after it.
func TestOffsetNeverPassesAPartlyAckedRowsEvent(t *testing.T) {
	l, h, path := newGatedListener(t)
	observe := l.gate.ObservePublished
	ch, unsub := l.service.SubscribeAll()
	defer unsub()

	if err := h.OnXID(nil, gomysql.Position{Name: "mysql-bin.000007", Pos: 400}); err != nil {
		t.Fatal(err)
	}
	rows := &canal.RowsEvent{
		Table:  &schema.Table{Schema: "shop", Name: "orders", Columns: []schema.TableColumn{{Name: "id"}}},
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{1}, {2}, {3}},
		Header: &replication.EventHeader{LogPos: 700},
	}
	if err := h.OnRow(rows); err != nil {
		t.Fatal(err)
	}
	if err := h.OnXID(nil, gomysql.Position{Name: "mysql-bin.000007", Pos: 731}); err != nil {
		t.Fatal(err)
	}
	<-ch // the first COMMIT
	observe(<-ch)
	observe(<-ch)

	if err := l.saveConfirmedOffset(); err != nil {
		t.Fatal(err)
	}
	if pos := loadOffset(t, path); pos == nil || pos.Position != 400 {
		t.Fatalf("offset after 2 of 3 rows acked = %v, want mysql-bin.000007:400", pos)
	}

	observe(<-ch)
	if err := l.saveConfirmedOffset(); err != nil {
		t.Fatal(err)
	}
	if pos := loadOffset(t, path); pos == nil || pos.Position != 731 {
		t.Fatalf("offset after every row acked = %v, want mysql-bin.000007:731", pos)
	}
}

func TestOnRotateUpdatesCurrentFileAndIsABoundary(t *testing.T) {
	h := &eventHandler{gate: newCommitGate()}
	ev := &replication.RotateEvent{NextLogName: []byte("mysql-bin.000099"), Position: 4}
	if err := h.OnRotate(nil, ev); err != nil {
		t.Fatal(err)
	}
	if got := h.currentBinlogFile(); got != "mysql-bin.000099" {
		t.Errorf("got %q", got)
	}
	if pos, _ := h.gate.Confirmable(); pos != (BinlogPosition{File: "mysql-bin.000099", Position: 4}) {
		t.Errorf("rotate boundary = %v", pos)
	}
}
