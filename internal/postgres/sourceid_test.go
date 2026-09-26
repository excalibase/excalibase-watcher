package postgres

import (
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
)

func buildBeginMsgAt(finalLSN uint64) []byte {
	buf := []byte{'B'}
	buf = appendUint64(buf, finalLSN)
	buf = appendUint64(buf, 1)
	return appendUint32(buf, 42)
}

// streamTransaction feeds one transaction through the listener the way the
// replication loop does and returns the source ids of what reached the bus.
func streamTransaction(t *testing.T, l *Listener, ch <-chan cdc.Event, finalLSN uint64, rows int) []string {
	t.Helper()
	l.emit(*l.parser.Parse(buildBeginMsgAt(finalLSN), "0/1"))
	for i := 0; i < rows; i++ {
		l.emit(*l.parser.Parse(buildInsertMsg(1, []testTupleVal{{marker: 't', value: "1"}}), "0/2"))
	}
	l.emit(*l.parser.Parse(buildCommitMsg(1), "0/3"))

	var ids []string
	for len(ids) < rows {
		if event := <-ch; event.Type == cdc.Insert {
			ids = append(ids, event.SourceID)
		}
	}
	return ids
}

func newSourceIDListener(t *testing.T) (*Listener, <-chan cdc.Event) {
	t.Helper()
	svc := cdc.NewService()
	t.Cleanup(svc.Shutdown)
	l, err := NewListener(config.PostgresConfig{}, svc)
	if err != nil {
		t.Fatal(err)
	}
	l.parser.Parse(buildRelationMsg(1, "public", "users", []testCol{{name: "id", typeOID: 23}}), "0/0")
	ch, unsub := svc.SubscribeAll()
	t.Cleanup(unsub)
	return l, ch
}

// Rows sharing one WAL record (multi-insert) must still get distinct ids, and
// the same transaction streamed again (reconnect, restart) must get the same.
func TestSourceIDIsTransactionPlusIndexAndStableAcrossReStreams(t *testing.T) {
	l, ch := newSourceIDListener(t)

	first := streamTransaction(t, l, ch, 0x1000, 3)
	want := []string{"pg:0/1000:0", "pg:0/1000:1", "pg:0/1000:2"}
	for i := range want {
		if first[i] != want[i] {
			t.Fatalf("ids = %v, want %v", first, want)
		}
	}

	again := streamTransaction(t, l, ch, 0x1000, 3)
	for i := range first {
		if again[i] != first[i] {
			t.Fatalf("re-streamed ids = %v, want %v", again, first)
		}
	}

	next := streamTransaction(t, l, ch, 0x2000, 1)
	if next[0] != "pg:0/2000:0" {
		t.Errorf("next transaction id = %q", next[0])
	}
}
