package mysql

import (
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

// Rows inside a compressed transaction payload carry binlog position 0.
func compressedInsert(ids ...int) *canal.RowsEvent {
	rows := make([][]interface{}, len(ids))
	for i, id := range ids {
		rows[i] = []interface{}{id}
	}
	return &canal.RowsEvent{
		Table:  &schema.Table{Schema: "shop", Name: "orders", Columns: []schema.TableColumn{{Name: "id"}}},
		Action: canal.InsertAction,
		Rows:   rows,
		Header: &replication.EventHeader{LogPos: 0},
	}
}

func gtidHeader(end uint32) *replication.EventHeader {
	return &replication.EventHeader{LogPos: end, EventSize: 79}
}

func sourceIDs(t *testing.T, ch <-chan cdc.Event, count int) []string {
	t.Helper()
	ids := make([]string, count)
	for i := range ids {
		ids[i] = (<-ch).SourceID
	}
	return ids
}

func TestCompressedTransactionRowsGetDistinctIDsThatSurviveAReplay(t *testing.T) {
	svc := cdc.NewService()
	defer svc.Shutdown()
	ch, unsub := svc.SubscribeAll()
	defer unsub()
	h := &eventHandler{service: svc, gate: newCommitGate(), currentFile: "mysql-bin.000003"}

	stream := func() []string {
		var ids []string
		for _, txn := range []struct {
			gtidEnd uint32
			events  []*canal.RowsEvent
		}{
			{200, []*canal.RowsEvent{compressedInsert(1, 2), compressedInsert(3)}},
			{600, []*canal.RowsEvent{compressedInsert(4, 5)}},
		} {
			if err := h.OnGTID(gtidHeader(txn.gtidEnd), nil); err != nil {
				t.Fatal(err)
			}
			for _, rows := range txn.events {
				if err := h.OnRow(rows); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, sourceIDs(t, ch, len(rows.Rows))...)
			}
		}
		return ids
	}

	first := stream()
	want := []string{
		"mysql:mysql-bin.000003:200:txn:0", "mysql:mysql-bin.000003:200:txn:1", "mysql:mysql-bin.000003:200:txn:2",
		"mysql:mysql-bin.000003:600:txn:0", "mysql:mysql-bin.000003:600:txn:1",
	}
	for i := range want {
		if first[i] != want[i] {
			t.Errorf("row %d source id = %q, want %q", i, first[i], want[i])
		}
	}
	replay := stream()
	for i := range first {
		if replay[i] != first[i] {
			t.Errorf("row %d source id changed on replay: %q then %q", i, first[i], replay[i])
		}
	}
}

// A row with no position and no transaction to place it in cannot get an id
// that is both unique and stable; publishing it anyway could drop rows.
func TestRowThatCannotBePlacedStopsTheWatcher(t *testing.T) {
	svc := cdc.NewService()
	defer svc.Shutdown()
	ch, unsub := svc.SubscribeAll()
	defer unsub()
	h := &eventHandler{service: svc, gate: newCommitGate(), currentFile: "mysql-bin.000003"}

	if err := h.OnRow(compressedInsert(1)); err == nil {
		t.Fatal("expected an error for a row with neither a position nor a transaction")
	}
	select {
	case event := <-ch:
		t.Fatalf("row was published anyway: %+v", event)
	default:
	}
}

// A compressed transaction's XID carries position 0, so the start of the
// next transaction is what lets the offset move past it.
func TestTransactionStartIsAResumeBoundary(t *testing.T) {
	h := &eventHandler{service: cdc.NewService(), gate: newCommitGate(), currentFile: "mysql-bin.000003"}
	if err := h.OnGTID(gtidHeader(600), nil); err != nil {
		t.Fatal(err)
	}
	if pos, _ := h.gate.Confirmable(); pos != (BinlogPosition{File: "mysql-bin.000003", Position: 521}) {
		t.Errorf("boundary = %v, want mysql-bin.000003:521", pos)
	}
}
