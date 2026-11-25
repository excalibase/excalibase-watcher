package mysql

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

type fakeGTID struct{ gtid string }

func (f fakeGTID) GTIDNext() (gomysql.GTIDSet, error) { return parseGTIDSet(f.gtid) }

func txnGTID(gno int) fakeGTID { return fakeGTID{fmt.Sprintf("%s:%d", serverA, gno)} }

func newHandler(t *testing.T, executed string) (*eventHandler, <-chan cdc.Event) {
	t.Helper()
	svc := cdc.NewService()
	t.Cleanup(svc.Shutdown)
	ch, unsub := svc.SubscribeAll()
	t.Cleanup(unsub)
	return &eventHandler{service: svc, gate: newCommitGate(), executed: gtidSet(t, executed)}, ch
}

func rowsEvent(logPos uint32, ids ...int) *canal.RowsEvent {
	rows := make([][]interface{}, len(ids))
	for i, id := range ids {
		rows[i] = []interface{}{id}
	}
	return &canal.RowsEvent{
		Table:  &schema.Table{Schema: "shop", Name: "orders", Columns: []schema.TableColumn{{Name: "id"}}},
		Action: canal.InsertAction,
		Rows:   rows,
		Header: &replication.EventHeader{LogPos: logPos},
	}
}

func sourceIDs(ch <-chan cdc.Event, count int) []string {
	ids := make([]string, count)
	for i := range ids {
		ids[i] = (<-ch).SourceID
	}
	return ids
}

func mustDo(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// Ids come from the transaction's GTID and the row's order in it, so a replica
// that holds the same transaction at another file and position, after a
// failover, publishes the same ids and the stream drops them as duplicates.
func TestRowIDsFollowTheGTIDNotTheBinlogPosition(t *testing.T) {
	h, ch := newHandler(t, serverA+":1-3")
	stream := func(logPos uint32) []string {
		mustDo(t, h.OnGTID(nil, txnGTID(4)))
		mustDo(t, h.OnRow(rowsEvent(logPos, 1, 2)))
		mustDo(t, h.OnRow(rowsEvent(logPos+50, 3)))
		return sourceIDs(ch, 3)
	}
	primary := stream(700)
	replica := stream(0) // compressed payload rows, or another server's offsets
	want := []string{"mysql:" + serverA + ":4:0", "mysql:" + serverA + ":4:1", "mysql:" + serverA + ":4:2"}
	for i := range want {
		if primary[i] != want[i] || replica[i] != want[i] {
			t.Errorf("row %d ids %q and %q, want %q", i, primary[i], replica[i], want[i])
		}
	}
}

func TestUpdateEmitsOneEventPerOldNewPairWithItsOwnID(t *testing.T) {
	h, ch := newHandler(t, "")
	mustDo(t, h.OnGTID(nil, txnGTID(1)))
	update := rowsEvent(10, 1, 2, 3, 4)
	update.Action = canal.UpdateAction
	mustDo(t, h.OnRow(update))
	got := sourceIDs(ch, 2)
	if got[0] != "mysql:"+serverA+":1:0" || got[1] != "mysql:"+serverA+":1:1" {
		t.Fatalf("ids = %v", got)
	}
}

// A row outside any transaction cannot get an id that is both unique and
// stable; publishing it anyway could drop rows.
func TestRowOutsideATransactionStopsTheWatcher(t *testing.T) {
	h, ch := newHandler(t, "")
	if err := h.OnRow(rowsEvent(100, 1)); err == nil {
		t.Fatal("expected an error for a row with no GTID transaction")
	}
	select {
	case event := <-ch:
		t.Fatalf("row was published anyway: %+v", event)
	default:
	}
}

func TestXIDConfirmsTheSetIncludingTheTransaction(t *testing.T) {
	h, _ := newHandler(t, serverA+":1-3")
	mustDo(t, h.OnGTID(nil, txnGTID(4)))
	mustDo(t, h.OnRow(rowsEvent(100, 1)))
	if got := confirmed(h.gate); got != "" {
		t.Fatalf("confirmed %q before the transaction ended", got)
	}
	mustDo(t, h.OnXID(nil, gomysql.Position{}))
	if got := confirmed(h.gate); got != serverA+":1-4" {
		t.Fatalf("confirmed %q, want %s:1-4", got, serverA)
	}
}

func TestDDLEndsItsTransactionWithAnIDOfItsOwn(t *testing.T) {
	h, ch := newHandler(t, serverA+":1-3")
	mustDo(t, h.OnGTID(nil, txnGTID(4)))
	mustDo(t, h.OnDDL(nil, gomysql.Position{},
		&replication.QueryEvent{Schema: []byte("shop"), Query: []byte("ALTER TABLE orders ADD c INT")}))
	if got := (<-ch).SourceID; got != "mysql:"+serverA+":4:0:ddl" {
		t.Errorf("DDL source id = %q", got)
	}
	if got := confirmed(h.gate); got != serverA+":1-4" {
		t.Fatalf("confirmed %q, want %s:1-4", got, serverA)
	}
}

func TestDDLOutsideATransactionStopsTheWatcher(t *testing.T) {
	h, _ := newHandler(t, "")
	err := h.OnDDL(nil, gomysql.Position{}, &replication.QueryEvent{Query: []byte("DROP TABLE t")})
	if err == nil {
		t.Fatal("expected an error for DDL with no GTID transaction")
	}
}

// A transaction canal reports no XID or DDL for (a statement it does not
// parse) still ends when the next one starts, so the set has no hole that
// would look like a purged transaction.
func TestATransactionWithoutABoundaryIsFoldedWhenTheNextOneStarts(t *testing.T) {
	h, _ := newHandler(t, serverA+":1-3")
	mustDo(t, h.OnGTID(nil, txnGTID(4)))
	mustDo(t, h.OnGTID(nil, txnGTID(5)))
	mustDo(t, h.OnXID(nil, gomysql.Position{}))
	if got := confirmed(h.gate); got != serverA+":1-5" {
		t.Fatalf("confirmed %q, want %s:1-5", got, serverA)
	}
}

func TestAnUnreadableGTIDEventStopsTheWatcher(t *testing.T) {
	h, _ := newHandler(t, "")
	if err := h.OnGTID(nil, fakeGTID{"garbage"}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestAnonymousGTIDStopsTheWatcher(t *testing.T) {
	h, _ := newHandler(t, "")
	anonymous := &replication.GTIDEvent{SID: make([]byte, 16), GNO: 0}
	if err := h.OnGTID(nil, anonymous); !errors.Is(err, errAnonymousGTID) {
		t.Fatalf("err = %v, want errAnonymousGTID", err)
	}
	zeroSID := &replication.GTIDEvent{SID: make([]byte, 16), GNO: 7}
	if err := h.OnGTID(nil, zeroSID); !errors.Is(err, errAnonymousGTID) {
		t.Fatalf("zero SID: err = %v, want errAnonymousGTID", err)
	}
	sid := []byte("0123456789abcdef")
	if err := h.OnGTID(nil, &replication.GTIDEvent{SID: sid, GNO: 0}); !errors.Is(err, errAnonymousGTID) {
		t.Fatalf("GNO 0: err = %v, want errAnonymousGTID", err)
	}
	if err := h.OnGTID(nil, fakeGTID{""}); !errors.Is(err, errAnonymousGTID) {
		t.Fatalf("empty GTID: err = %v, want errAnonymousGTID", err)
	}
	if h.open != nil {
		t.Fatal("a refused GTID opened a transaction")
	}
}

func TestATaggedGTIDStopsTheWatcher(t *testing.T) {
	h, _ := newHandler(t, "")
	tagged := &replication.GtidTaggedLogEvent{GTIDEvent: replication.GTIDEvent{SID: []byte("0123456789abcdef"), GNO: 3}}
	if err := h.OnGTID(nil, tagged); !errors.Is(err, errTaggedGTID) {
		t.Fatalf("err = %v, want errTaggedGTID", err)
	}
}

// After a commit the transaction is over: a row that arrives with no new GTID
// must hit the guard instead of reusing the finished transaction's ids.
func TestRowAfterACommitHasNoTransaction(t *testing.T) {
	h, ch := newHandler(t, serverA+":1-3")
	mustDo(t, h.OnGTID(nil, txnGTID(4)))
	mustDo(t, h.OnRow(rowsEvent(100, 1)))
	mustDo(t, h.OnXID(nil, gomysql.Position{}))
	<-ch
	<-ch
	if err := h.OnRow(rowsEvent(200, 2)); !errors.Is(err, errUnplacedRow) {
		t.Fatalf("err = %v, want errUnplacedRow", err)
	}
	if h.open != nil || h.events != 0 {
		t.Fatalf("transaction not cleared: open=%v events=%d", h.open, h.events)
	}
}

func TestDDLAfterACommitHasNoTransaction(t *testing.T) {
	h, _ := newHandler(t, serverA+":1-3")
	mustDo(t, h.OnGTID(nil, txnGTID(4)))
	mustDo(t, h.OnXID(nil, gomysql.Position{}))
	err := h.OnDDL(nil, gomysql.Position{}, &replication.QueryEvent{Query: []byte("DROP TABLE t")})
	if !errors.Is(err, errUnplacedRow) {
		t.Fatalf("err = %v, want errUnplacedRow", err)
	}
}

func TestAnUnplacedRowNamesTaggedGTIDsAsACause(t *testing.T) {
	if !strings.Contains(errUnplacedRow.Error(), "tagged") {
		t.Fatalf("message %q does not mention tagged GTIDs", errUnplacedRow)
	}
}
