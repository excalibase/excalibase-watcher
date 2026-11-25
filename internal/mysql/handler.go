package mysql

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// eventHandler implements canal.EventHandler. Only canal's goroutine calls
// it, so its fields need no lock.
//
// Events are placed by the transaction they belong to, named by its GTID, not
// by a binlog file and position: those differ between a primary and its
// replicas, and a failover would then replay events under new ids and defeat
// the stream's duplicate check. The GTID and the order of rows inside the
// transaction are the same on every server.
type eventHandler struct {
	canal.DummyEventHandler
	service *cdc.Service
	gate    *commitGate

	// executed is every transaction closed so far, the offset to save at the
	// next boundary.
	executed gomysql.GTIDSet
	// open is the current transaction's GTID; events counts what it emitted
	// so far.
	open   gomysql.GTIDSet
	events int
}

// errUnplacedRow means an event arrived outside any transaction, so no message
// id would be both unique and stable on replay.
var errUnplacedRow = errors.New("mysql event has no enclosing GTID transaction; " +
	"refusing to publish it under an id that could collide (is gtid_mode ON, or is this a tagged GTID?)")

// errAnonymousGTID is a transaction with no GTID. Its events would all share
// one id, and JetStream would silently drop every row after the first.
var errAnonymousGTID = errors.New("mysql transaction has an anonymous GTID (gtid_mode is not ON, or gtid_next was set to ANONYMOUS); " +
	"refusing to publish it under an id that would collide with others")

// errTaggedGTID is a MySQL 8.4 tagged GTID, which the offset and ids here do
// not represent.
var errTaggedGTID = errors.New("mysql transaction has a tagged GTID, which this watcher does not support")

// emit passes an event to the bus and counts it toward the commit gate when
// it will be published.
func (h *eventHandler) emit(event cdc.Event) {
	if h.service.HandleEvent(event) && ownEvent(event) {
		h.gate.Hand()
	}
}

// nextSourceID returns the id of the next event of the open transaction.
func (h *eventHandler) nextSourceID(suffix string) (string, error) {
	if h.open == nil {
		return "", errUnplacedRow
	}
	id := fmt.Sprintf("%s%s:%d%s", sourcePrefix, h.open, h.events, suffix)
	h.events++
	return id, nil
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	switch e.Action {
	case canal.InsertAction:
		return h.emitRows(e, cdc.Insert, "INSERT", 1, func(i int) string { return rowToJSON(e.Table, e.Rows[i]) })
	case canal.DeleteAction:
		return h.emitRows(e, cdc.Delete, "DELETE", 1, func(i int) string { return rowToJSON(e.Table, e.Rows[i]) })
	case canal.UpdateAction:
		return h.emitRows(e, cdc.Update, "UPDATE", 2, func(i int) string {
			return fmt.Sprintf(`{"old":%s, "new":%s}`, rowToJSON(e.Table, e.Rows[i]), rowToJSON(e.Table, e.Rows[i+1]))
		})
	}
	return nil
}

// emitRows publishes one event per step rows, in binlog order; data renders
// the event whose first row is i.
func (h *eventHandler) emitRows(e *canal.RowsEvent, kind cdc.EventType, op string, step int, data func(i int) string) error {
	for i := 0; i+step <= len(e.Rows); i += step {
		id, err := h.nextSourceID("")
		if err != nil {
			return err
		}
		event := cdc.NewEvent(kind, e.Table.Schema, e.Table.Name, data(i), op, h.open.String())
		event.SourceID = id
		h.emit(event)
	}
	return nil
}

// OnGTID opens a transaction. A transaction that ended without a boundary
// (one canal reports no DDL or XID for) is closed here, since the next one
// can only begin after it committed.
func (h *eventHandler) OnGTID(_ *replication.EventHeader, event gomysql.BinlogGTIDEvent) error {
	if err := refuseUnsupportedGTID(event); err != nil {
		return err
	}
	if err := h.foldOpen(); err != nil {
		return err
	}
	next, err := event.GTIDNext()
	if err != nil {
		return fmt.Errorf("reading the transaction's GTID: %w", err)
	}
	if next.String() == "" {
		return errAnonymousGTID
	}
	h.open, h.events = next, 0
	return nil
}

func refuseUnsupportedGTID(event gomysql.BinlogGTIDEvent) error {
	switch e := event.(type) {
	case *replication.GtidTaggedLogEvent:
		return errTaggedGTID
	case *replication.GTIDEvent:
		if e.GNO <= 0 || bytes.Equal(e.SID, make([]byte, len(e.SID))) {
			return errAnonymousGTID
		}
	}
	return nil
}

// foldOpen adds the open transaction to the executed set; adding it twice
// changes nothing.
func (h *eventHandler) foldOpen() error {
	if h.open == nil {
		return nil
	}
	executed, ok := h.executed.(*gomysql.MysqlGTIDSet)
	open, openOK := h.open.(*gomysql.MysqlGTIDSet)
	if !ok || !openOK {
		return errors.New("mysql: GTID sets of an unexpected flavor")
	}
	if err := executed.Add(*open); err != nil {
		return fmt.Errorf("adding %s to the executed set: %w", h.open, err)
	}
	return nil
}

// closeTransaction is a boundary: everything handed so far belongs to
// transactions that ended, and the offset may move to the set including them.
func (h *eventHandler) closeTransaction() error {
	if err := h.foldOpen(); err != nil {
		return err
	}
	h.gate.Committed(h.executed.String())
	h.open, h.events = nil, 0
	return nil
}

func (h *eventHandler) OnDDL(_ *replication.EventHeader, _ gomysql.Position, queryEvent *replication.QueryEvent) error {
	// Match Java format: raw SQL as data field
	event := cdc.NewEvent(cdc.DDL, string(queryEvent.Schema), "", string(queryEvent.Query), "DDL", "")
	id, err := h.nextSourceID(":ddl")
	if err != nil {
		return err
	}
	event.SourceID = id
	h.emit(event)
	return h.closeTransaction()
}

// OnXID closes a transaction. Its offset is only persisted once every row
// handed before it is acked (see commitGate), never per rows event.
func (h *eventHandler) OnXID(_ *replication.EventHeader, _ gomysql.Position) error {
	h.service.HandleEvent(cdc.NewEvent(cdc.Commit, "", "", "", "COMMIT", ""))
	return h.closeTransaction()
}

func (h *eventHandler) String() string {
	return "excalibase-watcher-mysql"
}
