package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/excalibase/watcher-go/internal/jsonutil"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

type Listener struct {
	cfg         config.MySQLConfig
	service     *cdc.Service
	offsetStore OffsetStore
	canal       *canal.Canal
	handler     *eventHandler
	running     atomic.Bool
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	gate        *commitGate
	savedOffset BinlogPosition
	failed      chan error
}

// Failed delivers the error that stopped binlog streaming while the listener
// was meant to be running; the watcher exits on it instead of idling.
func (l *Listener) Failed() <-chan error {
	return l.failed
}

// PublishedObserver enables ack gating and returns the callback the NATS
// publisher must invoke after each ack. Wire it before Start, so the offset
// only advances past transactions NATS has fully acknowledged.
func (l *Listener) PublishedObserver() func(cdc.Event) {
	l.gate.Enable()
	return l.gate.ObservePublished
}

func NewListener(cfg config.MySQLConfig, service *cdc.Service) (*Listener, error) {
	var store OffsetStore
	if cfg.OffsetFile != "" {
		store = NewFileOffsetStore(cfg.OffsetFile)
	}

	return &Listener{
		cfg:         cfg,
		service:     service,
		offsetStore: store,
		gate:        newCommitGate(),
		failed:      make(chan error, 1),
	}, nil
}

func (l *Listener) Start(ctx context.Context) error {
	if l.running.Load() {
		return errors.New("listener already running")
	}
	ctx, l.cancel = context.WithCancel(ctx)

	// Checked before the snapshot, so nothing is published from a server
	// the watcher will not connect to.
	canalConfig, err := l.buildCanalConfig()
	if err != nil {
		return err
	}

	if err := l.runSnapshot(); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}

	c, err := canal.NewCanal(canalConfig)
	if err != nil {
		return fmt.Errorf("creating canal: %w", err)
	}
	l.canal = c

	handler := &eventHandler{service: l.service, gate: l.gate}
	c.SetEventHandler(handler)
	l.handler = handler

	startPos, err := l.resolveStartPosition(c)
	if err != nil {
		return err
	}

	handler.lastPosMu.Lock()
	handler.currentFile = startPos.File
	handler.lastPosMu.Unlock()

	l.running.Store(true)

	if l.offsetStore != nil {
		l.wg.Add(1)
		go l.offsetSaver(ctx)
	}

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		runErr := c.RunFrom(mysql.Position{Name: startPos.File, Pos: startPos.Position})
		if runErr != nil && l.running.Load() {
			slog.Error("canal stopped with error", "error", runErr)
			l.failed <- runErr
		}
	}()

	return nil
}

func (l *Listener) buildCanalConfig() (*canal.Config, error) {
	tlsConfig, err := serverTLS(l.cfg.CACert, l.cfg.Host)
	if err != nil {
		return nil, err
	}
	cfg := canal.NewDefaultConfig()
	cfg.TLSConfig = tlsConfig
	cfg.Addr = l.cfg.Host
	cfg.User = l.cfg.Username
	cfg.Password = l.cfg.Password
	cfg.Flavor = "mysql"
	cfg.ServerID = generateServerID(l.cfg.Host)
	cfg.Dump.ExecutionPath = "" // disable canal's built-in dump
	cfg.ParseTime = true
	cfg.UseDecimal = true

	// Table filtering — QuoteMeta prevents regex injection from config values
	for _, t := range l.cfg.Tables {
		cfg.IncludeTableRegex = append(cfg.IncludeTableRegex,
			fmt.Sprintf(`^%s\.%s$`, regexp.QuoteMeta(l.cfg.Schema), regexp.QuoteMeta(t)))
	}
	return cfg, nil
}

// resolveStartPosition returns the persisted offset, or the current master
// position on first start. Canal with dump disabled would otherwise begin at
// position 0 of the oldest binlog file and flood the pipeline on startup.
func (l *Listener) resolveStartPosition(c *canal.Canal) (*BinlogPosition, error) {
	startPos, err := l.getStartPosition()
	if err != nil {
		return nil, fmt.Errorf("getting start position: %w", err)
	}
	if startPos != nil {
		return startPos, nil
	}
	pos, err := c.GetMasterPos()
	if err != nil {
		return nil, fmt.Errorf("getting current master position: %w", err)
	}
	startPos = &BinlogPosition{File: pos.Name, Position: pos.Pos}
	slog.Info("no persisted offset, starting from current master position",
		"file", pos.Name, "position", pos.Pos)
	if l.offsetStore != nil {
		if err := l.offsetStore.Save(*startPos); err != nil {
			slog.Warn("failed to save initial offset", "error", err)
		} else {
			l.savedOffset = *startPos
		}
	}
	return startPos, nil
}

func (l *Listener) Stop() {
	if !l.running.Swap(false) {
		return
	}
	if l.canal != nil {
		l.canal.Close()
	}
	if l.cancel != nil {
		l.cancel()
	}
	l.wg.Wait()

	// Flush the last fully acked boundary — never the canal position — so
	// restart re-delivers every transaction that wasn't fully acked.
	if l.offsetStore != nil {
		if err := l.saveConfirmedOffset(); err != nil {
			slog.Warn("failed to flush final offset", "error", err)
		}
	}
}

// offsetSaver periodically persists the last fully acked transaction boundary.
// Runs as a goroutine; exits when the context is cancelled.
func (l *Listener) offsetSaver(ctx context.Context) {
	defer l.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := l.saveConfirmedOffset(); err != nil {
				slog.Warn("failed to save confirmed offset", "error", err)
			}
		}
	}
}

// saveConfirmedOffset writes the newest boundary whose events are all acked,
// when it moved since the last save.
func (l *Listener) saveConfirmedOffset() error {
	pos, ok := l.gate.Confirmable()
	if !ok || pos == l.savedOffset {
		return nil
	}
	if err := l.offsetStore.Save(pos); err != nil {
		return err
	}
	l.savedOffset = pos
	return nil
}

func (l *Listener) IsRunning() bool {
	return l.running.Load()
}

func (l *Listener) getStartPosition() (*BinlogPosition, error) {
	if l.offsetStore == nil {
		return nil, nil
	}
	return l.offsetStore.Load()
}

func (l *Listener) runSnapshot() error {
	switch l.cfg.SnapshotMode {
	case "backup_file":
		if l.cfg.SnapshotBackupFile == "" {
			return errors.New("snapshot_backup_file required for backup_file mode")
		}
		pos, err := ParseMysqlDumpFile(l.cfg.SnapshotBackupFile, l.cfg.Schema, func(e cdc.Event) {
			l.service.HandleEvent(e)
		})
		if err != nil {
			return err
		}
		if l.offsetStore != nil && pos.File != "" {
			return l.offsetStore.Save(pos)
		}
		return nil
	default:
		return nil
	}
}

// eventHandler implements canal.EventHandler
type eventHandler struct {
	canal.DummyEventHandler
	service *cdc.Service
	gate    *commitGate

	// currentFile is updated by OnRotate; OnRow combines it with Header.LogPos.
	lastPosMu   sync.Mutex
	currentFile string

	// txn is the open transaction (end of its GTID event) and txnRows the
	// rows seen in it so far; only canal's goroutine touches them.
	txn     BinlogPosition
	txnRows int
}

// emit passes an event to the bus and counts it toward the commit gate when
// it will be published.
func (h *eventHandler) emit(event cdc.Event) {
	if h.service.HandleEvent(event) && ownEvent(event) {
		h.gate.Hand()
	}
}

func (h *eventHandler) committed(pos mysql.Position) {
	h.gate.Committed(BinlogPosition{File: pos.Name, Position: pos.Pos})
}

// currentBinlogFile returns the current binlog filename (tracked via OnRotate).
func (h *eventHandler) currentBinlogFile() string {
	h.lastPosMu.Lock()
	defer h.lastPosMu.Unlock()
	return h.currentFile
}

// errUnplacedRow means a row arrived with no binlog position and no enclosing
// transaction, so no message id would be both unique and stable on replay.
var errUnplacedRow = errors.New("mysql row event has no binlog position and no enclosing transaction; " +
	"refusing to publish it under an id that could collide")

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	rowID, lsn, err := h.rowIDs(e)
	if err != nil {
		return err
	}
	publish := func(event cdc.Event, row int) {
		if rowID != nil {
			event.SourceID = rowID(row)
		}
		h.emit(event)
	}

	schemaName := e.Table.Schema
	table := e.Table.Name
	switch e.Action {
	case canal.InsertAction:
		for i, row := range e.Rows {
			data := rowToJSON(e.Table, row)
			publish(cdc.NewEvent(cdc.Insert, schemaName, table, data, "INSERT", lsn), i)
		}
	case canal.UpdateAction:
		for i := 0; i+1 < len(e.Rows); i += 2 {
			oldData := rowToJSON(e.Table, e.Rows[i])
			newData := rowToJSON(e.Table, e.Rows[i+1])
			data := fmt.Sprintf(`{"old":%s, "new":%s}`, oldData, newData)
			publish(cdc.NewEvent(cdc.Update, schemaName, table, data, "UPDATE", lsn), i/2)
		}
	case canal.DeleteAction:
		for i, row := range e.Rows {
			data := rowToJSON(e.Table, row)
			publish(cdc.NewEvent(cdc.Delete, schemaName, table, data, "DELETE", lsn), i)
		}
	}
	return nil
}

// rowIDs returns how to build each row's source id, and the event's position.
//
// A rows event's own position is shared by all its rows, so the row index
// tells them apart. Events inside a compressed transaction payload carry
// position 0, so their rows are placed by the enclosing transaction (the end
// of its GTID event) and their order within it. Both are the same on replay.
func (h *eventHandler) rowIDs(e *canal.RowsEvent) (func(row int) string, string, error) {
	file := h.currentBinlogFile()
	if file == "" || e.Header == nil {
		return nil, "", nil
	}
	if e.Header.LogPos != 0 {
		lsn := fmt.Sprintf("%s:%d", file, e.Header.LogPos)
		return func(row int) string { return fmt.Sprintf("%s%s:%d", sourcePrefix, lsn, row) }, lsn, nil
	}
	if h.txn.File == "" {
		return nil, "", errUnplacedRow
	}
	lsn := h.txn.String()
	first := h.txnRows
	h.txnRows += rowCount(e)
	return func(row int) string { return fmt.Sprintf("%s%s:txn:%d", sourcePrefix, lsn, first+row) }, lsn, nil
}

func rowCount(e *canal.RowsEvent) int {
	if e.Action == canal.UpdateAction {
		return len(e.Rows) / 2
	}
	return len(e.Rows)
}

// OnGTID opens a transaction (anonymous GTIDs included). Where its GTID event
// starts is a transaction boundary: the only one a compressed transaction
// offers, since the XID inside its payload carries position 0.
func (h *eventHandler) OnGTID(header *replication.EventHeader, _ mysql.BinlogGTIDEvent) error {
	h.txn = BinlogPosition{}
	h.txnRows = 0
	file := h.currentBinlogFile()
	if header == nil || file == "" || header.LogPos == 0 {
		return nil
	}
	h.txn = BinlogPosition{File: file, Position: header.LogPos}
	if header.LogPos >= header.EventSize {
		h.gate.Committed(BinlogPosition{File: file, Position: header.LogPos - header.EventSize})
	}
	return nil
}

// OnRotate tracks the current binlog file so OnRow can stamp events with
// the correct position. A rotate falls between transactions, so it is a
// boundary too: an idle offset moves on to the new file.
func (h *eventHandler) OnRotate(_ *replication.EventHeader, ev *replication.RotateEvent) error {
	h.lastPosMu.Lock()
	h.currentFile = string(ev.NextLogName)
	h.lastPosMu.Unlock()
	h.committed(mysql.Position{Name: string(ev.NextLogName), Pos: uint32(ev.Position)})
	return nil
}

func (h *eventHandler) OnDDL(_ *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	ddlSchema := string(queryEvent.Schema)
	query := string(queryEvent.Query)
	// Match Java format: raw SQL as data field
	event := cdc.NewEvent(cdc.DDL, ddlSchema, "", query, "DDL", "")
	if nextPos.Name != "" {
		event.SourceID = fmt.Sprintf("%s%s:%d:ddl", sourcePrefix, nextPos.Name, nextPos.Pos)
	}
	h.emit(event)
	h.committed(nextPos)
	return nil
}

// OnXID closes a transaction. Its position is only persisted once every row
// handed before it is acked (see commitGate), never per rows event.
func (h *eventHandler) OnXID(_ *replication.EventHeader, nextPos mysql.Position) error {
	h.service.HandleEvent(cdc.NewEvent(cdc.Commit, "", "", "", "COMMIT", ""))
	h.committed(nextPos)
	return nil
}

func (h *eventHandler) String() string {
	return "excalibase-watcher-mysql"
}

func rowToJSON(table *schema.Table, row []interface{}) string {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, col := range table.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('"')
		sb.WriteString(jsonutil.EscapeString(col.Name))
		sb.WriteString(`":`)

		if i < len(row) && row[i] != nil {
			appendValue(&sb, row[i], col.Type)
		} else {
			sb.WriteString("null")
		}
	}
	sb.WriteByte('}')
	return sb.String()
}

func appendValue(sb *strings.Builder, val interface{}, colType int) {
	// MySQL JSON column — emit as raw JSON (canal gives us a JSON-formatted string/bytes).
	if colType == schema.TYPE_JSON {
		var s string
		switch v := val.(type) {
		case []byte:
			s = string(v)
		case string:
			s = v
		default:
			s = fmt.Sprintf("%v", v)
		}
		if json.Valid([]byte(s)) {
			sb.WriteString(s)
			return
		}
		// Fall through to quoted string if invalid
	}

	switch v := val.(type) {
	case int, int8, int16, int32, int64:
		sb.WriteString(fmt.Sprintf("%d", v))
	case uint, uint8, uint16, uint32, uint64:
		sb.WriteString(fmt.Sprintf("%d", v))
	case float32, float64:
		sb.WriteString(fmt.Sprintf("%v", v))
	case bool:
		if v {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case []byte:
		sb.WriteByte('"')
		sb.WriteString(jsonutil.EscapeString(string(v)))
		sb.WriteByte('"')
	case string:
		sb.WriteByte('"')
		sb.WriteString(jsonutil.EscapeString(v))
		sb.WriteByte('"')
	case time.Time:
		sb.WriteByte('"')
		sb.WriteString(v.Format("2006-01-02T15:04:05.000Z07:00"))
		sb.WriteByte('"')
	default:
		data, err := json.Marshal(v)
		if err != nil {
			sb.WriteString(fmt.Sprintf(`"%v"`, v))
		} else {
			sb.Write(data)
		}
	}
}

func generateServerID(host string) uint32 {
	h := uint32(0)
	for _, c := range host {
		h = h*31 + uint32(c)
	}
	return 65536 + uint32(math.Abs(float64(h)))%65536
}
