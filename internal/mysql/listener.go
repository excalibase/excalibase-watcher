package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"strconv"
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

	// ackedLSNProvider returns the most recent binlog position confirmed
	// by the downstream publisher (e.g., NATS). The listener advances its
	// persisted offset based on this, guaranteeing offset ≤ delivered.
	ackedLSNProvider func() string
}

// SetAckedLSNProvider wires up the durability source for offset saves.
// Must be called before Start. Typically: listener.SetAckedLSNProvider(natsPub.LastAckedLSN).
func (l *Listener) SetAckedLSNProvider(fn func() string) {
	l.ackedLSNProvider = fn
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
	}, nil
}

func (l *Listener) Start(ctx context.Context) error {
	if l.running.Load() {
		return errors.New("listener already running")
	}
	ctx, l.cancel = context.WithCancel(ctx)

	if err := l.runSnapshot(); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}

	c, err := canal.NewCanal(l.buildCanalConfig())
	if err != nil {
		return fmt.Errorf("creating canal: %w", err)
	}
	l.canal = c

	handler := &eventHandler{service: l.service, schema: l.cfg.Schema, offsetStore: l.offsetStore}
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

	if l.offsetStore != nil && l.ackedLSNProvider != nil {
		l.wg.Add(1)
		go l.offsetSaver(ctx)
	}

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		runErr := c.RunFrom(mysql.Position{Name: startPos.File, Pos: startPos.Position})
		if runErr != nil && l.running.Load() {
			slog.Error("canal stopped with error", "error", runErr)
		}
	}()

	return nil
}

func (l *Listener) buildCanalConfig() *canal.Config {
	cfg := canal.NewDefaultConfig()
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
	return cfg
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

	// Flush the final acked LSN — never the canal position — so restart
	// re-delivers anything that wasn't acked.
	if l.offsetStore != nil && l.ackedLSNProvider != nil {
		if err := l.saveAckedOffset(); err != nil {
			slog.Warn("failed to flush final offset", "error", err)
		}
	}
}

// offsetSaver periodically persists the last-acked binlog position.
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
			if err := l.saveAckedOffset(); err != nil {
				slog.Warn("failed to save acked offset", "error", err)
			}
		}
	}
}

// saveAckedOffset parses the last-acked LSN ("file:pos") and writes it to the
// offset store. Returns nil if there's nothing to save.
func (l *Listener) saveAckedOffset() error {
	lsn := l.ackedLSNProvider()
	if lsn == "" {
		return nil
	}
	colon := strings.LastIndex(lsn, ":")
	if colon < 0 {
		return fmt.Errorf("invalid acked LSN %q", lsn)
	}
	file := lsn[:colon]
	posStr := lsn[colon+1:]
	pos, err := strconv.ParseUint(posStr, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid position in LSN %q: %w", lsn, err)
	}
	return l.offsetStore.Save(BinlogPosition{File: file, Position: uint32(pos)})
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
	service     *cdc.Service
	schema      string
	offsetStore OffsetStore
	eventCount  int64
	lastSave    time.Time

	// Position tracking for at-least-once offset semantics.
	// lastPos is the most recent committed position (from OnXID).
	// currentFile is updated by OnRotate; OnRow combines it with Header.LogPos.
	lastPosMu   sync.Mutex
	lastPos     mysql.Position
	currentFile string
}

// currentBinlogFile returns the current binlog filename (tracked via OnRotate).
func (h *eventHandler) currentBinlogFile() string {
	h.lastPosMu.Lock()
	defer h.lastPosMu.Unlock()
	return h.currentFile
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	schemaName := e.Table.Schema
	table := e.Table.Name
	// Stamp each event with its binlog position so the NATS publisher can
	// drive offset persistence based on confirmed delivery, not canal's
	// current position.
	lsn := ""
	if file := h.currentBinlogFile(); file != "" && e.Header != nil {
		lsn = fmt.Sprintf("%s:%d", file, e.Header.LogPos)
	}

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			data := rowToJSON(e.Table, row)
			event := cdc.NewEvent(cdc.Insert, schemaName, table, data, "INSERT", lsn)
			h.service.HandleEvent(event)
		}
	case canal.UpdateAction:
		for i := 0; i+1 < len(e.Rows); i += 2 {
			oldData := rowToJSON(e.Table, e.Rows[i])
			newData := rowToJSON(e.Table, e.Rows[i+1])
			data := fmt.Sprintf(`{"old":%s, "new":%s}`, oldData, newData)
			event := cdc.NewEvent(cdc.Update, schemaName, table, data, "UPDATE", lsn)
			h.service.HandleEvent(event)
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			data := rowToJSON(e.Table, row)
			event := cdc.NewEvent(cdc.Delete, schemaName, table, data, "DELETE", lsn)
			h.service.HandleEvent(event)
		}
	}
	return nil
}

// OnRotate tracks the current binlog file so OnRow can stamp events with
// the correct position.
func (h *eventHandler) OnRotate(_ *replication.EventHeader, ev *replication.RotateEvent) error {
	h.lastPosMu.Lock()
	h.currentFile = string(ev.NextLogName)
	h.lastPosMu.Unlock()
	return nil
}

func (h *eventHandler) OnDDL(_ *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	ddlSchema := string(queryEvent.Schema)
	query := string(queryEvent.Query)
	// Match Java format: raw SQL as data field
	event := cdc.NewEvent(cdc.DDL, ddlSchema, "", query, "DDL", "")
	h.service.HandleEvent(event)
	return nil
}

func (h *eventHandler) OnXID(_ *replication.EventHeader, nextPos mysql.Position) error {
	event := cdc.NewEvent(cdc.Commit, "", "", "", "COMMIT", "")
	h.service.HandleEvent(event)

	h.lastPosMu.Lock()
	h.lastPos = nextPos
	h.lastPosMu.Unlock()

	// Do NOT save offset here. Offset is driven by NATS publisher's
	// LastAckedLSN to guarantee we never save a position ahead of delivery.
	// See Listener.offsetSaver goroutine.
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
