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
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
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
	record      OffsetRecord
	savedOffset string
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

// NewListener keeps the offset in cfg.OffsetFile when it is set; otherwise
// call SetOffsetStore before Start, or the listener resumes from the server's
// current position on every start.
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

// SetOffsetStore replaces the store; call it before Start.
func (l *Listener) SetOffsetStore(store OffsetStore) {
	l.offsetStore = store
}

func (l *Listener) Start(ctx context.Context) error {
	if l.running.Load() {
		return errors.New("listener already running")
	}
	ctx, l.cancel = context.WithCancel(ctx)

	// Only the TLS setup can fail here; the server itself is checked once
	// connected, before the snapshot publishes anything.
	canalConfig, err := l.buildCanalConfig()
	if err != nil {
		return err
	}
	if l.offsetStore == nil {
		slog.Warn("mysql: no offset store; every restart resumes from the server's current position and loses what happened meanwhile")
	}
	if err := l.loadOffset(); err != nil {
		return err
	}

	c, err := canal.NewCanal(canalConfig)
	if err != nil {
		return fmt.Errorf("creating canal: %w", err)
	}
	l.canal = c

	if err := verifyServer(c, l.cfg.Schema, l.cfg.Tables); err != nil {
		c.Close()
		return err
	}
	if err := l.runSnapshot(); err != nil {
		c.Close()
		return fmt.Errorf("snapshot: %w", err)
	}
	startSet, err := l.resolveStartSet(c)
	if err != nil {
		c.Close()
		return err
	}

	handler := &eventHandler{service: l.service, gate: l.gate, executed: startSet.Clone()}
	c.SetEventHandler(handler)
	l.handler = handler

	l.running.Store(true)

	if l.offsetStore != nil {
		l.wg.Add(1)
		go l.offsetSaver(ctx)
	}

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.reportFailure(streamFrom(c, startSet))
	}()

	return nil
}

// reportFailure delivers the error that ended streaming, unless the listener
// was stopped on purpose.
func (l *Listener) reportFailure(err error) {
	if err == nil || !l.running.Load() {
		return
	}
	slog.Error("canal stopped with error", "error", err)
	l.failed <- err
}

// streamFrom runs canal from the GTID set. A server that has executed nothing
// yet has no set to name, so canal starts at its current binlog position,
// which is the same place.
func streamFrom(c *canal.Canal, start gomysql.GTIDSet) error {
	if start.String() != "" {
		return c.StartFromGTID(start)
	}
	pos, err := c.GetMasterPos()
	if err != nil {
		return fmt.Errorf("getting the server's binlog position: %w", err)
	}
	return c.RunFrom(pos)
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

// loadOffset reads the saved offset and applies mysql.reset_offset: a token
// not used before discards the offset, and is saved so it is used only once.
func (l *Listener) loadOffset() error {
	if l.offsetStore == nil {
		return nil
	}
	saved, err := l.offsetStore.Load()
	// A lost offset comes back with the token the marker holds; only a token
	// different from it may wave the loss through.
	if err != nil && !(errors.Is(err, ErrOffsetLost) && l.cfg.ResetOffset != "" && l.cfg.ResetOffset != saved.ResetToken) {
		return fmt.Errorf("loading saved offset: %w", err)
	}
	record, reset := applyResetToken(saved, l.cfg.ResetOffset)
	if reset {
		slog.Warn("mysql.reset_offset: discarding the saved offset, streaming from the server's current position",
			"token", l.cfg.ResetOffset)
		if err := l.offsetStore.Save(record); err != nil {
			return fmt.Errorf("saving the reset token: %w", err)
		}
	}
	if err := record.usable(); err != nil {
		return err
	}
	l.record = record
	return nil
}

// saveOffset persists a new GTID set together with the reset token already used.
func (l *Listener) saveOffset(gtidSet string) error {
	next := l.record
	next.GTIDSet = gtidSet
	if err := l.offsetStore.Save(next); err != nil {
		return err
	}
	l.record = next
	l.savedOffset = gtidSet
	return nil
}

// resolveStartSet returns the GTID set to stream from. On first start that is
// everything the server has executed, never the start of the binlog, which
// would flood the pipeline; the set is saved at once so a restart before the
// first transaction still resumes from the same place.
func (l *Listener) resolveStartSet(c *canal.Canal) (gomysql.GTIDSet, error) {
	executed, purged, err := serverGTIDSets(c)
	if err != nil {
		return nil, err
	}
	start, fresh, err := resolveStart(l.record.GTIDSet, executed, purged)
	if err != nil {
		return nil, err
	}
	if !fresh {
		slog.Info("resuming from the saved GTID set", "gtid_set", start.String())
		return start, nil
	}
	slog.Info("no saved offset, starting from the server's executed GTID set", "gtid_set", start.String())
	if l.offsetStore != nil && start.String() != "" {
		if err := l.saveOffset(start.String()); err != nil {
			return nil, fmt.Errorf("saving initial offset: %w", err)
		}
	}
	return start, nil
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

	// Flush the last fully acked boundary, so restart re-delivers every
	// transaction that wasn't fully acked.
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
	gtidSet, ok := l.gate.Confirmable()
	if !ok || gtidSet == l.savedOffset {
		return nil
	}
	return l.saveOffset(gtidSet)
}

func (l *Listener) IsRunning() bool {
	return l.running.Load()
}

func (l *Listener) runSnapshot() error {
	switch l.cfg.SnapshotMode {
	case "backup_file":
		if l.cfg.SnapshotBackupFile == "" {
			return errors.New("snapshot_backup_file required for backup_file mode")
		}
		gtidSet, err := ParseMysqlDumpFile(l.cfg.SnapshotBackupFile, l.cfg.Schema, func(e cdc.Event) {
			l.service.HandleEvent(e)
		})
		if err != nil {
			return err
		}
		if l.offsetStore != nil && gtidSet != "" {
			return l.saveOffset(gtidSet)
		}
		return nil
	default:
		return nil
	}
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
