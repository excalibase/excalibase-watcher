package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/excalibase/watcher-go/internal/metrics"
	"github.com/excalibase/watcher-go/internal/schema"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	heartbeatInterval        = 30 * time.Second
	statusInterval           = 10 * time.Second
	maxReconnectDelay        = 30 * time.Second
	defaultSlotStatsInterval = 15 * time.Second
	maintenanceQueryTimeout  = 5 * time.Second
)

type Listener struct {
	cfg         config.PostgresConfig
	service     *cdc.Service
	parser      *Parser
	schemaStore schema.HistoryStore
	lag         *lagTracker
	ack         *ackGate

	conn *pgconn.PgConn
	// maintenance serves slot-stat sampling; separate from the replication
	// connection, which cannot run SQL.
	maintenance *pgxpool.Pool
	running     atomic.Bool
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// replicationState is the per-connection bookkeeping of one streaming session.
type replicationState struct {
	receivedLSN   pglogrepl.LSN // end of the last WAL record or keepalive position received
	lastFlushSent pglogrepl.LSN
	lastStatus    time.Time
	lastMessage   time.Time
}

func NewListener(cfg config.PostgresConfig, service *cdc.Service) (*Listener, error) {
	var tableFilter map[string]struct{}
	if len(cfg.Tables) > 0 {
		tableFilter = make(map[string]struct{}, len(cfg.Tables))
		for _, t := range cfg.Tables {
			tableFilter[t] = struct{}{}
		}
	}

	var store schema.HistoryStore
	if cfg.SchemaHistoryDir != "" {
		var err error
		store, err = schema.NewFileHistoryStore(cfg.SchemaHistoryDir)
		if err != nil {
			return nil, fmt.Errorf("creating schema history store: %w", err)
		}
	}

	listener := &Listener{
		cfg:         cfg,
		service:     service,
		parser:      NewParser(tableFilter, cfg.CaptureDDL, store),
		schemaStore: store,
		lag:         newLagTracker(time.Now),
		ack:         newAckGate(),
	}
	listener.parser.SetEventHandler(listener.emit)
	return listener, nil
}

// PublishedObserver enables ack gating and returns the callback the NATS
// publisher must invoke after each successful publish. Until wired, the
// listener confirms every received LSN to the primary.
func (l *Listener) PublishedObserver() func(cdc.Event) {
	l.ack.Enable()
	return l.ack.ObservePublished
}

// emit hands an event to the bus, remembering the position of publishable
// events so the standby status can be held back until NATS acks them.
func (l *Listener) emit(event cdc.Event) {
	if cdc.IsPublishable(event.Type) {
		if lsn, err := pglogrepl.ParseLSN(event.LSN); err == nil {
			l.ack.Handed(lsn)
		}
	}
	l.service.HandleEvent(event)
}

func (l *Listener) Start(ctx context.Context) error {
	if l.running.Load() {
		return errors.New("listener already running")
	}

	ctx, l.cancel = context.WithCancel(ctx)

	// Setup: publication, slot, DDL triggers (use standard connection, not replication)
	setupConn, err := connectStandard(ctx, l.cfg)
	if err != nil {
		return fmt.Errorf("setup connection: %w", err)
	}

	if l.cfg.CreatePublication {
		if err := createPublicationIfNotExists(ctx, setupConn, l.cfg.PublicationName); err != nil {
			setupConn.Close(ctx)
			return err
		}
	}
	if l.cfg.CreateSlot {
		if err := createReplicationSlotIfNotExists(ctx, setupConn, l.cfg.SlotName); err != nil {
			setupConn.Close(ctx)
			return err
		}
	}
	if l.cfg.CaptureDDL {
		if err := createDDLTriggerIfNotExists(ctx, setupConn); err != nil {
			setupConn.Close(ctx)
			return err
		}
	}
	setupConn.Close(ctx)

	// Run snapshot if configured
	if err := l.runSnapshot(ctx); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}

	pool, err := newMaintenancePool(ctx, l.cfg)
	if err != nil {
		return fmt.Errorf("maintenance pool: %w", err)
	}
	l.maintenance = pool

	// Start replication + slot sampling in background
	l.running.Store(true)
	l.wg.Add(2)
	go l.listenLoop(ctx)
	go l.sampleSlotStats(ctx)

	return nil
}

func (l *Listener) Stop() {
	if !l.running.Swap(false) {
		return
	}
	if l.cancel != nil {
		l.cancel()
	}
	l.wg.Wait()
	if l.conn != nil {
		l.conn.Close(context.Background())
		l.conn = nil
	}
	if l.maintenance != nil {
		l.maintenance.Close()
		l.maintenance = nil
	}
}

func (l *Listener) slotStatsInterval() time.Duration {
	if l.cfg.SlotStatsIntervalSeconds <= 0 {
		return defaultSlotStatsInterval
	}
	return time.Duration(l.cfg.SlotStatsIntervalSeconds) * time.Second
}

func (l *Listener) sampleSlotStats(ctx context.Context) {
	defer l.wg.Done()
	sampler := newSlotSampler(l.slotStatsInterval(), l.lag, func(ctx context.Context) (SlotStats, error) {
		ctx, cancel := context.WithTimeout(ctx, maintenanceQueryTimeout)
		defer cancel()
		return querySlotStats(ctx, l.maintenance, l.cfg.SlotName)
	})
	sampler.run(ctx)
}

func (l *Listener) IsRunning() bool {
	return l.running.Load()
}

func (l *Listener) listenLoop(ctx context.Context) {
	defer l.wg.Done()
	delay := time.Second

	for l.running.Load() {
		if err := l.connectAndStream(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("replication stream error, reconnecting",
				"error", err, "delay", delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
			delay = min(delay*2, maxReconnectDelay)
		} else {
			delay = time.Second // reset on success
		}
	}
}

func (l *Listener) connectAndStream(ctx context.Context) error {
	conn, err := l.openReplicationConn(ctx)
	if err != nil {
		return err
	}
	l.conn = conn
	defer func() {
		conn.Close(ctx)
		l.conn = nil
	}()

	if err := l.startReplication(ctx, conn); err != nil {
		return err
	}

	state := &replicationState{lastStatus: time.Now(), lastMessage: time.Now()}
	l.parser.SetLSNProvider(func() string { return state.receivedLSN.String() })

	for l.running.Load() {
		if ctx.Err() != nil {
			return nil
		}

		rawMsg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive message: %w", err)
		}

		if err := l.handleMessage(ctx, conn, rawMsg, state); err != nil {
			return err
		}

		if err := l.maintainHeartbeatAndStatus(ctx, conn, state); err != nil {
			return err
		}
	}

	return nil
}

// sendStatus confirms the ack-gated flush position to the primary. A zero
// position (nothing acked yet in this session) is sent as-is: Postgres treats
// it as "no progress" while still counting the message as a keepalive reply.
func (l *Listener) sendStatus(ctx context.Context, conn *pgconn.PgConn, state *replicationState) error {
	flush := l.ack.FlushLSN(state.receivedLSN)
	position := flush
	if position > 0 {
		position++ // Must add 1 for ack
	}
	err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: position})
	if err != nil {
		return fmt.Errorf("send status: %w", err)
	}
	state.lastFlushSent = flush
	state.lastStatus = time.Now()
	return nil
}

func (l *Listener) openReplicationConn(ctx context.Context) (*pgconn.PgConn, error) {
	if err := validateIdentifier(l.cfg.PublicationName); err != nil {
		return nil, fmt.Errorf("invalid publication name: %w", err)
	}
	if err := validateIdentifier(l.cfg.SlotName); err != nil {
		return nil, fmt.Errorf("invalid slot name: %w", err)
	}

	connStr := l.cfg.URL
	if l.cfg.Username != "" {
		if u, err := url.Parse(connStr); err == nil && u.User == nil {
			u.User = url.UserPassword(l.cfg.Username, l.cfg.Password)
			connStr = u.String()
		}
	}
	connStr = ensureReplicationParam(connStr)
	conn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	return conn, nil
}

func (l *Listener) startReplication(ctx context.Context, conn *pgconn.PgConn) error {
	if _, err := pglogrepl.IdentifySystem(ctx, conn); err != nil {
		return fmt.Errorf("identify system: %w", err)
	}
	err := pglogrepl.StartReplication(ctx, conn, l.cfg.SlotName, 0,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				"publication_names '" + l.cfg.PublicationName + "'",
			},
		})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	return nil
}

func (l *Listener) handleMessage(ctx context.Context, conn *pgconn.PgConn, rawMsg pgproto3.BackendMessage,
	state *replicationState) error {
	copyData, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		slog.Debug("unexpected message type", "msg", fmt.Sprintf("%T", rawMsg))
		return nil
	}
	switch copyData.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return l.handleKeepalive(ctx, conn, copyData.Data[1:], state)
	case pglogrepl.XLogDataByteID:
		return l.handleXLogData(copyData.Data[1:], state)
	}
	return nil
}

// handleKeepalive processes a primary keepalive. The walsender sends one when
// it has nothing more to send, so its position is safe to receive (every
// record before it was already streamed) and, if no event awaits a NATS ack,
// the watcher is caught up. A status is sent when the primary asks for a
// reply or when the confirmable position moved since the last status, which
// lets restart_lsn advance promptly during idle periods.
func (l *Listener) handleKeepalive(ctx context.Context, conn *pgconn.PgConn, data []byte,
	state *replicationState) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("parse keepalive: %w", err)
	}
	if pkm.ServerWALEnd > state.receivedLSN {
		state.receivedLSN = pkm.ServerWALEnd
	}
	if !l.ack.Pending() {
		l.lag.ObserveCaughtUp()
	}
	if !pkm.ReplyRequested && l.ack.FlushLSN(state.receivedLSN) == state.lastFlushSent {
		return nil
	}
	return l.sendStatus(ctx, conn, state)
}

func (l *Listener) handleXLogData(data []byte, state *replicationState) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("parse xlog data: %w", err)
	}
	state.receivedLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
	state.lastMessage = time.Now()
	metrics.SetLastEventTimestamp(state.lastMessage)

	event := l.parser.Parse(xld.WALData, state.receivedLSN.String())
	if event == nil {
		return nil
	}
	if event.Type == cdc.Commit && event.SourceTimestamp > 0 {
		l.lag.ObserveCommit(time.UnixMilli(event.SourceTimestamp))
	}
	l.emit(*event)
	return nil
}

func (l *Listener) maintainHeartbeatAndStatus(ctx context.Context, conn *pgconn.PgConn,
	state *replicationState) error {
	if time.Since(state.lastStatus) >= statusInterval {
		if err := l.sendStatus(ctx, conn, state); err != nil {
			return err
		}
	}
	if time.Since(state.lastMessage) >= heartbeatInterval {
		e := cdc.NewEvent(cdc.Heartbeat, "", "", "", "HEARTBEAT", state.receivedLSN.String())
		l.service.HandleEvent(e)
		state.lastMessage = time.Now()
		if err := l.sendStatus(ctx, conn, state); err != nil {
			return err
		}
	}
	return nil
}

func (l *Listener) runSnapshot(ctx context.Context) error {
	schemaName := parseSchema(l.cfg.URL)

	switch l.cfg.SnapshotMode {
	case "chunked":
		connStr := stripReplicationParam(l.cfg.URL)
		return RunChunkedSnapshot(ctx, connStr, schemaName, l.cfg.Tables,
			l.cfg.SnapshotChunkSize, func(e cdc.Event) {
				l.service.HandleEvent(e)
			})
	case "backup_file":
		if l.cfg.SnapshotBackupFile == "" {
			return errors.New("snapshot_backup_file required for backup_file mode")
		}
		return ParseDumpFile(l.cfg.SnapshotBackupFile, l.cfg.SnapshotStartLSN,
			func(e cdc.Event) {
				l.service.HandleEvent(e)
			})
	default:
		return nil // "none" or empty
	}
}

// connectStandard creates a standard (non-replication) pgx connection.
func connectStandard(ctx context.Context, cfg config.PostgresConfig) (*pgx.Conn, error) {
	return pgx.Connect(ctx, standardConnString(cfg))
}

// newMaintenancePool is a tiny lazily-connecting pool for periodic SQL
// (slot stats). It reconnects on its own after the database restarts or
// resumes from hibernation, so no maintenance goroutine has to manage that.
func newMaintenancePool(ctx context.Context, cfg config.PostgresConfig) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(standardConnString(cfg))
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = 2
	poolCfg.MinConns = 0
	poolCfg.MaxConnIdleTime = time.Minute
	return pgxpool.NewWithConfig(ctx, poolCfg)
}

func standardConnString(cfg config.PostgresConfig) string {
	connStr := stripReplicationParam(cfg.URL)
	if cfg.Username != "" {
		// Inject credentials if not in URL
		u, err := url.Parse(connStr)
		if err == nil && u.User == nil {
			u.User = url.UserPassword(cfg.Username, cfg.Password)
			connStr = u.String()
		}
	}
	return connStr
}

func ensureReplicationParam(connStr string) string {
	if strings.Contains(connStr, "replication=") {
		return connStr
	}
	if strings.Contains(connStr, "?") {
		return connStr + "&replication=database"
	}
	return connStr + "?replication=database"
}

func stripReplicationParam(connStr string) string {
	// Remove replication=database from connection string for standard connections
	connStr = strings.ReplaceAll(connStr, "&replication=database", "")
	connStr = strings.ReplaceAll(connStr, "?replication=database&", "?")
	connStr = strings.ReplaceAll(connStr, "?replication=database", "")
	return connStr
}

func parseSchema(connStr string) string {
	u, err := url.Parse(connStr)
	if err != nil {
		return "public"
	}
	if s := u.Query().Get("currentSchema"); s != "" {
		return s
	}
	if s := u.Query().Get("search_path"); s != "" {
		return s
	}
	return "public"
}
