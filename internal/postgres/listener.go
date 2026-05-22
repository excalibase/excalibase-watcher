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
	"github.com/excalibase/watcher-go/internal/schema"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	heartbeatInterval = 30 * time.Second
	statusInterval    = 10 * time.Second
	maxReconnectDelay = 30 * time.Second
)

type Listener struct {
	cfg         config.PostgresConfig
	service     *cdc.Service
	parser      *Parser
	schemaStore schema.HistoryStore

	conn    *pgconn.PgConn
	running atomic.Bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
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

	parser := NewParser(tableFilter, cfg.CaptureDDL, store)
	parser.SetEventHandler(func(e cdc.Event) {
		service.HandleEvent(e)
	})

	return &Listener{
		cfg:         cfg,
		service:     service,
		parser:      parser,
		schemaStore: store,
	}, nil
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

	// Start replication in background
	l.running.Store(true)
	l.wg.Add(1)
	go l.listenLoop(ctx)

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

	var currentLSN pglogrepl.LSN
	l.parser.SetLSNProvider(func() string { return currentLSN.String() })

	lastStatus := time.Now()
	lastMessage := time.Now()

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

		if err := l.handleMessage(ctx, conn, rawMsg, &currentLSN, &lastStatus, &lastMessage); err != nil {
			return err
		}

		if err := l.maintainHeartbeatAndStatus(ctx, conn, currentLSN, &lastStatus, &lastMessage); err != nil {
			return err
		}
	}

	return nil
}

func (l *Listener) sendStatus(ctx context.Context, conn *pgconn.PgConn, lsn pglogrepl.LSN) error {
	err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lsn + 1, // Must add 1 for ack
		})
	if err != nil {
		return fmt.Errorf("send status: %w", err)
	}
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
	currentLSN *pglogrepl.LSN, lastStatus, lastMessage *time.Time) error {
	copyData, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		slog.Debug("unexpected message type", "msg", fmt.Sprintf("%T", rawMsg))
		return nil
	}
	switch copyData.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return l.handleKeepalive(ctx, conn, copyData.Data[1:], *currentLSN, lastStatus)
	case pglogrepl.XLogDataByteID:
		return l.handleXLogData(copyData.Data[1:], currentLSN, lastMessage)
	}
	return nil
}

func (l *Listener) handleKeepalive(ctx context.Context, conn *pgconn.PgConn, data []byte,
	currentLSN pglogrepl.LSN, lastStatus *time.Time) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("parse keepalive: %w", err)
	}
	if !pkm.ReplyRequested {
		return nil
	}
	if err := l.sendStatus(ctx, conn, currentLSN); err != nil {
		return err
	}
	*lastStatus = time.Now()
	return nil
}

func (l *Listener) handleXLogData(data []byte, currentLSN *pglogrepl.LSN, lastMessage *time.Time) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("parse xlog data: %w", err)
	}
	*currentLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
	*lastMessage = time.Now()

	if event := l.parser.Parse(xld.WALData, currentLSN.String()); event != nil {
		l.service.HandleEvent(*event)
	}
	return nil
}

func (l *Listener) maintainHeartbeatAndStatus(ctx context.Context, conn *pgconn.PgConn,
	currentLSN pglogrepl.LSN, lastStatus, lastMessage *time.Time) error {
	if time.Since(*lastStatus) >= statusInterval {
		if err := l.sendStatus(ctx, conn, currentLSN); err != nil {
			return err
		}
		*lastStatus = time.Now()
	}
	if time.Since(*lastMessage) >= heartbeatInterval {
		e := cdc.NewEvent(cdc.Heartbeat, "", "", "", "HEARTBEAT", currentLSN.String())
		l.service.HandleEvent(e)
		*lastMessage = time.Now()
		if err := l.sendStatus(ctx, conn, currentLSN); err != nil {
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
	connStr := stripReplicationParam(cfg.URL)
	if cfg.Username != "" {
		// Inject credentials if not in URL
		u, err := url.Parse(connStr)
		if err == nil && u.User == nil {
			u.User = url.UserPassword(cfg.Username, cfg.Password)
			connStr = u.String()
		}
	}
	return pgx.Connect(ctx, connStr)
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
