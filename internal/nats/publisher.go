package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/excalibase/watcher-go/internal/metrics"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Publisher struct {
	cfg     config.NATSConfig
	service *cdc.Service
	conn    *nats.Conn
	js      jetstream.JetStream
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// lastAckedLSN is updated after every successful js.Publish ack.
	// Offset-persisting listeners read this to advance storage safely.
	lastAckedLSN atomic.Value // string
	onPublished  atomic.Pointer[func(cdc.Event)]

	// publishFn performs one publish attempt; tests substitute a fake.
	publishFn      func(ctx context.Context, subject string, data []byte) error
	retryBaseDelay time.Duration
}

const (
	defaultRetryBaseDelay = 500 * time.Millisecond
	maxRetryDelay         = 10 * time.Second
)

func NewPublisher(cfg config.NATSConfig, service *cdc.Service) *Publisher {
	p := &Publisher{
		cfg:            cfg,
		service:        service,
		retryBaseDelay: defaultRetryBaseDelay,
	}
	p.lastAckedLSN.Store("")
	return p
}

// LastAckedLSN returns the position of the most recently acknowledged event.
// Empty string if nothing has been published yet.
func (p *Publisher) LastAckedLSN() string {
	v, _ := p.lastAckedLSN.Load().(string)
	return v
}

// SetOnPublished registers a callback invoked after each successful publish.
// The callback runs on the publisher goroutine, so it must be fast and
// non-blocking (writing to a channel or atomic is fine). Safe to call after Start.
func (p *Publisher) SetOnPublished(fn func(cdc.Event)) {
	p.onPublished.Store(&fn)
}

// connectOptions turns the configured credential into dial options. A NATS
// server that scopes permissions per principal also scopes the reply inbox,
// so the prefix travels with the credential.
func connectOptions(cfg config.NATSConfig) []nats.Option {
	opts := []nats.Option{
		nats.ReconnectWait(2 * time.Second),
		nats.MaxReconnects(-1),
	}
	if cfg.Username != "" {
		opts = append(opts, nats.UserInfo(cfg.Username, cfg.Password))
	}
	if cfg.InboxPrefix != "" {
		opts = append(opts, nats.CustomInboxPrefix(cfg.InboxPrefix))
	}
	return opts
}

func (p *Publisher) Start(ctx context.Context) error {
	ctx, p.cancel = context.WithCancel(ctx)

	conn, err := nats.Connect(p.cfg.URL, connectOptions(p.cfg)...)
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}
	p.conn = conn

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return fmt.Errorf("creating JetStream: %w", err)
	}
	p.js = js
	p.publishFn = func(ctx context.Context, subject string, data []byte) error {
		_, err := js.Publish(ctx, subject, data)
		return err
	}

	// Create or update stream (idempotent)
	if err := p.ensureStream(ctx); err != nil {
		conn.Close()
		return err
	}

	// Subscribe to all CDC events and publish to NATS
	ch, unsub := p.service.SubscribeAll()
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer unsub()
		p.publishLoop(ctx, ch)
	}()

	return nil
}

func (p *Publisher) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
}

// ensureStream verifies the JetStream stream exists. The watcher NEVER
// creates or modifies the stream — it is a pure publisher.
//
// Infra provisions the stream once with a wildcard subject pattern. All
// tenants publish under that pattern via their own SubjectPrefix without
// any topology changes. Example:
//
//	# one-time infra setup
//	nats stream add CDC --subjects='cdc.>' --storage=file --retention=limits \
//	  --max-age=15m --discard=old --replicas=3 --defaults
//
//	# then each tenant watcher uses a unique subject_prefix that falls under
//	# the stream's wildcard:
//	tenant-a: subject_prefix=cdc.excalibase.tenant-a
//	tenant-b: subject_prefix=cdc.excalibase.tenant-b
//
// Publishing to cdc.excalibase.tenant-a.public.users is captured by cdc.>.
//
// This design prevents the multi-tenant clobbering bug where one watcher's
// CreateOrUpdateStream would overwrite another tenant's subject list.
func (p *Publisher) ensureStream(ctx context.Context) error {
	_, err := p.js.Stream(ctx, p.cfg.StreamName)
	if err == nil {
		slog.Debug("NATS stream verified",
			"name", p.cfg.StreamName,
			"subject_prefix", p.cfg.SubjectPrefix)
		return nil
	}
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("NATS stream %q does not exist — provision it before starting "+
			"the watcher (e.g. `nats stream add %s --subjects='cdc.>'`). The watcher is "+
			"a pure publisher and will not create or modify streams",
			p.cfg.StreamName, p.cfg.StreamName)
	}
	return fmt.Errorf("looking up NATS stream %q: %w", p.cfg.StreamName, err)
}

func (p *Publisher) publishLoop(ctx context.Context, ch <-chan cdc.Event) {
	for {
		select {
		case event, ok := <-ch:
			if !ok {
				return
			}
			if !shouldPublish(event.Type) {
				continue
			}
			if err := p.publish(ctx, event); err != nil && ctx.Err() == nil {
				slog.Warn("giving up on event", "type", event.Type.String(), "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// publish delivers one event to JetStream, retrying with backoff until the
// broker acks or ctx ends. Skipping a failed event would let the source
// position advance past it (see ackGate / offsetSaver) and lose it, so the
// publisher blocks instead: the database retains WAL/binlog meanwhile.
func (p *Publisher) publish(ctx context.Context, event cdc.Event) error {
	subject := buildSubject(p.cfg.SubjectPrefix, event.Schema, event.Table)

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling event: %w", err)
	}

	if err := p.publishWithRetry(ctx, subject, data, event.Type.String()); err != nil {
		return err
	}

	// Ack received from NATS. Record position and notify subscribers so they
	// can safely advance persisted offsets.
	metrics.IncNATSPublished(event.Type.String())
	if event.LSN != "" {
		p.lastAckedLSN.Store(event.LSN)
	}
	if fn := p.onPublished.Load(); fn != nil {
		(*fn)(event)
	}
	return nil
}

func (p *Publisher) publishWithRetry(ctx context.Context, subject string, data []byte, eventType string) error {
	delay := p.retryBaseDelay
	for {
		err := p.publishFn(ctx, subject, data)
		if err == nil {
			return nil
		}
		metrics.IncNATSError()
		slog.Warn("publish failed, retrying", "type", eventType, "delay", delay, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, maxRetryDelay)
	}
}

func buildSubject(prefix, schema, table string) string {
	if schema == "" {
		schema = "default"
	}
	if table == "" {
		table = "_ddl"
	}
	return prefix + "." + sanitizeSubjectToken(schema) + "." + sanitizeSubjectToken(table)
}

// sanitizeSubjectToken replaces NATS wildcard characters to prevent subject injection.
func sanitizeSubjectToken(s string) string {
	return strings.NewReplacer(
		".", "_",
		"*", "_",
		">", "_",
		" ", "_",
	).Replace(s)
}

func shouldPublish(t cdc.EventType) bool {
	return cdc.IsPublishable(t)
}
