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
	onPublished  func(cdc.Event)
}

func NewPublisher(cfg config.NATSConfig, service *cdc.Service) *Publisher {
	p := &Publisher{
		cfg:     cfg,
		service: service,
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
// non-blocking (writing to a channel or atomic is fine).
func (p *Publisher) SetOnPublished(fn func(cdc.Event)) {
	p.onPublished = fn
}

func (p *Publisher) Start(ctx context.Context) error {
	ctx, p.cancel = context.WithCancel(ctx)

	conn, err := nats.Connect(p.cfg.URL,
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1),
	)
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
			if err := p.publish(ctx, event); err != nil {
				slog.Warn("failed to publish event",
					"type", event.Type.String(),
					"error", err,
				)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (p *Publisher) publish(ctx context.Context, event cdc.Event) error {
	subject := buildSubject(p.cfg.SubjectPrefix, event.Schema, event.Table)

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling event: %w", err)
	}

	if _, err := p.js.Publish(ctx, subject, data); err != nil {
		return err
	}

	// Ack received from NATS. Record position and notify subscribers so they
	// can safely advance persisted offsets.
	if event.LSN != "" {
		p.lastAckedLSN.Store(event.LSN)
	}
	if p.onPublished != nil {
		p.onPublished(event)
	}
	return nil
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
	switch t {
	case cdc.Insert, cdc.Update, cdc.Delete, cdc.DDL, cdc.Truncate:
		return true
	default:
		return false
	}
}
