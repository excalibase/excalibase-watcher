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
	"github.com/nats-io/nuid"
)

type Publisher struct {
	cfg     config.NATSConfig
	service *cdc.Service
	conn    atomic.Pointer[nats.Conn]
	js      jetstream.JetStream
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	streamVerified atomic.Bool
	stopping       atomic.Bool
	failed         chan error
	reconnect      backoff

	// lastAckedLSN is updated after every successful js.Publish ack.
	// Offset-persisting listeners read this to advance storage safely.
	lastAckedLSN atomic.Value // string
	onPublished  atomic.Pointer[func(cdc.Event)]

	// publishFn performs one publish attempt; tests substitute a fake.
	publishFn      func(ctx context.Context, subject string, data []byte, msgID string) error
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
		failed:         make(chan error, 1),
		reconnect:      defaultBackoff(),
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

// Ready reports whether events can be published right now: connected, with
// the stream verified.
func (p *Publisher) Ready() bool {
	conn := p.conn.Load()
	return conn != nil && conn.IsConnected() && p.streamVerified.Load()
}

// Failed delivers the error that makes the publisher unable to continue: the
// stream is missing, or the connection closed for good.
func (p *Publisher) Failed() <-chan error {
	return p.failed
}

func (p *Publisher) fail(err error) {
	select {
	case p.failed <- err:
	default:
	}
}

// Start returns once the connection is being attempted, not once it is up:
// NATS being unreachable at boot is transient, and the
// events meanwhile wait on the bus subscription taken here.
func (p *Publisher) Start(ctx context.Context) error {
	ctx, p.cancel = context.WithCancel(ctx)

	conn, err := nats.Connect(p.cfg.URL, p.connectOptions()...)
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}
	p.conn.Store(conn)

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return fmt.Errorf("creating JetStream: %w", err)
	}
	p.js = js
	p.publishFn = func(ctx context.Context, subject string, data []byte, msgID string) error {
		_, err := js.Publish(ctx, subject, data, jetstream.WithMsgID(msgID))
		return err
	}

	if err := p.verifyStreamIfConnected(ctx, conn); err != nil {
		conn.Close()
		return err
	}

	ch, unsub := p.service.SubscribeAll()
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer unsub()
		if p.streamVerified.Load() {
			p.publishLoop(ctx, ch)
			return
		}
		if err := p.awaitStream(ctx, conn); err != nil {
			if ctx.Err() == nil {
				p.fail(err)
			}
			return
		}
		p.publishLoop(ctx, ch)
	}()

	return nil
}

func (p *Publisher) Stop() {
	p.stopping.Store(true)
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	if conn := p.conn.Load(); conn != nil {
		conn.Close()
	}
}

// verifyStreamIfConnected fails Start on a missing stream when the first
// login was accepted, before any database listener starts. Other lookup
// errors are left to awaitStream to retry.
func (p *Publisher) verifyStreamIfConnected(ctx context.Context, conn *nats.Conn) error {
	if !conn.IsConnected() {
		return nil
	}
	err := p.ensureStream(ctx)
	if err == nil {
		p.streamVerified.Store(true)
	}
	if errors.Is(err, errStreamMissing) {
		return err
	}
	return nil
}

// awaitStream verifies the stream once connected. A missing stream is fatal;
// any other lookup error is retried, since the connection can drop again
// between connecting and asking.
func (p *Publisher) awaitStream(ctx context.Context, conn *nats.Conn) error {
	for attempt := 1; ; attempt++ {
		if err := p.waitConnected(ctx, conn); err != nil {
			return err
		}
		err := p.ensureStream(ctx)
		if err == nil {
			p.streamVerified.Store(true)
			return nil
		}
		if errors.Is(err, errStreamMissing) || ctx.Err() != nil {
			return err
		}
		delay := p.reconnect.delay(attempt)
		slog.Warn("verifying NATS stream failed, retrying", "retry_in", delay, "error", err)
		if err := sleepContext(ctx, delay); err != nil {
			return err
		}
	}
}

var errStreamMissing = errors.New("NATS stream does not exist")

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
		return fmt.Errorf("%w: %q — provision it before starting "+
			"the watcher (e.g. `nats stream add %s --subjects='cdc.>'`). The watcher is "+
			"a pure publisher and will not create or modify streams",
			errStreamMissing, p.cfg.StreamName, p.cfg.StreamName)
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
			if !p.deliver(ctx, event) {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// deliver publishes one event and reports whether the loop may continue. A
// closed connection stops it: skipping ahead would let later acks confirm the
// source position past this event.
func (p *Publisher) deliver(ctx context.Context, event cdc.Event) bool {
	if !shouldPublish(event.Type) {
		return true
	}
	err := p.publish(ctx, event)
	if errors.Is(err, errConnectionClosed) {
		return false
	}
	if err != nil && ctx.Err() == nil {
		slog.Warn("giving up on event", "type", event.Type.String(), "error", err)
	}
	return true
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

	if err := p.publishWithRetry(ctx, subject, data, p.messageID(event), event.Type.String()); err != nil {
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

// messageID is scoped by subject prefix because every tenant publishes into
// one stream. Events with no source position (snapshot rows) cannot be
// recognised when re-read, so they only dedupe their own retries.
func (p *Publisher) messageID(event cdc.Event) string {
	if event.SourceID == "" {
		return nuid.Next()
	}
	return p.cfg.SubjectPrefix + "/" + event.SourceID
}

// publishWithRetry sends every attempt with the same message id: an attempt
// whose ack was lost may still have been stored, and the stream drops the
// repeat within its duplicate window.
func (p *Publisher) publishWithRetry(ctx context.Context, subject string, data []byte, msgID, eventType string) error {
	delay := p.retryBaseDelay
	for {
		err := p.publishFn(ctx, subject, data, msgID)
		if err == nil {
			return nil
		}
		metrics.IncNATSError()
		if p.connectionLost() {
			// The connection layer logs its own retries; waiting here keeps
			// one line per attempt instead of one per attempt per event.
			if err := p.waitConnected(ctx, p.conn.Load()); err != nil {
				return err
			}
			continue
		}
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
