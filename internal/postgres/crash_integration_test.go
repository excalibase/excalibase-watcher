//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	natspub "github.com/excalibase/watcher-go/internal/nats"
	"github.com/excalibase/watcher-go/internal/natstest"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
)

// watcherProcess is one watcher's CDC pipeline. stop without a graceful
// flush is what a crash leaves behind: the slot keeps the last confirmed
// position and everything buffered in memory is gone.
type watcherProcess struct {
	svc      *cdc.Service
	pub      *natspub.Publisher
	listener *Listener
}

func startWatcherProcess(t *testing.T, pgCfg config.PostgresConfig, natsCfg config.NATSConfig) *watcherProcess {
	t.Helper()
	svc := cdc.NewService()
	pub := natspub.NewPublisher(natsCfg, svc)
	listener, err := NewListener(pgCfg, svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	pub.SetOnPublished(listener.PublishedObserver())
	if err := pub.Start(context.Background()); err != nil {
		t.Fatalf("publisher Start: %v", err)
	}
	if err := listener.Start(context.Background()); err != nil {
		t.Fatalf("listener Start: %v", err)
	}
	return &watcherProcess{svc: svc, pub: pub, listener: listener}
}

func (w *watcherProcess) crash() {
	w.listener.Stop()
	w.pub.Stop()
	w.svc.Shutdown()
}

// interleavingWriters commit two-row transactions from several connections
// with a pause inside each, so transactions overlap and commit out of order.
type interleavingWriters struct {
	connStr   string
	committed sync.Map
	count     atomic.Int64
	stop      chan struct{}
	wg        sync.WaitGroup
}

func (w *interleavingWriters) start(t *testing.T, writers int) {
	w.stop = make(chan struct{})
	for writer := range writers {
		w.wg.Add(1)
		go w.run(t, writer)
	}
}

func (w *interleavingWriters) run(t *testing.T, writer int) {
	defer w.wg.Done()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, stripReplicationParam(w.connStr))
	if err != nil {
		t.Errorf("writer connect: %v", err)
		return
	}
	defer conn.Close(ctx)
	for txn := 0; ; txn++ {
		select {
		case <-w.stop:
			return
		default:
		}
		names := []string{fmt.Sprintf("w%d-t%d-a", writer, txn), fmt.Sprintf("w%d-t%d-b", writer, txn)}
		if err := w.commitPair(ctx, conn, names); err != nil {
			t.Errorf("writer %d: %v", writer, err)
			return
		}
		for _, name := range names {
			w.committed.Store(name, true)
			w.count.Add(1)
		}
	}
}

func (w *interleavingWriters) commitPair(ctx context.Context, conn *pgx.Conn, names []string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "INSERT INTO cdc_test_users (name) VALUES ($1)", names[0]); err != nil {
		return err
	}
	time.Sleep(time.Duration(rand.IntN(20)) * time.Millisecond)
	if _, err := tx.Exec(ctx, "INSERT INTO cdc_test_users (name) VALUES ($1)", names[1]); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (w *interleavingWriters) halt() {
	close(w.stop)
	w.wg.Wait()
}

// streamedNames reads every message in the stream and counts row names.
func streamedNames(t *testing.T, js jetstream.JetStream, stream string) map[string]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	consumer, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("ordered consumer: %v", err)
	}
	info, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	total := info.CachedInfo().State.Msgs
	names := make(map[string]int)
	for read := uint64(0); read < total; read++ {
		msg, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
		if err != nil {
			t.Fatalf("reading message %d of %d: %v", read+1, total, err)
		}
		var event cdc.Event
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			t.Fatalf("decode event: %v", err)
		}
		var row struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal([]byte(event.Data), &row); err != nil {
			t.Fatalf("decode row %q: %v", event.Data, err)
		}
		names[row.Name]++
	}
	return names
}

// Crash the watcher twice under concurrent, out-of-order commits: once while
// NATS is down (events buffered, unacked) and once while it is publishing.
// Every committed row must be in the stream exactly once afterwards.
func TestCrashUnderConcurrentWritesLosesNoCommittedRow(t *testing.T) {
	connStr, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	env := natstest.Start(t)
	natsCfg := config.NATSConfig{URL: env.URL, StreamName: "CDC_CRASH", SubjectPrefix: "cdc.proj"}
	js, err := jetstream.New(env.Connect(t))
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: natsCfg.StreamName, Subjects: []string{natsCfg.SubjectPrefix + ".>"},
		Storage: jetstream.FileStorage, Duplicates: 10 * time.Minute,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	pgCfg := makeConfig(connStr)

	writers := &interleavingWriters{connStr: connStr}
	first := startWatcherProcess(t, pgCfg, natsCfg)
	writers.start(t, 4)
	time.Sleep(statusInterval + 2*time.Second)

	env.Stop()
	time.Sleep(statusInterval + 2*time.Second)
	first.crash()
	env.Start(t)

	second := startWatcherProcess(t, pgCfg, natsCfg)
	time.Sleep(statusInterval + 2*time.Second)
	second.crash()

	third := startWatcherProcess(t, pgCfg, natsCfg)
	defer third.crash()
	time.Sleep(3 * time.Second)
	writers.halt()

	want := writers.count.Load()
	t.Logf("%d rows committed", want)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		stream, err := js.Stream(ctx, natsCfg.StreamName)
		if err == nil && int64(stream.CachedInfo().State.Msgs) >= want {
			break
		}
		time.Sleep(time.Second)
	}
	time.Sleep(2 * time.Second)

	names := streamedNames(t, js, natsCfg.StreamName)
	missing := 0
	writers.committed.Range(func(key, _ any) bool {
		if names[key.(string)] == 0 {
			missing++
			if missing <= 5 {
				t.Errorf("committed row %s never published", key)
			}
		}
		return true
	})
	for name, seen := range names {
		if seen > 1 {
			t.Errorf("row %s published %d times", name, seen)
		}
	}
	if missing > 0 {
		t.Fatalf("%d of %d committed rows lost", missing, want)
	}
}
