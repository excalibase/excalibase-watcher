//go:build integration

package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	natspub "github.com/excalibase/watcher-go/internal/nats"
	"github.com/excalibase/watcher-go/internal/natstest"
	"github.com/nats-io/nats.go/jetstream"
)

// watcherProcess is one watcher's MySQL CDC pipeline. stop without draining
// is what a crash leaves behind: the offset file keeps the last persisted
// position and everything buffered in memory is gone.
type watcherProcess struct {
	svc      *cdc.Service
	pub      *natspub.Publisher
	listener *Listener
}

func startWatcherProcess(t *testing.T, mysqlCfg config.MySQLConfig, natsCfg config.NATSConfig) *watcherProcess {
	t.Helper()
	svc := cdc.NewService()
	pub := natspub.NewPublisher(natsCfg, svc)
	listener, err := NewListener(mysqlCfg, svc)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	pub.AddOnPublished(listener.PublishedObserver())
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

func streamedRowNames(t *testing.T, js jetstream.JetStream, stream string) map[string]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	info, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	consumer, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("ordered consumer: %v", err)
	}
	names := make(map[string]int)
	for read := uint64(0); read < info.CachedInfo().State.Msgs; read++ {
		msg, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
		if err != nil {
			t.Fatalf("reading message %d: %v", read+1, err)
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

func waitForStreamMsgs(t *testing.T, js jetstream.JetStream, stream string, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info, err := js.Stream(context.Background(), stream)
		if err == nil && info.CachedInfo().State.Msgs >= want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func insertRows(t *testing.T, addr, prefix string, count int) []string {
	t.Helper()
	names := make([]string, count)
	values := make([]string, count)
	for i := range names {
		names[i] = fmt.Sprintf("%s-%d", prefix, i+1)
		values[i] = fmt.Sprintf("('%s')", names[i])
	}
	execMySQL(t, addr, "INSERT INTO cdc_test_users (name) VALUES "+strings.Join(values, ", "))
	return names
}

// Two transactions of three rows each, every transaction one multi-row
// INSERT. The stream accepts four messages, so the watcher is stuck in the
// middle of the second transaction's rows event when it crashes. After the
// restart every row must be in the stream exactly once: none skipped, and
// rows already published deduplicated by a message id that is the same on
// the replay and different for every row.
func crashMidTransactionLosesNoRow(t *testing.T, mysqlFlags ...string) {
	addr, _, cleanup := setupMySQLWithFlags(t, mysqlFlags...)
	defer cleanup()
	ctx := context.Background()

	env := natstest.Start(t)
	natsCfg := config.NATSConfig{URL: env.URL, StreamName: "CDC_MYSQL_CRASH", SubjectPrefix: "cdc.proj"}
	js, err := jetstream.New(env.Connect(t))
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	streamCfg := jetstream.StreamConfig{
		Name: natsCfg.StreamName, Subjects: []string{natsCfg.SubjectPrefix + ".>"},
		Storage: jetstream.FileStorage, Duplicates: 10 * time.Minute,
		MaxMsgs: 4, Discard: jetstream.DiscardNew,
	}
	if _, err := js.CreateStream(ctx, streamCfg); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	mysqlCfg := makeMySQLConfig(addr)
	mysqlCfg.Tables = []string{"cdc_test_users"}
	mysqlCfg.OffsetFile = filepath.Join(t.TempDir(), "binlog.offset")

	first := startWatcherProcess(t, mysqlCfg, natsCfg)
	time.Sleep(3 * time.Second)

	names := insertRows(t, addr, "a", 3)
	names = append(names, insertRows(t, addr, "b", 3)...)

	waitForStreamMsgs(t, js, natsCfg.StreamName, 4, 30*time.Second)
	time.Sleep(3 * time.Second) // let the offset saver run while b-2 and b-3 are stuck
	first.crash()

	streamCfg.MaxMsgs = -1
	if _, err := js.UpdateStream(ctx, streamCfg); err != nil {
		t.Fatalf("lift stream limit: %v", err)
	}

	second := startWatcherProcess(t, mysqlCfg, natsCfg)
	defer second.crash()
	waitForStreamMsgs(t, js, natsCfg.StreamName, uint64(len(names)), 60*time.Second)
	time.Sleep(2 * time.Second)

	streamed := streamedRowNames(t, js, natsCfg.StreamName)
	for _, name := range names {
		if streamed[name] != 1 {
			t.Errorf("%s published %d times, want exactly once", name, streamed[name])
		}
	}
}

func TestCrashMidRowsEventLosesNoRow(t *testing.T) {
	crashMidTransactionLosesNoRow(t)
}

// Events inside a compressed transaction payload carry binlog position 0, so
// a position-based message id would be the same for the first row of every
// compressed transaction and the stream would drop all but one of them.
func TestCompressedTransactionsLoseNoRow(t *testing.T) {
	crashMidTransactionLosesNoRow(t, "--binlog-transaction-compression=ON")
}
