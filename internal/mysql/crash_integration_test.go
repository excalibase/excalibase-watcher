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

// One INSERT of five rows is one binlog rows event. The stream only accepts
// two messages, so the watcher is stuck with rows 1-2 acked and 3-5 pending
// when it crashes. After restart every row must be in the stream exactly once.
func TestCrashMidRowsEventLosesNoRow(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
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
		MaxMsgs: 2, Discard: jetstream.DiscardNew,
	}
	if _, err := js.CreateStream(ctx, streamCfg); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	mysqlCfg := makeMySQLConfig(addr)
	mysqlCfg.Tables = []string{"cdc_test_users"}
	mysqlCfg.OffsetFile = filepath.Join(t.TempDir(), "binlog.offset")

	first := startWatcherProcess(t, mysqlCfg, natsCfg)
	time.Sleep(3 * time.Second)

	const rowCount = 5
	values := make([]string, rowCount)
	for i := range values {
		values[i] = fmt.Sprintf("('row-%d')", i+1)
	}
	execMySQL(t, addr, "INSERT INTO cdc_test_users (name) VALUES "+strings.Join(values, ", "))

	waitForStreamMsgs(t, js, natsCfg.StreamName, 2, 30*time.Second)
	time.Sleep(3 * time.Second) // let the offset saver run while rows 3-5 are stuck
	first.crash()

	streamCfg.MaxMsgs = -1
	if _, err := js.UpdateStream(ctx, streamCfg); err != nil {
		t.Fatalf("lift stream limit: %v", err)
	}

	second := startWatcherProcess(t, mysqlCfg, natsCfg)
	defer second.crash()
	waitForStreamMsgs(t, js, natsCfg.StreamName, rowCount, 60*time.Second)
	time.Sleep(2 * time.Second)

	names := streamedRowNames(t, js, natsCfg.StreamName)
	for i := 1; i <= rowCount; i++ {
		name := fmt.Sprintf("row-%d", i)
		if names[name] != 1 {
			t.Errorf("%s published %d times, want exactly once", name, names[name])
		}
	}
}
