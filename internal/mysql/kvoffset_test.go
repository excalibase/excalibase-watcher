package mysql

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// fakeKV is a bucket in memory.
type fakeKV struct {
	jetstream.KeyValue
	data     map[string][]byte
	putErrs  map[string]error // Put to these keys fails
	replicas int              // what Status reports
}

type fakeStatus struct {
	jetstream.KeyValueStatus
	replicas int
}

func (s fakeStatus) Config() jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{Replicas: s.replicas}
}

func (f *fakeKV) Status(context.Context) (jetstream.KeyValueStatus, error) {
	return fakeStatus{replicas: f.replicas}, nil
}

type fakeEntry struct {
	jetstream.KeyValueEntry
	value []byte
}

func (e fakeEntry) Value() []byte { return e.value }

func (f *fakeKV) Get(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
	value, ok := f.data[key]
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return fakeEntry{value: value}, nil
}

func (f *fakeKV) Put(_ context.Context, key string, value []byte) (uint64, error) {
	if err := f.putErrs[key]; err != nil {
		return 0, err
	}
	f.data[key] = value
	return 1, nil
}

// fakeJS opens or creates buckets as the test scripts it.
type fakeJS struct {
	jetstream.JetStream
	bucket       *fakeKV
	openFailures int // KeyValue calls that fail before one finds the bucket
	exists       bool
	created      []int // Replicas of each CreateKeyValue call
	createErrs   []error
}

func (f *fakeJS) KeyValue(context.Context, string) (jetstream.KeyValue, error) {
	if f.openFailures > 0 {
		f.openFailures--
		return nil, errors.New("nats: connection not ready")
	}
	if !f.exists {
		return nil, jetstream.ErrBucketNotFound
	}
	return f.bucket, nil
}

func (f *fakeJS) CreateKeyValue(_ context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	f.created = append(f.created, cfg.Replicas)
	if len(f.createErrs) > 0 {
		err := f.createErrs[0]
		f.createErrs = f.createErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	f.exists = true
	return f.bucket, nil
}

func newFakeJS() *fakeJS {
	return &fakeJS{bucket: &fakeKV{data: map[string][]byte{}, putErrs: map[string]error{}, replicas: 3}}
}

func fastRetries(t *testing.T) {
	t.Helper()
	saved := kvOpenBackoff
	kvOpenBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { kvOpenBackoff = saved })
}

func TestKVStoreRoundTripsTheRecord(t *testing.T) {
	store, err := NewKVOffsetStore(context.Background(), newFakeJS(), "B", "k", 3)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := store.Load(); err != nil || got != (OffsetRecord{}) {
		t.Fatalf("empty Load = %+v, %v", got, err)
	}
	want := OffsetRecord{GTIDSet: serverA + ":1-5", ResetToken: "t1"}
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Load(); err != nil || got != want {
		t.Fatalf("Load = %+v, %v", got, err)
	}
}

// A key that was there and is gone must not read as "first start": that would
// silently skip every change since.
func TestKVStoreRefusesALostKeyThatWasSeededBefore(t *testing.T) {
	js := newFakeJS()
	store, _ := NewKVOffsetStore(context.Background(), js, "B", "k", 3)
	mustDo(t, store.Save(OffsetRecord{GTIDSet: serverA + ":1-5"}))
	delete(js.bucket.data, "k")

	if _, err := store.Load(); !errors.Is(err, ErrOffsetLost) {
		t.Fatalf("Load = %v, want ErrOffsetLost", err)
	}
	// A restarted process has no memory of the Save, only the marker.
	again, _ := NewKVOffsetStore(context.Background(), js, "B", "k", 3)
	if _, err := again.Load(); !errors.Is(err, ErrOffsetLost) {
		t.Fatalf("Load after restart = %v, want ErrOffsetLost", err)
	}
	// A different watcher's key in the same bucket is unaffected.
	other, _ := NewKVOffsetStore(context.Background(), js, "B", "other", 3)
	if got, err := other.Load(); err != nil || got != (OffsetRecord{}) {
		t.Fatalf("other key Load = %+v, %v", got, err)
	}
}

func TestKVBucketFallsBackToOneReplicaOnASingleNodeCluster(t *testing.T) {
	js := newFakeJS()
	js.createErrs = []error{&jetstream.APIError{ErrorCode: jsReplicasNotClustered, Description: "non-clustered"}}
	if _, err := NewKVOffsetStore(context.Background(), js, "B", "k", 3); err != nil {
		t.Fatal(err)
	}
	if len(js.created) != 2 || js.created[0] != 3 || js.created[1] != 1 {
		t.Fatalf("created with replicas %v, want [3 1]", js.created)
	}
}

func TestKVBucketDoesNotHideOtherCreateErrors(t *testing.T) {
	fastRetries(t)
	js := newFakeJS()
	js.createErrs = []error{errors.New("permissions violation"), errors.New("permissions violation"),
		errors.New("permissions violation"), errors.New("permissions violation")}
	if _, err := NewKVOffsetStore(context.Background(), js, "B", "k", 3); err == nil {
		t.Fatal("expected an error")
	}
	for _, replicas := range js.created {
		if replicas != 3 {
			t.Fatalf("a non-capacity error must not drop replicas: %v", js.created)
		}
	}
}

func TestKVOpenRetriesWhileNATSComesUp(t *testing.T) {
	fastRetries(t)
	js := newFakeJS()
	js.exists = true
	js.openFailures = 2
	if _, err := NewKVOffsetStore(context.Background(), js, "B", "k", 3); err != nil {
		t.Fatalf("open did not ride out a slow NATS: %v", err)
	}
	js = newFakeJS()
	js.exists = true
	js.openFailures = 100
	if _, err := NewKVOffsetStore(context.Background(), js, "B", "k", 3); err == nil {
		t.Fatal("open must give up after its retries")
	}
}

func TestKVOpenStopsRetryingWhenCancelled(t *testing.T) {
	fastRetries(t)
	js := newFakeJS()
	js.openFailures = 100
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewKVOffsetStore(ctx, js, "B", "k", 3); err == nil {
		t.Fatal("expected an error")
	}
}

func TestKVBucketWithOneReplicaDoesNotRetry(t *testing.T) {
	js := newFakeJS()
	js.createErrs = []error{&jetstream.APIError{ErrorCode: jsInsufficientResources}}
	if _, err := createBucket(context.Background(), js, "B", 1); err == nil {
		t.Fatal("expected the capacity error to be returned")
	}
	if len(js.created) != 1 {
		t.Fatalf("created %v, want a single attempt", js.created)
	}
}

func TestKVBucketKeepsItsReplicasOnOtherAPIErrors(t *testing.T) {
	fastRetries(t)
	js := newFakeJS()
	other := &jetstream.APIError{ErrorCode: 10077, Description: "something else"}
	js.createErrs = []error{other, other, other, other}
	if _, err := NewKVOffsetStore(context.Background(), js, "B", "k", 3); err == nil {
		t.Fatal("expected an error")
	}
	for _, replicas := range js.created {
		if replicas != 3 {
			t.Fatalf("a different API error dropped the replicas: %v", js.created)
		}
	}
}

func TestKVBucketFallsBackOnANonClusteredServer(t *testing.T) {
	js := newFakeJS()
	js.createErrs = []error{&jetstream.APIError{ErrorCode: jsReplicasNotClustered, Description: "replicas > 1 not supported in non-clustered mode"}}
	if _, err := NewKVOffsetStore(context.Background(), js, "B", "k", 3); err != nil {
		t.Fatal(err)
	}
	if len(js.created) != 2 || js.created[1] != 1 {
		t.Fatalf("created with replicas %v, want [3 1]", js.created)
	}
}

// A cluster that has too few peers right now (a node restarting) is not a
// single-node deployment: the open must retry or fail, never quietly keep the
// offset on one server.
func TestKVBucketDoesNotDropReplicasWhenPeersAreMissing(t *testing.T) {
	fastRetries(t)
	js := newFakeJS()
	insufficient := &jetstream.APIError{ErrorCode: jsInsufficientResources, Description: "no suitable peers"}
	js.createErrs = []error{insufficient, insufficient, insufficient, insufficient}
	if _, err := NewKVOffsetStore(context.Background(), js, "B", "k", 3); err == nil {
		t.Fatal("expected the open to fail")
	}
	for _, replicas := range js.created {
		if replicas != 3 {
			t.Fatalf("replicas dropped to %v on missing peers", js.created)
		}
	}
}

func TestKVOpenRidesOutMissingPeers(t *testing.T) {
	fastRetries(t)
	js := newFakeJS()
	js.createErrs = []error{&jetstream.APIError{ErrorCode: jsInsufficientResources}}
	if _, err := NewKVOffsetStore(context.Background(), js, "B", "k", 3); err != nil {
		t.Fatalf("a node coming back should be retried: %v", err)
	}
	if js.created[len(js.created)-1] != 3 {
		t.Fatalf("created %v", js.created)
	}
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

func TestKVOpenWarnsWhenTheBucketHasFewerReplicasThanWanted(t *testing.T) {
	js := newFakeJS()
	js.exists = true
	js.bucket.replicas = 1
	logs := captureLogs(t)
	if _, err := NewKVOffsetStore(context.Background(), js, "B", "k", 3); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "fewer replicas") {
		t.Fatalf("no warning for an under-replicated bucket: %q", logs.String())
	}

	js.bucket.replicas = 3
	logs.Reset()
	if _, err := NewKVOffsetStore(context.Background(), js, "B", "k", 3); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "fewer replicas") {
		t.Fatalf("warned although the bucket has the replicas: %q", logs.String())
	}
}

// The marker carries the token the last reset used, so a lost offset can tell a
// stale token from a new one.
func TestKVLostKeyReportsTheTokenTheMarkerHolds(t *testing.T) {
	js := newFakeJS()
	store, _ := NewKVOffsetStore(context.Background(), js, "B", "k", 3)
	mustDo(t, store.Save(OffsetRecord{GTIDSet: serverA + ":1-5", ResetToken: "t1"}))
	delete(js.bucket.data, "k")
	rec, err := store.Load()
	if !errors.Is(err, ErrOffsetLost) || rec.ResetToken != "t1" {
		t.Fatalf("Load = %+v, %v; want ErrOffsetLost with token t1", rec, err)
	}
	// A reset moves the marker to the new token.
	mustDo(t, store.Save(OffsetRecord{ResetToken: "t2"}))
	delete(js.bucket.data, "k")
	if rec, _ := store.Load(); rec.ResetToken != "t2" {
		t.Fatalf("marker token after a reset = %q, want t2", rec.ResetToken)
	}
}

// A crash between the two writes of the first Save leaves an offset without a
// marker. Loading it must not mark the watcher as seeded, or the marker would
// never be written.
func TestKVLoadOfAnOffsetWithoutAMarkerWritesTheMarkerOnNextSave(t *testing.T) {
	js := newFakeJS()
	js.bucket.data["k"] = []byte(`{"gtid_set":"` + serverA + `:1-5"}`)
	store, _ := NewKVOffsetStore(context.Background(), js, "B", "k", 3)
	if _, err := store.Load(); err != nil {
		t.Fatal(err)
	}
	mustDo(t, store.Save(OffsetRecord{GTIDSet: serverA + ":1-6"}))
	if _, ok := js.bucket.data["k"+seededSuffix]; !ok {
		t.Fatal("the marker was never written")
	}
}

// The offset goes first: if the marker write fails, what is left is an offset
// with no marker, never a marker with no offset.
func TestKVSaveWritesTheOffsetBeforeTheMarker(t *testing.T) {
	js := newFakeJS()
	js.bucket.putErrs["k"+seededSuffix] = errors.New("nats: timeout")
	store, _ := NewKVOffsetStore(context.Background(), js, "B", "k", 3)
	if err := store.Save(OffsetRecord{GTIDSet: serverA + ":1-5"}); err == nil {
		t.Fatal("expected the marker failure to surface")
	}
	if _, ok := js.bucket.data["k"]; !ok {
		t.Fatal("the offset was not written before the marker failed")
	}
	fresh, _ := NewKVOffsetStore(context.Background(), js, "B", "k", 3)
	if rec, err := fresh.Load(); err != nil || rec.GTIDSet == "" {
		t.Fatalf("Load after the half save = %+v, %v", rec, err)
	}
}
