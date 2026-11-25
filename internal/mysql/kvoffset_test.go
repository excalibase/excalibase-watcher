package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// fakeKV is a bucket in memory.
type fakeKV struct {
	jetstream.KeyValue
	data map[string][]byte
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

func newFakeJS() *fakeJS { return &fakeJS{bucket: &fakeKV{data: map[string][]byte{}}} }

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
	js.createErrs = []error{&jetstream.APIError{ErrorCode: jsInsufficientResources, Description: "no suitable peers"}}
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
