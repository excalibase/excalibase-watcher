package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	kvTimeout = 10 * time.Second
	// JetStream's answers when the cluster cannot hold the requested replicas:
	// too few servers, or no cluster at all.
	jsInsufficientResources jetstream.ErrorCode = 10023
	jsReplicasNotClustered  jetstream.ErrorCode = 10074
	seededSuffix                                = ".seeded"
)

// kvOpenBackoff is the wait before each retry of opening the bucket: NATS may
// still be coming up when the watcher starts.
var kvOpenBackoff = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second}

// ErrOffsetLost means the watcher ran before (its marker is in the bucket) but
// its offset is gone. Starting from the server's current position would skip
// everything since, so the watcher refuses.
var ErrOffsetLost = errors.New("mysql: the saved offset is missing although this watcher has run before; " +
	"refusing to start from the server's current position, which would skip every change since. " +
	"If that is intended, set mysql.reset_offset (WATCHER_MYSQL_RESET_OFFSET) to a new token for one start")

// KVOffsetStore keeps the offset in a JetStream key-value bucket, one key per
// watcher. The watcher already needs JetStream, so there is no volume to
// attach and nothing extra in the customer's database. A second key marks
// that the watcher has saved an offset, so a lost offset is told apart from a
// first start.
type KVOffsetStore struct {
	bucket jetstream.KeyValue
	key    string
	seeded bool
}

// NewKVOffsetStore opens the bucket, creating it on file storage when absent,
// retrying while NATS is unreachable. Creation asks for the given replicas
// and, on a cluster too small for them, takes one replica and says so.
func NewKVOffsetStore(ctx context.Context, js jetstream.JetStream, bucket, key string, replicas int) (*KVOffsetStore, error) {
	var lastErr error
	for attempt := 0; ; attempt++ {
		kv, err := openBucket(ctx, js, bucket, replicas)
		if err == nil {
			return &KVOffsetStore{bucket: kv, key: key}, nil
		}
		lastErr = err
		if attempt >= len(kvOpenBackoff) {
			break
		}
		slog.Warn("mysql: opening the offset bucket failed, retrying", "bucket", bucket, "error", err, "in", kvOpenBackoff[attempt])
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("opening offset bucket %q: %w", bucket, ctx.Err())
		case <-time.After(kvOpenBackoff[attempt]):
		}
	}
	return nil, fmt.Errorf("opening offset bucket %q: %w", bucket, lastErr)
}

func openBucket(ctx context.Context, js jetstream.JetStream, bucket string, replicas int) (jetstream.KeyValue, error) {
	kv, err := js.KeyValue(ctx, bucket)
	if !errors.Is(err, jetstream.ErrBucketNotFound) {
		return kv, err
	}
	kv, err = createBucket(ctx, js, bucket, replicas)
	if errors.Is(err, jetstream.ErrBucketExists) {
		return js.KeyValue(ctx, bucket)
	}
	return kv, err
}

func createBucket(ctx context.Context, js jetstream.JetStream, bucket string, replicas int) (jetstream.KeyValue, error) {
	create := func(replicas int) (jetstream.KeyValue, error) {
		return js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:      bucket,
			Description: "excalibase-watcher MySQL resume offsets",
			Storage:     jetstream.FileStorage,
			Replicas:    replicas,
		})
	}
	kv, err := create(replicas)
	var apiErr *jetstream.APIError
	if replicas > 1 && errors.As(err, &apiErr) && (apiErr.ErrorCode == jsInsufficientResources || apiErr.ErrorCode == jsReplicasNotClustered) {
		slog.Warn("mysql: the NATS cluster is too small for the offset bucket's replicas; the offset is kept on ONE server and is lost if it is lost",
			"bucket", bucket, "wanted", replicas)
		return create(1)
	}
	return kv, err
}

func (s *KVOffsetStore) Save(record OffsetRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
	defer cancel()
	if _, err := s.bucket.Put(ctx, s.key, data); err != nil {
		return err
	}
	if s.seeded {
		return nil
	}
	// After the offset, so a crash between the two leaves an offset with no
	// marker, which is harmless, and never a marker with no offset.
	if _, err := s.bucket.Put(ctx, s.key+seededSuffix, []byte("1")); err != nil {
		return err
	}
	s.seeded = true
	return nil
}

func (s *KVOffsetStore) Load() (OffsetRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
	defer cancel()
	entry, err := s.bucket.Get(ctx, s.key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return OffsetRecord{}, s.missingKey(ctx)
	}
	if err != nil {
		return OffsetRecord{}, err
	}
	var record OffsetRecord
	if err := json.Unmarshal(entry.Value(), &record); err != nil {
		return OffsetRecord{}, fmt.Errorf("reading offset key %s: %w", s.key, err)
	}
	s.seeded = true
	return record, nil
}

// missingKey is nil on a first start and ErrOffsetLost when the marker shows
// the watcher saved an offset before.
func (s *KVOffsetStore) missingKey(ctx context.Context) error {
	_, err := s.bucket.Get(ctx, s.key+seededSuffix)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return ErrOffsetLost
}

var invalidKeyChars = regexp.MustCompile(`[^A-Za-z0-9_.=/-]`)

// OffsetKey names a watcher's key from the pieces that tell watchers apart in
// a shared bucket. The server's address is left out on purpose: after a
// failover the same GTID set must still be found.
func OffsetKey(subjectPrefix, schemaName string) string {
	return invalidKeyChars.ReplaceAllString(subjectPrefix+"."+schemaName, "_")
}
