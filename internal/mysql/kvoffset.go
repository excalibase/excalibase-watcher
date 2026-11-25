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
	jsInsufficientResources jetstream.ErrorCode = 10023 // too few peers right now: retry, never downgrade
	jsReplicasNotClustered  jetstream.ErrorCode = 10074 // a single server: one replica is all there is
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
// attach and nothing extra in the customer's database. A second key, the
// marker, shows that the watcher has saved an offset, so a lost offset is told
// apart from a first start; its value is the reset token last used.
type KVOffsetStore struct {
	bucket jetstream.KeyValue
	key    string
	// marked is whether the marker exists, and markerToken what it holds.
	marked      bool
	markerToken string
}

// NewKVOffsetStore opens the bucket, creating it on file storage when absent,
// retrying while NATS is unreachable. Creation asks for the given replicas
// and, on a cluster too small for them, takes one replica and says so.
func NewKVOffsetStore(ctx context.Context, js jetstream.JetStream, bucket, key string, replicas int) (*KVOffsetStore, error) {
	var lastErr error
	for attempt := 0; ; attempt++ {
		kv, err := openBucket(ctx, js, bucket, replicas)
		if err == nil {
			warnIfUnderReplicated(ctx, kv, bucket, replicas)
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

// warnIfUnderReplicated says so on every open when the bucket keeps fewer
// copies than configured, however it came to be (a past fallback, an older
// deployment).
func warnIfUnderReplicated(ctx context.Context, kv jetstream.KeyValue, bucket string, want int) {
	status, err := kv.Status(ctx)
	if err != nil {
		slog.Warn("mysql: could not read the offset bucket's replicas", "bucket", bucket, "error", err)
		return
	}
	if have := status.Config().Replicas; have < want {
		slog.Warn("mysql: the offset bucket has fewer replicas than configured; the offset is lost if its server is lost",
			"bucket", bucket, "have", have, "want", want)
	}
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
	if replicas > 1 && errors.As(err, &apiErr) && (apiErr.ErrorCode == jsReplicasNotClustered) {
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
	if s.marked && s.markerToken == record.ResetToken {
		return nil
	}
	// After the offset, so a crash between the two leaves an offset with no
	// marker, which the next Save repairs, and never a marker with no offset.
	if _, err := s.bucket.Put(ctx, s.key+seededSuffix, []byte(record.ResetToken)); err != nil {
		return err
	}
	s.marked, s.markerToken = true, record.ResetToken
	return nil
}

func (s *KVOffsetStore) Load() (OffsetRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
	defer cancel()
	marker, err := s.readMarker(ctx)
	if err != nil {
		return OffsetRecord{}, err
	}
	entry, err := s.bucket.Get(ctx, s.key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		if marker.exists {
			return OffsetRecord{ResetToken: marker.token}, ErrOffsetLost
		}
		return OffsetRecord{}, nil
	}
	if err != nil {
		return OffsetRecord{}, err
	}
	var record OffsetRecord
	if err := json.Unmarshal(entry.Value(), &record); err != nil {
		return OffsetRecord{}, fmt.Errorf("reading offset key %s: %w", s.key, err)
	}
	return record, nil
}

type markerState struct {
	exists bool
	token  string
}

// readMarker reads the marker and remembers what it found, so Save writes it
// when it is missing or out of date.
func (s *KVOffsetStore) readMarker(ctx context.Context) (markerState, error) {
	entry, err := s.bucket.Get(ctx, s.key+seededSuffix)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		s.marked, s.markerToken = false, ""
		return markerState{}, nil
	}
	if err != nil {
		return markerState{}, err
	}
	s.marked, s.markerToken = true, string(entry.Value())
	return markerState{exists: true, token: s.markerToken}, nil
}

var invalidKeyChars = regexp.MustCompile(`[^A-Za-z0-9_.=/-]`)

// OffsetKey names a watcher's key from the pieces that tell watchers apart in
// a shared bucket. The server's address is left out on purpose: after a
// failover the same GTID set must still be found.
func OffsetKey(subjectPrefix, schemaName string) string {
	return invalidKeyChars.ReplaceAllString(subjectPrefix+"."+schemaName, "_")
}
