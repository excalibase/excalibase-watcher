package mysql

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const kvTimeout = 10 * time.Second

// KVOffsetStore keeps the offset in a JetStream key-value bucket, one key per
// watcher. The watcher already needs JetStream, so there is no volume to
// attach and nothing extra in the customer's database.
type KVOffsetStore struct {
	bucket jetstream.KeyValue
	key    string
}

// NewKVOffsetStore opens the bucket, creating it on file storage when absent.
// The creation is idempotent, so watchers that start together are safe.
func NewKVOffsetStore(ctx context.Context, js jetstream.JetStream, bucket, key string) (*KVOffsetStore, error) {
	kv, err := js.KeyValue(ctx, bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:      bucket,
			Description: "excalibase-watcher MySQL resume offsets",
			Storage:     jetstream.FileStorage,
		})
		if errors.Is(err, jetstream.ErrBucketExists) {
			kv, err = js.KeyValue(ctx, bucket)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("opening offset bucket %q: %w", bucket, err)
	}
	return &KVOffsetStore{bucket: kv, key: key}, nil
}

func (s *KVOffsetStore) Save(gtidSet string) error {
	ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
	defer cancel()
	_, err := s.bucket.PutString(ctx, s.key, gtidSet)
	return err
}

func (s *KVOffsetStore) Load() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
	defer cancel()
	entry, err := s.bucket.Get(ctx, s.key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return string(entry.Value()), nil
}

func (s *KVOffsetStore) Clear() error {
	ctx, cancel := context.WithTimeout(context.Background(), kvTimeout)
	defer cancel()
	err := s.bucket.Purge(ctx, s.key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	return err
}

var invalidKeyChars = regexp.MustCompile(`[^A-Za-z0-9_.=/-]`)

// OffsetKey names a watcher's key from the pieces that tell watchers apart in
// a shared bucket. The server's address is left out on purpose: after a
// failover the same GTID set must still be found.
func OffsetKey(subjectPrefix, schemaName string) string {
	return invalidKeyChars.ReplaceAllString(subjectPrefix+"."+schemaName, "_")
}
