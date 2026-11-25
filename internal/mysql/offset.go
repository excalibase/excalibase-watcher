package mysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// OffsetStore keeps the GTID set of the last transaction that was published
// and acknowledged. An empty set means nothing has been saved.
type OffsetStore interface {
	Save(gtidSet string) error
	Load() (string, error)
	Clear() error
}

// ErrLegacyOffset is an offset written before resume by GTID: a binlog file
// and position, which means nothing on another replica.
var ErrLegacyOffset = errors.New("mysql: the saved offset is a binlog file and position from before resume by GTID; " +
	"set mysql.reset_offset (WATCHER_MYSQL_RESET_OFFSET=true) for one start to begin from the server's current position")

type savedOffset struct {
	GTIDSet string `json:"gtid_set"`
	// Retired fields, read only to tell a legacy offset from an empty one.
	File string `json:"file,omitempty"`
}

// FileOffsetStore writes the offset to a file; on Kubernetes the file must
// sit on a volume that outlives the pod.
type FileOffsetStore struct {
	path string
	mu   sync.Mutex
}

func NewFileOffsetStore(path string) *FileOffsetStore {
	return &FileOffsetStore{path: path}
}

func (s *FileOffsetStore) Save(gtidSet string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(savedOffset{GTIDSet: gtidSet})
	if err != nil {
		return err
	}
	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmpPath, s.path)
}

func (s *FileOffsetStore) Load() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var saved savedOffset
	if err := json.Unmarshal(data, &saved); err != nil {
		return "", fmt.Errorf("reading offset file %s: %w", s.path, err)
	}
	if saved.GTIDSet == "" && saved.File != "" {
		return "", ErrLegacyOffset
	}
	return saved.GTIDSet, nil
}

func (s *FileOffsetStore) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
