package mysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// OffsetRecord is what a watcher keeps between runs: the GTID set of the last
// transaction that was published and acknowledged (empty means nothing yet),
// and the last reset token it has used.
type OffsetRecord struct {
	GTIDSet    string `json:"gtid_set"`
	ResetToken string `json:"reset_token,omitempty"`
	// File is retired: an offset written before resume by GTID.
	File string `json:"file,omitempty"`
}

type OffsetStore interface {
	Save(record OffsetRecord) error
	Load() (OffsetRecord, error)
}

// ErrLegacyOffset is an offset written before resume by GTID: a binlog file
// and position, which means nothing on another replica.
var ErrLegacyOffset = errors.New("mysql: the saved offset is a binlog file and position from before resume by GTID; " +
	"set mysql.reset_offset (WATCHER_MYSQL_RESET_OFFSET) to a new token for one start to begin from the server's current position")

// usable refuses a record the watcher cannot resume from.
func (r OffsetRecord) usable() error {
	if r.GTIDSet == "" && r.File != "" {
		return ErrLegacyOffset
	}
	return nil
}

// applyResetToken discards the saved offset when the token is one this
// watcher has not used yet, and remembers the token so a restart that still
// carries it keeps the offset.
func applyResetToken(saved OffsetRecord, token string) (OffsetRecord, bool) {
	if token == "" || saved.ResetToken == token {
		return saved, false
	}
	return OffsetRecord{ResetToken: token}, true
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

func (s *FileOffsetStore) Save(record OffsetRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmpPath, s.path)
}

func (s *FileOffsetStore) Load() (OffsetRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return OffsetRecord{}, nil
	}
	if err != nil {
		return OffsetRecord{}, err
	}
	var record OffsetRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return OffsetRecord{}, fmt.Errorf("reading offset file %s: %w", s.path, err)
	}
	return record, nil
}
