package mysql

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type BinlogPosition struct {
	File     string `json:"file"`
	Position uint32 `json:"position"`
}

func (p BinlogPosition) String() string {
	return fmt.Sprintf("%s:%d", p.File, p.Position)
}

type OffsetStore interface {
	Save(pos BinlogPosition) error
	Load() (*BinlogPosition, error)
}

type FileOffsetStore struct {
	path string
	mu   sync.Mutex
}

func NewFileOffsetStore(path string) *FileOffsetStore {
	return &FileOffsetStore{path: path}
}

func (s *FileOffsetStore) Save(pos BinlogPosition) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(pos)
	if err != nil {
		return err
	}

	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmpPath, s.path)
}

func (s *FileOffsetStore) Load() (*BinlogPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var pos BinlogPosition
	if err := json.Unmarshal(data, &pos); err != nil {
		return nil, err
	}
	return &pos, nil
}
