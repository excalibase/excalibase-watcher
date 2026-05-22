package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ColumnDef struct {
	Name    string `json:"name"`
	TypeName string `json:"typeName,omitempty"`
	TypeOID int    `json:"typeOid"`
}

type HistoryEntry struct {
	Position  string      `json:"position"`
	Schema    string      `json:"schema"`
	Table     string      `json:"table"`
	Columns   []ColumnDef `json:"columns"`
	Timestamp int64       `json:"timestamp"`
}

type HistoryStore interface {
	Save(entry HistoryEntry) error
	GetLatest(schema, table string) (*HistoryEntry, error)
	GetHistory(schema, table string) ([]HistoryEntry, error)
}

type FileHistoryStore struct {
	dir   string
	mu    sync.RWMutex
	cache map[string][]HistoryEntry // key: "schema.table"
}

func NewFileHistoryStore(dir string) (*FileHistoryStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating schema history dir: %w", err)
	}
	return &FileHistoryStore{
		dir:   dir,
		cache: make(map[string][]HistoryEntry),
	}, nil
}

func (s *FileHistoryStore) Save(entry HistoryEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixMilli()
	}

	key := entry.Schema + "." + entry.Table
	s.cache[key] = append(s.cache[key], entry)

	return s.writeFile(key)
}

func (s *FileHistoryStore) GetLatest(schemaName, table string) (*HistoryEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := schemaName + "." + table
	entries, err := s.loadEntries(key)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	return &entries[len(entries)-1], nil
}

func (s *FileHistoryStore) GetHistory(schemaName, table string) ([]HistoryEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := schemaName + "." + table
	return s.loadEntries(key)
}

func (s *FileHistoryStore) loadEntries(key string) ([]HistoryEntry, error) {
	if cached, ok := s.cache[key]; ok {
		return cached, nil
	}

	filePath := filepath.Join(s.dir, key+".json")
	data, err := os.ReadFile(filePath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var entries []HistoryEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	s.cache[key] = entries
	return entries, nil
}

func (s *FileHistoryStore) writeFile(key string) error {
	entries := s.cache[key]
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}

	filePath := filepath.Join(s.dir, key+".json")
	tmpPath := filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmpPath, filePath)
}
