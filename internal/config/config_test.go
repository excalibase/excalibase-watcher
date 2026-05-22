package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromYAML(t *testing.T) {
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.yaml")
	yaml := `
postgres:
  enabled: true
  url: "postgres://localhost:5432/mydb?replication=database"
  username: pguser
  password: pgpass
  slot_name: my_slot
  publication_name: my_pub
  create_slot: false
  create_publication: false
  tables:
    - users
    - orders
  snapshot_mode: chunked
  snapshot_chunk_size: 5000
  capture_ddl: true
  schema_history_dir: /tmp/schema

mysql:
  enabled: true
  host: "localhost:3307"
  username: root
  password: secret
  schema: mydb
  tables:
    - products
  snapshot_mode: backup_file
  snapshot_chunk_size: 2000
  snapshot_backup_file: /tmp/dump.sql
  offset_file: /tmp/offset.json

nats:
  enabled: true
  url: "nats://nats.example.com:4222"
  stream_name: MY_CDC
  subject_prefix: events
  max_age_minutes: 30
  storage: file

health:
  port: 9090

metrics:
  enabled: false
`
	if err := os.WriteFile(cfgFile, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgFile)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	// Postgres
	if !cfg.Postgres.Enabled {
		t.Error("postgres.enabled should be true")
	}
	if cfg.Postgres.URL != "postgres://localhost:5432/mydb?replication=database" {
		t.Errorf("postgres.url = %q", cfg.Postgres.URL)
	}
	if cfg.Postgres.Username != "pguser" {
		t.Errorf("postgres.username = %q", cfg.Postgres.Username)
	}
	if cfg.Postgres.Password != "pgpass" {
		t.Errorf("postgres.password = %q", cfg.Postgres.Password)
	}
	if cfg.Postgres.SlotName != "my_slot" {
		t.Errorf("postgres.slot_name = %q", cfg.Postgres.SlotName)
	}
	if cfg.Postgres.PublicationName != "my_pub" {
		t.Errorf("postgres.publication_name = %q", cfg.Postgres.PublicationName)
	}
	if cfg.Postgres.CreateSlot {
		t.Error("postgres.create_slot should be false")
	}
	if cfg.Postgres.CreatePublication {
		t.Error("postgres.create_publication should be false")
	}
	if len(cfg.Postgres.Tables) != 2 || cfg.Postgres.Tables[0] != "users" {
		t.Errorf("postgres.tables = %v", cfg.Postgres.Tables)
	}
	if cfg.Postgres.SnapshotMode != "chunked" {
		t.Errorf("postgres.snapshot_mode = %q", cfg.Postgres.SnapshotMode)
	}
	if cfg.Postgres.SnapshotChunkSize != 5000 {
		t.Errorf("postgres.snapshot_chunk_size = %d", cfg.Postgres.SnapshotChunkSize)
	}
	if !cfg.Postgres.CaptureDDL {
		t.Error("postgres.capture_ddl should be true")
	}
	if cfg.Postgres.SchemaHistoryDir != "/tmp/schema" {
		t.Errorf("postgres.schema_history_dir = %q", cfg.Postgres.SchemaHistoryDir)
	}

	// MySQL
	if !cfg.MySQL.Enabled {
		t.Error("mysql.enabled should be true")
	}
	if cfg.MySQL.Host != "localhost:3307" {
		t.Errorf("mysql.host = %q", cfg.MySQL.Host)
	}
	if cfg.MySQL.Schema != "mydb" {
		t.Errorf("mysql.schema = %q", cfg.MySQL.Schema)
	}
	if len(cfg.MySQL.Tables) != 1 || cfg.MySQL.Tables[0] != "products" {
		t.Errorf("mysql.tables = %v", cfg.MySQL.Tables)
	}
	if cfg.MySQL.SnapshotMode != "backup_file" {
		t.Errorf("mysql.snapshot_mode = %q", cfg.MySQL.SnapshotMode)
	}
	if cfg.MySQL.SnapshotBackupFile != "/tmp/dump.sql" {
		t.Errorf("mysql.snapshot_backup_file = %q", cfg.MySQL.SnapshotBackupFile)
	}
	if cfg.MySQL.OffsetFile != "/tmp/offset.json" {
		t.Errorf("mysql.offset_file = %q", cfg.MySQL.OffsetFile)
	}

	// NATS
	if !cfg.NATS.Enabled {
		t.Error("nats.enabled should be true")
	}
	if cfg.NATS.URL != "nats://nats.example.com:4222" {
		t.Errorf("nats.url = %q", cfg.NATS.URL)
	}
	if cfg.NATS.StreamName != "MY_CDC" {
		t.Errorf("nats.stream_name = %q", cfg.NATS.StreamName)
	}
	if cfg.NATS.SubjectPrefix != "events" {
		t.Errorf("nats.subject_prefix = %q", cfg.NATS.SubjectPrefix)
	}
	if cfg.NATS.MaxAgeMinutes != 30 {
		t.Errorf("nats.max_age_minutes = %d", cfg.NATS.MaxAgeMinutes)
	}
	if cfg.NATS.Storage != "file" {
		t.Errorf("nats.storage = %q", cfg.NATS.Storage)
	}

	// Health
	if cfg.Health.Port != 9090 {
		t.Errorf("health.port = %d", cfg.Health.Port)
	}

	// Metrics
	if cfg.Metrics.Enabled {
		t.Error("metrics.enabled should be false")
	}
}

func TestDefaults(t *testing.T) {
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.yaml")
	// Minimal config — all defaults should apply
	yaml := `
postgres:
  url: "postgres://localhost:5432/mydb"
  username: pg
  password: pg
`
	if err := os.WriteFile(cfgFile, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgFile)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	// Postgres defaults
	if cfg.Postgres.SlotName != "cdc_slot" {
		t.Errorf("default slot_name = %q, want cdc_slot", cfg.Postgres.SlotName)
	}
	if cfg.Postgres.PublicationName != "cdc_publication" {
		t.Errorf("default publication_name = %q, want cdc_publication", cfg.Postgres.PublicationName)
	}
	if !cfg.Postgres.CreateSlot {
		t.Error("default create_slot should be true")
	}
	if !cfg.Postgres.CreatePublication {
		t.Error("default create_publication should be true")
	}
	if cfg.Postgres.SnapshotMode != "none" {
		t.Errorf("default snapshot_mode = %q, want none", cfg.Postgres.SnapshotMode)
	}
	if cfg.Postgres.SnapshotChunkSize != 10000 {
		t.Errorf("default snapshot_chunk_size = %d, want 10000", cfg.Postgres.SnapshotChunkSize)
	}

	// MySQL defaults
	if cfg.MySQL.Enabled {
		t.Error("default mysql.enabled should be false")
	}
	if cfg.MySQL.SnapshotChunkSize != 10000 {
		t.Errorf("default mysql.snapshot_chunk_size = %d, want 10000", cfg.MySQL.SnapshotChunkSize)
	}

	// NATS defaults
	if cfg.NATS.URL != "nats://localhost:4222" {
		t.Errorf("default nats.url = %q", cfg.NATS.URL)
	}
	if cfg.NATS.StreamName != "CDC" {
		t.Errorf("default nats.stream_name = %q", cfg.NATS.StreamName)
	}
	if cfg.NATS.SubjectPrefix != "cdc" {
		t.Errorf("default nats.subject_prefix = %q", cfg.NATS.SubjectPrefix)
	}
	if cfg.NATS.MaxAgeMinutes != 5 {
		t.Errorf("default nats.max_age_minutes = %d", cfg.NATS.MaxAgeMinutes)
	}
	if cfg.NATS.Storage != "memory" {
		t.Errorf("default nats.storage = %q", cfg.NATS.Storage)
	}
	if !cfg.NATS.Enabled {
		t.Error("default nats.enabled should be true")
	}

	// Health default
	if cfg.Health.Port != 8080 {
		t.Errorf("default health.port = %d, want 8080", cfg.Health.Port)
	}

	// Metrics default
	if !cfg.Metrics.Enabled {
		t.Error("default metrics.enabled should be true")
	}
}

func TestEnvOverride(t *testing.T) {
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.yaml")
	yaml := `
postgres:
  url: "postgres://localhost:5432/mydb"
  username: pg
  password: pg
`
	if err := os.WriteFile(cfgFile, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	t.Setenv("WATCHER_POSTGRES_URL", "postgres://override:5432/other")
	t.Setenv("WATCHER_NATS_STREAM_NAME", "OVERRIDE_STREAM")

	cfg, err := Load(cfgFile)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Postgres.URL != "postgres://override:5432/other" {
		t.Errorf("env override postgres.url = %q", cfg.Postgres.URL)
	}
	if cfg.NATS.StreamName != "OVERRIDE_STREAM" {
		t.Errorf("env override nats.stream_name = %q", cfg.NATS.StreamName)
	}
}

func TestValidateSnapshotMode(t *testing.T) {
	tests := []struct {
		mode  string
		valid bool
	}{
		{"none", true},
		{"chunked", true},
		{"backup_file", true},
		{"invalid", false},
		{"", true}, // empty treated as none
	}

	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			err := ValidateSnapshotMode(tt.mode)
			if tt.valid && err != nil {
				t.Errorf("ValidateSnapshotMode(%q) unexpected error: %v", tt.mode, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("ValidateSnapshotMode(%q) expected error", tt.mode)
			}
		})
	}
}
