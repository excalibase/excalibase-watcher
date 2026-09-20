package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Postgres PostgresConfig `mapstructure:"postgres"`
	MySQL    MySQLConfig    `mapstructure:"mysql"`
	NATS     NATSConfig     `mapstructure:"nats"`
	Health   HealthConfig   `mapstructure:"health"`
	Metrics  MetricsConfig  `mapstructure:"metrics"`
}

type PostgresConfig struct {
	Enabled            bool     `mapstructure:"enabled"`
	URL                string   `mapstructure:"url"`
	Username           string   `mapstructure:"username"`
	Password           string   `mapstructure:"password"`
	SlotName           string   `mapstructure:"slot_name"`
	PublicationName    string   `mapstructure:"publication_name"`
	CreateSlot         bool     `mapstructure:"create_slot"`
	CreatePublication  bool     `mapstructure:"create_publication"`
	Tables             []string `mapstructure:"tables"`
	SnapshotMode       string   `mapstructure:"snapshot_mode"`
	SnapshotChunkSize  int      `mapstructure:"snapshot_chunk_size"`
	SnapshotBackupFile string   `mapstructure:"snapshot_backup_file"`
	SnapshotStartLSN   string   `mapstructure:"snapshot_start_lsn"`
	CaptureDDL         bool     `mapstructure:"capture_ddl"`
	SchemaHistoryDir   string   `mapstructure:"schema_history_dir"`
	// SlotStatsIntervalSeconds is how often pg_replication_slots is sampled for
	// the cdc_slot_* gauges and cdc_lag_seconds is refreshed.
	SlotStatsIntervalSeconds int `mapstructure:"slot_stats_interval_seconds"`
	// OwnerID identifies this watcher in the slot owner registry (default: hostname).
	OwnerID     string            `mapstructure:"owner_id"`
	SlotCleanup SlotCleanupConfig `mapstructure:"slot_cleanup"`
}

// SlotCleanupConfig controls orphaned replication slot detection and removal.
type SlotCleanupConfig struct {
	Enabled           bool `mapstructure:"enabled"`
	IntervalMinutes   int  `mapstructure:"interval_minutes"`
	StaleAfterMinutes int  `mapstructure:"stale_after_minutes"`
	// RetainedWALThresholdBytes: only slots retaining more WAL than this are dropped; 0 = any.
	RetainedWALThresholdBytes int64 `mapstructure:"retained_wal_threshold_bytes"`
	DryRun                    bool  `mapstructure:"dry_run"`
	// SlotPattern: only slots whose name matches this regexp are ever considered.
	SlotPattern      string `mapstructure:"slot_pattern"`
	HeartbeatSeconds int    `mapstructure:"heartbeat_seconds"`
}

type MySQLConfig struct {
	Enabled            bool     `mapstructure:"enabled"`
	Host               string   `mapstructure:"host"`
	Username           string   `mapstructure:"username"`
	Password           string   `mapstructure:"password"`
	Schema             string   `mapstructure:"schema"`
	Tables             []string `mapstructure:"tables"`
	SnapshotMode       string   `mapstructure:"snapshot_mode"`
	SnapshotChunkSize  int      `mapstructure:"snapshot_chunk_size"`
	SnapshotBackupFile string   `mapstructure:"snapshot_backup_file"`
	OffsetFile         string   `mapstructure:"offset_file"`
}

type NATSConfig struct {
	Enabled       bool   `mapstructure:"enabled"`
	URL           string `mapstructure:"url"`
	StreamName    string `mapstructure:"stream_name"`
	SubjectPrefix string `mapstructure:"subject_prefix"`
	MaxAgeMinutes int    `mapstructure:"max_age_minutes"`
	Storage       string `mapstructure:"storage"`
	// Username/Password authenticate this watcher to a NATS server that
	// scopes publish permissions per principal. Blank connects anonymously.
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	// InboxPrefix is the request/reply subject space this principal is
	// allowed to subscribe to. It must match what the server grants,
	// otherwise JetStream publish acks never arrive. Blank keeps the
	// client default (_INBOX).
	InboxPrefix string `mapstructure:"inbox_prefix"`
}

type HealthConfig struct {
	Port        int    `mapstructure:"port"`
	BindAddress string `mapstructure:"bind_address"`
}

type MetricsConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

func Load(cfgFile string) (*Config, error) {
	v := viper.New()

	// Defaults — Postgres
	v.SetDefault("postgres.slot_name", "cdc_slot")
	v.SetDefault("postgres.publication_name", "cdc_publication")
	v.SetDefault("postgres.create_slot", true)
	v.SetDefault("postgres.create_publication", true)
	v.SetDefault("postgres.snapshot_mode", "none")
	v.SetDefault("postgres.snapshot_chunk_size", 10000)
	v.SetDefault("postgres.slot_stats_interval_seconds", 15)
	v.SetDefault("postgres.owner_id", defaultOwnerID())
	v.SetDefault("postgres.slot_cleanup.enabled", true)
	v.SetDefault("postgres.slot_cleanup.interval_minutes", 10)
	v.SetDefault("postgres.slot_cleanup.stale_after_minutes", 30)
	v.SetDefault("postgres.slot_cleanup.retained_wal_threshold_bytes", 0)
	v.SetDefault("postgres.slot_cleanup.dry_run", false)
	v.SetDefault("postgres.slot_cleanup.slot_pattern", "^cdc_")
	v.SetDefault("postgres.slot_cleanup.heartbeat_seconds", 30)

	// Defaults — MySQL
	v.SetDefault("mysql.enabled", false)
	v.SetDefault("mysql.snapshot_mode", "none")
	v.SetDefault("mysql.snapshot_chunk_size", 10000)

	// Defaults — NATS
	v.SetDefault("nats.enabled", true)
	v.SetDefault("nats.url", "nats://localhost:4222")
	v.SetDefault("nats.stream_name", "CDC")
	v.SetDefault("nats.subject_prefix", "cdc")
	v.SetDefault("nats.max_age_minutes", 5)
	v.SetDefault("nats.storage", "memory")

	// Defaults — Health
	v.SetDefault("health.port", 8080)
	v.SetDefault("health.bind_address", "127.0.0.1")

	// Defaults — Metrics
	v.SetDefault("metrics.enabled", true)

	// Read config file
	v.SetConfigFile(cfgFile)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	// Env var override: WATCHER_POSTGRES_URL -> postgres.url
	v.SetEnvPrefix("WATCHER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Explicit bindings for sensitive fields to ensure env vars override config file
	v.BindEnv("postgres.url", "WATCHER_POSTGRES_URL")
	v.BindEnv("postgres.username", "WATCHER_POSTGRES_USERNAME")
	v.BindEnv("postgres.password", "WATCHER_POSTGRES_PASSWORD")
	v.BindEnv("mysql.host", "WATCHER_MYSQL_HOST")
	v.BindEnv("mysql.username", "WATCHER_MYSQL_USERNAME")
	v.BindEnv("mysql.password", "WATCHER_MYSQL_PASSWORD")
	v.BindEnv("nats.url", "WATCHER_NATS_URL")
	v.BindEnv("nats.username", "WATCHER_NATS_USERNAME")
	v.BindEnv("nats.password", "WATCHER_NATS_PASSWORD")
	v.BindEnv("nats.inbox_prefix", "WATCHER_NATS_INBOX_PREFIX")

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	if err := ValidateSnapshotMode(cfg.Postgres.SnapshotMode); err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	if err := ValidateSnapshotMode(cfg.MySQL.SnapshotMode); err != nil {
		return nil, fmt.Errorf("mysql: %w", err)
	}

	return &cfg, nil
}

func defaultOwnerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "watcher"
	}
	return host
}

func ValidateSnapshotMode(mode string) error {
	switch mode {
	case "", "none", "chunked", "backup_file":
		return nil
	default:
		return fmt.Errorf("invalid snapshot_mode %q: must be none, chunked, or backup_file", mode)
	}
}
