package config

import (
	"fmt"
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
	Enabled           bool     `mapstructure:"enabled"`
	URL               string   `mapstructure:"url"`
	Username          string   `mapstructure:"username"`
	Password          string   `mapstructure:"password"`
	SlotName          string   `mapstructure:"slot_name"`
	PublicationName   string   `mapstructure:"publication_name"`
	CreateSlot        bool     `mapstructure:"create_slot"`
	CreatePublication bool     `mapstructure:"create_publication"`
	Tables            []string `mapstructure:"tables"`
	SnapshotMode      string   `mapstructure:"snapshot_mode"`
	SnapshotChunkSize int      `mapstructure:"snapshot_chunk_size"`
	SnapshotBackupFile string  `mapstructure:"snapshot_backup_file"`
	SnapshotStartLSN  string  `mapstructure:"snapshot_start_lsn"`
	CaptureDDL        bool     `mapstructure:"capture_ddl"`
	SchemaHistoryDir  string   `mapstructure:"schema_history_dir"`
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

func ValidateSnapshotMode(mode string) error {
	switch mode {
	case "", "none", "chunked", "backup_file":
		return nil
	default:
		return fmt.Errorf("invalid snapshot_mode %q: must be none, chunked, or backup_file", mode)
	}
}
