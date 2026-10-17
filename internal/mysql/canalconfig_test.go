package mysql

import (
	"crypto/x509"
	"encoding/pem"
	"regexp"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
	"github.com/go-mysql-org/go-mysql/canal"
)

func TestBuildCanalConfigBasic(t *testing.T) {
	svc := cdc.NewService()
	l, _ := NewListener(config.MySQLConfig{
		Host:     "db:3306",
		Username: "u",
		Password: "p",
		Schema:   "app",
		CACert:   newTestCA(t).pem,
	}, svc)
	cfg := mustCanalConfig(t, l)

	if cfg.Addr != "db:3306" {
		t.Errorf("Addr = %q", cfg.Addr)
	}
	if cfg.User != "u" || cfg.Password != "p" {
		t.Errorf("creds = %q/%q", cfg.User, cfg.Password)
	}
	if cfg.Flavor != "mysql" {
		t.Errorf("Flavor = %q", cfg.Flavor)
	}
	if cfg.Dump.ExecutionPath != "" {
		t.Errorf("ExecutionPath should be empty, got %q", cfg.Dump.ExecutionPath)
	}
	if !cfg.ParseTime || !cfg.UseDecimal {
		t.Errorf("ParseTime/UseDecimal not set: %v/%v", cfg.ParseTime, cfg.UseDecimal)
	}
	if cfg.ServerID == 0 {
		t.Error("ServerID should be nonzero")
	}
	if len(cfg.IncludeTableRegex) != 0 {
		t.Errorf("expected no table regex without filter, got %v", cfg.IncludeTableRegex)
	}
}

func TestBuildCanalConfigTableFilter(t *testing.T) {
	svc := cdc.NewService()
	l, _ := NewListener(config.MySQLConfig{
		Host:   "db:3306",
		Schema: "app",
		Tables: []string{"users", "orders"},
		CACert: newTestCA(t).pem,
	}, svc)
	cfg := mustCanalConfig(t, l)

	if len(cfg.IncludeTableRegex) != 2 {
		t.Fatalf("expected 2 regex entries, got %v", cfg.IncludeTableRegex)
	}
	for _, rx := range cfg.IncludeTableRegex {
		if _, err := regexp.Compile(rx); err != nil {
			t.Errorf("invalid regex %q: %v", rx, err)
		}
	}
	if cfg.IncludeTableRegex[0] != `^app\.users$` {
		t.Errorf("regex[0] = %q", cfg.IncludeTableRegex[0])
	}
}

func TestBuildCanalConfigEscapesRegexMetachars(t *testing.T) {
	// A schema containing regex metacharacters must be quoted so an
	// attacker-controlled config can't widen the filter.
	svc := cdc.NewService()
	l, _ := NewListener(config.MySQLConfig{
		Host:   "db:3306",
		Schema: "a.b",
		Tables: []string{"t.1"},
		CACert: newTestCA(t).pem,
	}, svc)
	cfg := mustCanalConfig(t, l)

	if cfg.IncludeTableRegex[0] != `^a\.b\.t\.1$` {
		t.Errorf("regex not properly escaped: %q", cfg.IncludeTableRegex[0])
	}
}

func mustCanalConfig(t *testing.T, l *Listener) *canal.Config {
	t.Helper()
	cfg, err := l.buildCanalConfig()
	if err != nil {
		t.Fatalf("buildCanalConfig: %v", err)
	}
	return cfg
}

func mustParse(t *testing.T, certPEM string) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode([]byte(certPEM))
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}
