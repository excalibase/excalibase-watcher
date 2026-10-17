package mysql

import (
	"context"
	"crypto/x509"
	"errors"
	"testing"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/excalibase/watcher-go/internal/config"
)

// EXC-589: the watcher verifies the server like every other platform client:
// the project's CA only, the recorded host name, TLS on every connection.
func TestBuildCanalConfigVerifiesTheServerWithTheProjectCA(t *testing.T) {
	ca, other := newTestCA(t), newTestCA(t)
	host := "moco-p-mysql-primary.org-p.svc.cluster.local"
	l, _ := NewListener(config.MySQLConfig{Host: host + ":3306", CACert: ca.pem}, cdc.NewService())
	cfg, err := l.buildCanalConfig()
	if err != nil {
		t.Fatalf("buildCanalConfig: %v", err)
	}
	tlsConfig := cfg.TLSConfig
	if tlsConfig == nil || tlsConfig.InsecureSkipVerify || tlsConfig.ServerName != host {
		t.Fatalf("TLS config %+v", tlsConfig)
	}
	verify := func(issuer testCA, name string) error {
		certPEM, _ := issuer.issue(t, name)
		cert := mustParse(t, certPEM)
		_, err := cert.Verify(x509.VerifyOptions{Roots: tlsConfig.RootCAs, DNSName: tlsConfig.ServerName})
		return err
	}
	if err := verify(ca, host); err != nil {
		t.Errorf("the project's server is refused: %v", err)
	}
	if verify(other, host) == nil {
		t.Error("a certificate from another CA is trusted")
	}
	if verify(ca, "elsewhere.example") == nil {
		t.Error("a certificate for another host is trusted")
	}
}

// Without a CA there is nothing to verify the server with, and the watcher
// does not connect at all rather than fall back to plaintext or no checks.
func TestTheListenerRefusesToStartWithoutAServerCA(t *testing.T) {
	for name, caPEM := range map[string]string{"missing": "", "not a certificate": "garbage"} {
		t.Run(name, func(t *testing.T) {
			l, _ := NewListener(config.MySQLConfig{Host: "db:3306", CACert: caPEM}, cdc.NewService())
			if _, err := l.buildCanalConfig(); !errors.Is(err, ErrNoServerCA) {
				t.Fatalf("buildCanalConfig err = %v, want ErrNoServerCA", err)
			}
			if err := l.Start(context.Background()); !errors.Is(err, ErrNoServerCA) {
				t.Fatalf("Start err = %v, want ErrNoServerCA", err)
			}
		})
	}
}

func TestBuildCanalConfigNeedsAHostName(t *testing.T) {
	l, _ := NewListener(config.MySQLConfig{Host: ":3306", CACert: newTestCA(t).pem}, cdc.NewService())
	if _, err := l.buildCanalConfig(); err == nil {
		t.Fatal("built a TLS config with no server name to verify")
	}
}
