//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
)

// EXC-589: against a server that requires TLS of the watcher's login, the
// watcher streams over TLS verified against the project's CA, and refuses a
// server signed by any other CA instead of falling back.
func TestMySQLListenerStreamsOverVerifiedTLSOnly(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()

	plain, err := sql.Open("mysql", fmt.Sprintf("root:testpass@%s/testdb", addr))
	if err != nil {
		t.Fatal(err)
	}
	if err := plain.Ping(); err == nil {
		t.Fatal("the server accepted root without TLS; the test would prove nothing")
	}
	plain.Close()

	svc := cdc.NewService()
	defer svc.Shutdown()
	cfg := makeMySQLConfig(addr)
	cfg.Tables = []string{"cdc_test_users"}
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatal(err)
	}
	ch, unsub := svc.Subscribe("cdc_test_users")
	defer unsub()
	if err := listener.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()
	svc.MarkRunning()
	time.Sleep(3 * time.Second)

	execMySQL(t, addr, "INSERT INTO cdc_test_users (name) VALUES ('over tls')")
	if event := drainUntilType(t, ch, cdc.Insert, 15*time.Second); event.Table != "cdc_test_users" {
		t.Fatalf("event %+v", event)
	}
}

func TestMySQLListenerRefusesAServerFromAnotherCA(t *testing.T) {
	addr, _, cleanup := setupMySQL(t)
	defer cleanup()

	svc := cdc.NewService()
	defer svc.Shutdown()
	cfg := makeMySQLConfig(addr)
	cfg.CACert = newTestCA(t).pem
	listener, err := NewListener(cfg, svc)
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Start(context.Background()); err == nil {
		listener.Stop()
		t.Fatal("connected to a server its CA did not sign")
	}
}
