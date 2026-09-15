package nats

import (
	"testing"

	"github.com/excalibase/watcher-go/internal/config"
)

// A watcher with no credential must keep connecting exactly as before, so an
// unauthenticated local/dev NATS is unaffected.
func TestConnectOptionsWithoutCredential(t *testing.T) {
	opts := connectOptions(config.NATSConfig{URL: "nats://localhost:4222"})
	if len(opts) != 2 {
		t.Errorf("got %d options, want reconnect settings only", len(opts))
	}
}

func TestConnectOptionsCarriesCredential(t *testing.T) {
	opts := connectOptions(config.NATSConfig{
		Username: "tenant-watcher:proj-a",
		Password: "pw",
	})
	if len(opts) != 3 {
		t.Errorf("got %d options, want reconnect settings + credential", len(opts))
	}
}

func TestConnectOptionsCarriesInboxPrefix(t *testing.T) {
	opts := connectOptions(config.NATSConfig{
		Username:    "tenant-watcher:proj-a",
		Password:    "pw",
		InboxPrefix: "_INBOX_tw_proj-a",
	})
	if len(opts) != 4 {
		t.Errorf("got %d options, want reconnect settings + credential + inbox prefix", len(opts))
	}
}
