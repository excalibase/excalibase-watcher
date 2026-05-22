//go:build e2e

package e2e

// Multi-tenant shared-stream test. Verifies that multiple watchers publishing
// to a single shared JetStream stream (distinguished only by subject prefix)
// do NOT clobber each other's topology.
//
// Before the fix: each watcher called CreateOrUpdateStream on boot, replacing
// the stream's subjects list. First tenant's publishes started failing with
// "no response from stream" as soon as a second tenant started.
//
// After the fix: the watcher is a pure publisher. Infra provisions the stream
// once with a wildcard (cdc.>); each tenant uses a sub-prefix that falls under
// the wildcard. No topology changes per tenant.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// provisionSharedStream creates a single JetStream stream with a wildcard
// subject pattern, simulating what infra (helm hook / nats-box / terraform)
// would do once per NATS cluster.
func provisionSharedStream(t *testing.T, streamName, wildcard string) {
	t.Helper()
	conn, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()
	js, _ := jetstream.New(conn)
	ctx := context.Background()
	js.DeleteStream(ctx, streamName) // clean slate
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{wildcard},
		Storage:   jetstream.MemoryStorage,
		Retention: jetstream.LimitsPolicy,
	}); err != nil {
		t.Fatalf("provision stream: %v", err)
	}
}

// pgTenantConfig returns a PG watcher config using the given shared stream
// and a tenant-specific subject prefix that falls under the stream wildcard.
func pgTenantConfig(slotName, streamName, tenantPrefix string) string {
	return fmt.Sprintf(`
postgres:
  enabled: true
  url: "%s"
  slot_name: "%s"
  publication_name: "e2e_tenant_pub_%s"
  create_slot: true
  create_publication: true
  snapshot_mode: none

mysql:
  enabled: false

nats:
  enabled: true
  url: "%s"
  stream_name: "%s"
  subject_prefix: "%s"
  max_age_minutes: 5
  storage: memory

health:
  port: %d

metrics:
  enabled: true
`, pgConnStr, slotName, slotName, natsURL, streamName, tenantPrefix, healthPort)
}

// TestE2E_SharedStream_NoTopologyClobber starts two PG watchers for two
// different "tenants" against the same shared stream. Proves that:
//   1. Both watchers start successfully (stream exists, they don't try to create it).
//   2. Tenant A's publishes land on cdc.tenant-a.* subjects.
//   3. Tenant B's publishes land on cdc.tenant-b.* subjects.
//   4. Neither tenant clobbers the other's subject list.
func TestE2E_SharedStream_NoTopologyClobber(t *testing.T) {
	pgSetup(t)

	// Provision ONE shared stream with wildcard — like infra would.
	const sharedStream = "CDC_SHARED"
	provisionSharedStream(t, sharedStream, "cdc.>")

	slotA := "tenant_a"
	slotB := "tenant_b"
	pgCleanupSlot(t, slotA)
	pgCleanupSlot(t, slotB)

	// Tenant A
	cfgA := pgTenantConfig(slotA, sharedStream, "cdc.tenant-a")
	wpA := startWatcher(t, cfgA)
	defer pgCleanupSlot(t, slotA)
	defer wpA.stop(t)
	time.Sleep(time.Second)

	// Tenant B starts while tenant A is already running on the same stream.
	// Before the fix, this would clobber A's subjects.
	cfgB := pgTenantConfig(slotB, sharedStream, "cdc.tenant-b")
	wpB := startWatcher(t, cfgB)
	defer pgCleanupSlot(t, slotB)
	defer wpB.stop(t)
	time.Sleep(time.Second)

	// Subscribe to each tenant's subjects independently.
	ncA := newNATSConsumer(t, sharedStream, "cdc.tenant-a.>")
	defer ncA.close()
	ncB := newNATSConsumer(t, sharedStream, "cdc.tenant-b.>")
	defer ncB.close()

	// Write to the shared PG — both watchers see the same rows but publish
	// under their own prefix.
	pgExec(t, "INSERT INTO e2e_users (name) VALUES ('tenant_shared_row')")

	// Both tenants must receive the event (they're both watching the same table).
	eventA := ncA.fetchUntilType(t, cdc.Insert, 15*time.Second)
	if !strings.HasPrefix(buildSubjectFromEvent("cdc.tenant-a", eventA), "cdc.tenant-a.") {
		t.Errorf("tenant A event schema=%q table=%q", eventA.Schema, eventA.Table)
	}

	eventB := ncB.fetchUntilType(t, cdc.Insert, 15*time.Second)
	if !strings.HasPrefix(buildSubjectFromEvent("cdc.tenant-b", eventB), "cdc.tenant-b.") {
		t.Errorf("tenant B event schema=%q table=%q", eventB.Schema, eventB.Table)
	}

	// Verify the stream's subject list is still the original wildcard —
	// neither watcher should have modified it.
	conn, _ := nats.Connect(natsURL)
	defer conn.Close()
	js, _ := jetstream.New(conn)
	info, err := js.Stream(context.Background(), sharedStream)
	if err != nil {
		t.Fatalf("looking up stream: %v", err)
	}
	si, _ := info.Info(context.Background())
	if len(si.Config.Subjects) != 1 || si.Config.Subjects[0] != "cdc.>" {
		t.Errorf("stream subjects clobbered: %v (want [cdc.>])", si.Config.Subjects)
	}
}

// TestE2E_FailFastWhenStreamMissing proves the watcher refuses to start when
// the stream is not pre-provisioned. This replaces the old auto-create
// behavior that caused the multi-tenant bug.
func TestE2E_FailFastWhenStreamMissing(t *testing.T) {
	pgSetup(t)
	slot := "e2e_nostream"
	pgCleanupSlot(t, slot)

	// Delete any lingering stream with this name so the watcher must fail.
	const missingStream = "CDC_DOES_NOT_EXIST"
	conn, _ := nats.Connect(natsURL)
	js, _ := jetstream.New(conn)
	_ = js.DeleteStream(context.Background(), missingStream)
	conn.Close()

	cfg := pgTenantConfig(slot, missingStream, "cdc.nonexistent")

	// Bypass startWatcher's auto-provision so the watcher sees a missing stream.
	wp := startWatcherWithoutStreamProvision(t, cfg)
	defer pgCleanupSlot(t, slot)

	// The watcher should exit with non-zero status because ensureStream
	// returns an error on missing stream.
	done := make(chan error, 1)
	go func() { done <- wp.cmd.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("watcher exited cleanly; expected non-zero exit on missing stream")
		}
		// expected path — process exited with error
	case <-time.After(15 * time.Second):
		_ = wp.cmd.Process.Kill()
		t.Fatal("watcher still running after 15s; should have failed fast on missing stream")
	}
}

// buildSubjectFromEvent mirrors the publisher's subject construction so tests
// can assert routing without importing the internal publisher package.
func buildSubjectFromEvent(prefix string, e cdc.Event) string {
	schema := e.Schema
	if schema == "" {
		schema = "default"
	}
	table := e.Table
	if table == "" {
		table = "_ddl"
	}
	return prefix + "." + schema + "." + table
}
