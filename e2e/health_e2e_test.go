//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/cdc"
)

func TestE2E_HealthEndpoint(t *testing.T) {
pgSetup(t)
	slot := "e2e_health"
	deleteNATSStream(pgStream(slot))

	wp := startWatcher(t,pgOnlyConfig(slot))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	url := fmt.Sprintf("http://localhost:%d", healthPort)
	code, body := httpGet(t, url+"/healthz")
	if code != 200 {
		t.Errorf("health status = %d, want 200\nbody: %s", code, body)
	}

	var status map[string]interface{}
	if err := json.Unmarshal([]byte(body), &status); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if status["status"] != "UP" {
		t.Errorf("status = %v, want UP", status["status"])
	}
}

func TestE2E_ReadyEndpoint(t *testing.T) {
pgSetup(t)
	slot := "e2e_ready"
	deleteNATSStream(pgStream(slot))

	wp := startWatcher(t,pgOnlyConfig(slot))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	url := fmt.Sprintf("http://localhost:%d", healthPort)
	code, body := httpGet(t, url+"/readyz")
	if code != 200 {
		t.Errorf("ready status = %d, want 200\nbody: %s", code, body)
	}
	if !strings.Contains(body, "ready") {
		t.Errorf("body should contain 'ready': %s", body)
	}
}

func TestE2E_MetricsEndpoint(t *testing.T) {
pgSetup(t)
	slot := "e2e_metr"
	deleteNATSStream(pgStream(slot))

	wp := startWatcher(t,pgOnlyConfig(slot))
	defer pgCleanupSlot(t, slot)
	defer wp.stop(t)

	nc := newNATSConsumer(t, pgStream(slot), pgSubjectWild(slot))
	defer nc.close()

	pgExec(t, "INSERT INTO e2e_users (name) VALUES ('MetricsTest')")
	nc.fetchUntilType(t, cdc.Insert, 10*time.Second)

	url := fmt.Sprintf("http://localhost:%d", healthPort)
	code, body := httpGet(t, url+"/metrics")
	if code != 200 {
		t.Errorf("metrics status = %d", code)
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Error("metrics missing go_goroutines")
	}
	if !strings.Contains(body, "# HELP") {
		t.Error("metrics missing Prometheus HELP lines")
	}
}
