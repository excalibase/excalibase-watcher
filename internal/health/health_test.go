package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthUp(t *testing.T) {
	checker := NewChecker(func() bool { return true }, func() int { return 3 })

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	checker.HealthHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}

	var status Status
	json.Unmarshal(w.Body.Bytes(), &status)
	if status.Status != "UP" {
		t.Errorf("status = %q", status.Status)
	}
	if status.Subscriptions != 3 {
		t.Errorf("subscriptions = %d", status.Subscriptions)
	}
}

func TestHealthDown(t *testing.T) {
	checker := NewChecker(func() bool { return false }, func() int { return 0 })

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	checker.HealthHandler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}

	var status Status
	json.Unmarshal(w.Body.Bytes(), &status)
	if status.Status != "DOWN" {
		t.Errorf("status = %q", status.Status)
	}
}

func TestReadyUp(t *testing.T) {
	checker := NewChecker(func() bool { return true }, func() int { return 0 })
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	checker.ReadyHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestReadyDown(t *testing.T) {
	checker := NewChecker(func() bool { return false }, func() int { return 0 })

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	checker.ReadyHandler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func readyResponse(t *testing.T, checker *Checker) (int, map[string]string) {
	t.Helper()
	w := httptest.NewRecorder()
	checker.ReadyHandler(w, httptest.NewRequest("GET", "/readyz", nil))
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("readyz body %q: %v", w.Body.String(), err)
	}
	return w.Code, body
}

func TestReadyDownWhileADependencyIsNotReady(t *testing.T) {
	connected := false
	checker := NewChecker(func() bool { return true }, func() int { return 0 }).
		WithReadiness("NATS not connected", func() bool { return connected })

	code, body := readyResponse(t, checker)
	if code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 while disconnected", code)
	}
	if body["status"] != "not ready" || body["reason"] != "NATS not connected" {
		t.Errorf("body = %v", body)
	}

	connected = true
	code, body = readyResponse(t, checker)
	if code != http.StatusOK || body["status"] != "ready" {
		t.Errorf("status = %d body = %v, want 200 ready once connected", code, body)
	}
}

// Liveness must not follow NATS: restarting the pod cannot bring the server
// back sooner, it only replays the slot from the start again.
func TestHealthStaysUpWhileADependencyIsNotReady(t *testing.T) {
	checker := NewChecker(func() bool { return true }, func() int { return 0 }).
		WithReadiness("NATS not connected", func() bool { return false })

	w := httptest.NewRecorder()
	checker.HealthHandler(w, httptest.NewRequest("GET", "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Errorf("healthz = %d, want 200", w.Code)
	}
}

func TestWithReadinessLeavesTheOriginalCheckerUnchanged(t *testing.T) {
	base := NewChecker(func() bool { return true }, func() int { return 0 })
	_ = base.WithReadiness("never", func() bool { return false })

	if code, _ := readyResponse(t, base); code != http.StatusOK {
		t.Errorf("base checker status = %d, want 200", code)
	}
}
