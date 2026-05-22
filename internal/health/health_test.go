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
