package health

import (
	"encoding/json"
	"net/http"
)

type Status struct {
	Status        string `json:"status"`
	CDCEnabled    bool   `json:"cdc.enabled"`
	Subscriptions int    `json:"cdc.subscriptions"`
	Reason        string `json:"reason,omitempty"`
}

type Checker struct {
	isRunning    func() bool
	subscriberFn func() int
}

func NewChecker(isRunning func() bool, subscriberFn func() int) *Checker {
	return &Checker{
		isRunning:    isRunning,
		subscriberFn: subscriberFn,
	}
}

func (c *Checker) HealthHandler(w http.ResponseWriter, r *http.Request) {
	status := Status{
		CDCEnabled:    true,
		Subscriptions: c.subscriberFn(),
	}

	w.Header().Set("Content-Type", "application/json")
	if c.isRunning() {
		status.Status = "UP"
		w.WriteHeader(http.StatusOK)
	} else {
		status.Status = "DOWN"
		status.Reason = "CDC listener not running"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(status)
}

func (c *Checker) ReadyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if c.isRunning() {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready"}`))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"not ready"}`))
	}
}
