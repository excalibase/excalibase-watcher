package health

import (
	"encoding/json"
	"net/http"
	"slices"
)

type Status struct {
	Status        string `json:"status"`
	CDCEnabled    bool   `json:"cdc.enabled"`
	Subscriptions int    `json:"cdc.subscriptions"`
	Reason        string `json:"reason,omitempty"`
}

type readyBody struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

type readinessCheck struct {
	reason string
	ready  func() bool
}

type Checker struct {
	isRunning    func() bool
	subscriberFn func() int
	readiness    []readinessCheck
}

const listenerNotRunning = "CDC listener not running"

func NewChecker(isRunning func() bool, subscriberFn func() int) *Checker {
	return &Checker{
		isRunning:    isRunning,
		subscriberFn: subscriberFn,
	}
}

// WithReadiness returns a checker that also reports not ready, with reason,
// while ready is false. Liveness is unaffected.
func (c *Checker) WithReadiness(reason string, ready func() bool) *Checker {
	next := *c
	next.readiness = append(slices.Clone(c.readiness), readinessCheck{reason: reason, ready: ready})
	return &next
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
		status.Reason = listenerNotRunning
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(status)
}

func (c *Checker) ReadyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	reason := c.notReadyReason()
	if reason == "" {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(readyBody{Status: "ready"})
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	json.NewEncoder(w).Encode(readyBody{Status: "not ready", Reason: reason})
}

func (c *Checker) notReadyReason() string {
	if !c.isRunning() {
		return listenerNotRunning
	}
	for _, check := range c.readiness {
		if !check.ready() {
			return check.reason
		}
	}
	return ""
}
