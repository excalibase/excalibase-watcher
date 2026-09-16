package postgres

import (
	"sync"
	"time"

	"github.com/excalibase/watcher-go/internal/metrics"
)

// lagTracker computes cdc_lag_seconds.
//
// Definition: the time elapsed since the commit timestamp of the last COMMIT
// record the watcher processed. The value keeps growing while the watcher is
// behind (WAL is still arriving, or events wait for a NATS ack) and is reset
// to 0 as soon as the watcher observes it is caught up: a primary keepalive
// arrived (the walsender has nothing more to send) and no handed-off event is
// still waiting for its NATS ack. It stays 0 until the next COMMIT record.
type lagTracker struct {
	mu         sync.Mutex
	now        func() time.Time
	lastCommit time.Time
	caughtUp   bool
}

func newLagTracker(now func() time.Time) *lagTracker {
	return &lagTracker{now: now}
}

func (t *lagTracker) ObserveCommit(commitTS time.Time) {
	t.mu.Lock()
	t.lastCommit = commitTS
	t.caughtUp = false
	t.mu.Unlock()
	t.Publish()
}

func (t *lagTracker) ObserveCaughtUp() {
	t.mu.Lock()
	t.caughtUp = true
	t.mu.Unlock()
	t.Publish()
}

func (t *lagTracker) Lag() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.caughtUp || t.lastCommit.IsZero() {
		return 0
	}
	return max(t.now().Sub(t.lastCommit), 0)
}

func (t *lagTracker) Publish() {
	metrics.SetLagSeconds(t.Lag().Seconds())
}
