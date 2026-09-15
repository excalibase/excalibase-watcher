package postgres

import (
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
}

func TestLagIsZeroBeforeAnyCommit(t *testing.T) {
	clock := newFakeClock()
	tracker := newLagTracker(clock.Now)

	if got := tracker.Lag(); got != 0 {
		t.Errorf("Lag() before any commit = %v, want 0", got)
	}
}

func TestLagGrowsFromCommitTimestamp(t *testing.T) {
	clock := newFakeClock()
	tracker := newLagTracker(clock.Now)

	tracker.ObserveCommit(clock.Now().Add(-2 * time.Second))
	if got := tracker.Lag(); got != 2*time.Second {
		t.Errorf("Lag() right after commit = %v, want 2s", got)
	}

	clock.Advance(3 * time.Second)
	if got := tracker.Lag(); got != 5*time.Second {
		t.Errorf("Lag() 3s later = %v, want 5s", got)
	}
}

func TestLagResetsToZeroWhenCaughtUp(t *testing.T) {
	clock := newFakeClock()
	tracker := newLagTracker(clock.Now)

	tracker.ObserveCommit(clock.Now().Add(-10 * time.Second))
	tracker.ObserveCaughtUp()
	clock.Advance(time.Minute)

	if got := tracker.Lag(); got != 0 {
		t.Errorf("Lag() while caught up = %v, want 0", got)
	}
}

func TestLagResumesAfterNewCommitFollowingCaughtUp(t *testing.T) {
	clock := newFakeClock()
	tracker := newLagTracker(clock.Now)

	tracker.ObserveCommit(clock.Now())
	tracker.ObserveCaughtUp()
	clock.Advance(time.Minute)
	tracker.ObserveCommit(clock.Now().Add(-4 * time.Second))

	if got := tracker.Lag(); got != 4*time.Second {
		t.Errorf("Lag() after new commit = %v, want 4s", got)
	}
}

func TestLagNeverNegativeWithSkewedCommitTimestamp(t *testing.T) {
	clock := newFakeClock()
	tracker := newLagTracker(clock.Now)

	tracker.ObserveCommit(clock.Now().Add(5 * time.Second))
	if got := tracker.Lag(); got != 0 {
		t.Errorf("Lag() with future commit ts = %v, want 0 (clamped)", got)
	}
}

func TestLagPublishUpdatesGauge(t *testing.T) {
	clock := newFakeClock()
	tracker := newLagTracker(clock.Now)

	tracker.ObserveCommit(clock.Now().Add(-1500 * time.Millisecond))
	tracker.Publish()
	if got := testutil.ToFloat64(metrics.LagSeconds); got != 1.5 {
		t.Errorf("cdc_lag_seconds = %v, want 1.5", got)
	}

	tracker.ObserveCaughtUp()
	if got := testutil.ToFloat64(metrics.LagSeconds); got != 0 {
		t.Errorf("cdc_lag_seconds after caught up = %v, want 0", got)
	}
}
