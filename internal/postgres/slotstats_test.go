package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/excalibase/watcher-go/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestParseLSNText(t *testing.T) {
	cases := []struct {
		in      string
		want    uint64
		wantErr bool
	}{
		{"0/0", 0, false},
		{"0/16B3D80", 0x16B3D80, false},
		{"1/0", 1 << 32, false},
		{"FFFFFFFF/FFFFFFFF", ^uint64(0), false},
		{"", 0, false},
		{"garbage", 0, true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := parseLSNText(c.in)
			if (err != nil) != c.wantErr {
				t.Fatalf("parseLSNText(%q) err=%v wantErr=%v", c.in, err, c.wantErr)
			}
			if got != c.want {
				t.Errorf("parseLSNText(%q) = %d, want %d", c.in, got, c.want)
			}
		})
	}
}

func TestRetainedWALBytes(t *testing.T) {
	cases := []struct {
		name  string
		stats SlotStats
		want  uint64
	}{
		{"primary ahead of restart", SlotStats{RestartLSN: 100, WALLSN: 350}, 250},
		{"equal", SlotStats{RestartLSN: 100, WALLSN: 100}, 0},
		{"restart ahead (clock skew or standby)", SlotStats{RestartLSN: 400, WALLSN: 350}, 0},
		{"no restart lsn", SlotStats{RestartLSN: 0, WALLSN: 350}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.stats.RetainedWALBytes(); got != c.want {
				t.Errorf("RetainedWALBytes() = %d, want %d", got, c.want)
			}
		})
	}
}

func TestSampleOncePublishesSlotGauges(t *testing.T) {
	clock := newFakeClock()
	lag := newLagTracker(clock.Now)
	lag.ObserveCommit(clock.Now().Add(-7 * time.Second))

	sampler := newSlotSampler(time.Hour, lag, func(context.Context) (SlotStats, error) {
		return SlotStats{ConfirmedFlushLSN: 1000, RestartLSN: 800, WALLSN: 1300, Active: true}, nil
	})

	if err := sampler.sampleOnce(context.Background()); err != nil {
		t.Fatalf("sampleOnce: %v", err)
	}

	assertGauge(t, "cdc_slot_confirmed_flush_lsn", metrics.SlotConfirmedFlushLSN, 1000)
	assertGauge(t, "cdc_slot_restart_lsn", metrics.SlotRestartLSN, 800)
	assertGauge(t, "cdc_slot_retained_wal_bytes", metrics.SlotRetainedWALBytes, 500)
	assertGauge(t, "cdc_lag_seconds", metrics.LagSeconds, 7)
}

func TestSampleOncePropagatesQueryErrorButStillRefreshesLag(t *testing.T) {
	clock := newFakeClock()
	lag := newLagTracker(clock.Now)
	lag.ObserveCommit(clock.Now().Add(-3 * time.Second))

	sampler := newSlotSampler(time.Hour, lag, func(context.Context) (SlotStats, error) {
		return SlotStats{}, errors.New("connection refused")
	})

	if err := sampler.sampleOnce(context.Background()); err == nil {
		t.Fatal("expected query error")
	}
	assertGauge(t, "cdc_lag_seconds", metrics.LagSeconds, 3)
}

func TestSamplerRunSamplesOnTicker(t *testing.T) {
	var calls atomic.Int32
	sampler := newSlotSampler(10*time.Millisecond, newLagTracker(time.Now), func(context.Context) (SlotStats, error) {
		calls.Add(1)
		return SlotStats{}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sampler.run(ctx)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for calls.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("sampler ran %d times, want >= 3", calls.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sampler did not stop on context cancel")
	}
}

func TestSamplerRunKeepsGoingAfterQueryError(t *testing.T) {
	var calls atomic.Int32
	sampler := newSlotSampler(10*time.Millisecond, newLagTracker(time.Now), func(context.Context) (SlotStats, error) {
		calls.Add(1)
		return SlotStats{}, errors.New("db hibernated")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	sampler.run(ctx)

	if calls.Load() < 2 {
		t.Errorf("sampler stopped after error: %d calls", calls.Load())
	}
}

func assertGauge(t *testing.T, name string, collector prometheus.Collector, want float64) {
	t.Helper()
	got := testutil.ToFloat64(collector)
	if got != want {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}
