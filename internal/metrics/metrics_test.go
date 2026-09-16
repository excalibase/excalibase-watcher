package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestIncEvent(t *testing.T) {
	IncEvent("INSERT")
	IncEvent("INSERT")
	IncEvent("DELETE")

	if v := testutil.ToFloat64(EventsTotal.WithLabelValues("INSERT")); v != 2 {
		t.Errorf("INSERT count = %f, want 2", v)
	}
	if v := testutil.ToFloat64(EventsTotal.WithLabelValues("DELETE")); v != 1 {
		t.Errorf("DELETE count = %f, want 1", v)
	}
}

func TestIncNATSPublished(t *testing.T) {
	IncNATSPublished("INSERT")
	if v := testutil.ToFloat64(NATSPublished.WithLabelValues("INSERT")); v < 1 {
		t.Errorf("NATS published count = %f, want >= 1", v)
	}
}

func TestIncNATSError(t *testing.T) {
	before := testutil.ToFloat64(NATSErrors)
	IncNATSError()
	after := testutil.ToFloat64(NATSErrors)
	if after != before+1 {
		t.Errorf("NATS errors = %f, want %f", after, before+1)
	}
}

func TestSetLagSeconds(t *testing.T) {
	SetLagSeconds(2.5)
	if v := testutil.ToFloat64(LagSeconds); v != 2.5 {
		t.Errorf("cdc_lag_seconds = %f, want 2.5", v)
	}
}

func TestSetSlotStats(t *testing.T) {
	SetSlotStats(1000, 800, 200)
	if v := testutil.ToFloat64(SlotConfirmedFlushLSN); v != 1000 {
		t.Errorf("cdc_slot_confirmed_flush_lsn = %f, want 1000", v)
	}
	if v := testutil.ToFloat64(SlotRestartLSN); v != 800 {
		t.Errorf("cdc_slot_restart_lsn = %f, want 800", v)
	}
	if v := testutil.ToFloat64(SlotRetainedWALBytes); v != 200 {
		t.Errorf("cdc_slot_retained_wal_bytes = %f, want 200", v)
	}
}

func TestSetLastEventTimestamp(t *testing.T) {
	at := time.Unix(1_700_000_000, 500_000_000)
	SetLastEventTimestamp(at)
	if v := testutil.ToFloat64(LastEventTimestampSeconds); v != 1_700_000_000.5 {
		t.Errorf("cdc_last_event_timestamp_seconds = %f, want 1700000000.5", v)
	}
}

func TestIncSlotDropped(t *testing.T) {
	before := testutil.ToFloat64(SlotsDropped)
	IncSlotDropped()
	if got := testutil.ToFloat64(SlotsDropped); got != before+1 {
		t.Errorf("cdc_slots_dropped_total = %f, want %f", got, before+1)
	}
}
