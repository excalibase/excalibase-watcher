package metrics

import (
	"testing"

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
