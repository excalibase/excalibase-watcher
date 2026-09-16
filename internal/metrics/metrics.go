package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_events_total",
			Help: "Total CDC events processed, by type",
		},
		[]string{"type"},
	)

	NATSPublished = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_nats_published_total",
			Help: "Total events published to NATS, by type",
		},
		[]string{"type"},
	)

	NATSErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "cdc_nats_errors_total",
			Help: "Total NATS publish errors",
		},
	)

	// LagSeconds is now minus the commit timestamp of the last COMMIT record the
	// watcher processed. It is reset to 0 once the primary reports the watcher
	// caught up (keepalive received with no event awaiting a NATS ack).
	LagSeconds = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "cdc_lag_seconds",
			Help: "Seconds between the last processed commit timestamp and now; 0 when caught up",
		},
	)

	SlotConfirmedFlushLSN = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "cdc_slot_confirmed_flush_lsn",
			Help: "confirmed_flush_lsn of the replication slot as a 64-bit integer",
		},
	)

	SlotRestartLSN = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "cdc_slot_restart_lsn",
			Help: "restart_lsn of the replication slot as a 64-bit integer",
		},
	)

	SlotRetainedWALBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "cdc_slot_retained_wal_bytes",
			Help: "WAL bytes retained by the slot: current WAL LSN minus restart_lsn",
		},
	)

	SlotsDropped = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "cdc_slots_dropped_total",
			Help: "Orphaned replication slots dropped by the cleanup routine",
		},
	)

	LastEventTimestampSeconds = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "cdc_last_event_timestamp_seconds",
			Help: "Unix time at which the watcher last processed a WAL data record",
		},
	)
)

func IncEvent(eventType string) {
	EventsTotal.WithLabelValues(eventType).Inc()
}

func IncNATSPublished(eventType string) {
	NATSPublished.WithLabelValues(eventType).Inc()
}

func IncNATSError() {
	NATSErrors.Inc()
}

func IncSlotDropped() {
	SlotsDropped.Inc()
}

func SetLagSeconds(seconds float64) {
	LagSeconds.Set(seconds)
}

// SetSlotStats publishes the slot LSN gauges. LSNs are exposed as float64,
// which is exact up to 2^53 (about 8 PB of WAL).
func SetSlotStats(confirmedFlushLSN, restartLSN, retainedWALBytes uint64) {
	SlotConfirmedFlushLSN.Set(float64(confirmedFlushLSN))
	SlotRestartLSN.Set(float64(restartLSN))
	SlotRetainedWALBytes.Set(float64(retainedWALBytes))
}

func SetLastEventTimestamp(at time.Time) {
	LastEventTimestampSeconds.Set(float64(at.UnixNano()) / 1e9)
}
