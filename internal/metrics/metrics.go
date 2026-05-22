package metrics

import (
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
