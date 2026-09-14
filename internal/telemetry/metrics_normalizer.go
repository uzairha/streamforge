package telemetry

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// NormalizerMetrics holds the normalizer's Prometheus collectors on a
// private registry, so constructing it twice (e.g. in tests) never panics on
// duplicate registration.
type NormalizerMetrics struct {
	EventsConsumed *prometheus.CounterVec // by topic
	EventsInvalid  *prometheus.CounterVec // by reason, rejected by normalize.Validate
	EventsProduced *prometheus.CounterVec // by topic
	ProduceErrors  *prometheus.CounterVec // by topic
	CommitErrors   prometheus.Counter

	reg *prometheus.Registry
}

// NewNormalizerMetrics builds and registers the collectors, plus the
// standard Go and process collectors.
func NewNormalizerMetrics() *NormalizerMetrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	f := promauto.With(reg)

	return &NormalizerMetrics{
		reg: reg,
		EventsConsumed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "streamforge_normalizer_events_consumed_total",
			Help: "Records read from the input topic.",
		}, []string{"topic"}),
		EventsInvalid: f.NewCounterVec(prometheus.CounterOpts{
			Name: "streamforge_normalizer_events_invalid_total",
			Help: "Events rejected by normalize.Validate, by reason.",
		}, []string{"reason"}),
		EventsProduced: f.NewCounterVec(prometheus.CounterOpts{
			Name: "streamforge_normalizer_events_produced_total",
			Help: "Enriched records acknowledged by Kafka.",
		}, []string{"topic"}),
		ProduceErrors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "streamforge_normalizer_produce_errors_total",
			Help: "Failed produce attempts (enqueue or delivery).",
		}, []string{"topic"}),
		CommitErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "streamforge_normalizer_commit_errors_total",
			Help: "Consumer group offset commit failures.",
		}),
	}
}

// Handler serves /metrics (Prometheus exposition) and /healthz (liveness).
func (m *NormalizerMetrics) Handler() http.Handler { return registryHandler(m.reg) }

// Serve runs the metrics/health HTTP server until ctx is cancelled, then
// shuts it down within a short grace period. It returns nil on a clean
// shutdown.
func (m *NormalizerMetrics) Serve(ctx context.Context, addr string) error {
	return serveHandler(ctx, addr, m.Handler())
}
