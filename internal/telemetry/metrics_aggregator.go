package telemetry

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// AggregatorMetrics holds the aggregator's Prometheus collectors on a
// private registry, so constructing it twice (e.g. in tests) never panics on
// duplicate registration.
type AggregatorMetrics struct {
	EventsConsumed prometheus.Counter
	EventsLate     prometheus.Counter     // dropped by window.Engine as arriving after their window closed
	WindowsClosed  *prometheus.CounterVec // by metric name
	StoreErrors    prometheus.Counter
	StoreSeconds   prometheus.Histogram   // time spent in UpsertAggregates per batch
	ProduceErrors  *prometheus.CounterVec // by topic, publishing closed windows to TOPIC_AGGREGATES
	CommitErrors   prometheus.Counter

	reg *prometheus.Registry
}

// NewAggregatorMetrics builds and registers the collectors, plus the
// standard Go and process collectors.
func NewAggregatorMetrics() *AggregatorMetrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	f := promauto.With(reg)

	return &AggregatorMetrics{
		reg: reg,
		EventsConsumed: f.NewCounter(prometheus.CounterOpts{
			Name: "streamforge_aggregator_events_consumed_total",
			Help: "Normalized events folded into a window.",
		}),
		EventsLate: f.NewCounter(prometheus.CounterOpts{
			Name: "streamforge_aggregator_events_late_total",
			Help: "Events dropped because their window had already closed.",
		}),
		WindowsClosed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "streamforge_aggregator_windows_closed_total",
			Help: "Aggregate rows emitted by a closed window, by metric name.",
		}, []string{"metric"}),
		StoreErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "streamforge_aggregator_store_errors_total",
			Help: "Failed TimescaleDB upsert batches.",
		}),
		StoreSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "streamforge_aggregator_store_seconds",
			Help:    "Time spent upserting one batch of closed windows.",
			Buckets: prometheus.DefBuckets,
		}),
		ProduceErrors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "streamforge_aggregator_produce_errors_total",
			Help: "Failed produce attempts publishing closed windows.",
		}, []string{"topic"}),
		CommitErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "streamforge_aggregator_commit_errors_total",
			Help: "Consumer group offset commit failures.",
		}),
	}
}

// Handler serves /metrics (Prometheus exposition) and /healthz (liveness).
func (m *AggregatorMetrics) Handler() http.Handler { return registryHandler(m.reg) }

// Serve runs the metrics/health HTTP server until ctx is cancelled, then
// shuts it down within a short grace period. It returns nil on a clean
// shutdown.
func (m *AggregatorMetrics) Serve(ctx context.Context, addr string) error {
	return serveHandler(ctx, addr, m.Handler())
}
