package telemetry

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// APIMetrics holds the api service's Prometheus collectors on a private
// registry, so constructing it twice (e.g. in tests) never panics on
// duplicate registration.
type APIMetrics struct {
	RequestsTotal      *prometheus.CounterVec // by rpc method, grpc status code
	AuthFailuresTotal  prometheus.Counter
	StreamEventsActive prometheus.Gauge // StreamEvents calls currently open

	reg *prometheus.Registry
}

// NewAPIMetrics builds and registers the collectors, plus the standard Go
// and process collectors.
func NewAPIMetrics() *APIMetrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	f := promauto.With(reg)

	return &APIMetrics{
		reg: reg,
		RequestsTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "streamforge_api_requests_total",
			Help: "RPCs handled, by method and grpc status code.",
		}, []string{"method", "code"}),
		AuthFailuresTotal: f.NewCounter(prometheus.CounterOpts{
			Name: "streamforge_api_auth_failures_total",
			Help: "Requests rejected for a missing or invalid API key.",
		}),
		StreamEventsActive: f.NewGauge(prometheus.GaugeOpts{
			Name: "streamforge_api_stream_events_active",
			Help: "StreamEvents calls currently connected.",
		}),
	}
}

// Handler serves /metrics (Prometheus exposition) and /healthz (liveness).
func (m *APIMetrics) Handler() http.Handler { return registryHandler(m.reg) }

// Serve runs the metrics/health HTTP server until ctx is cancelled, then
// shuts it down within a short grace period. It returns nil on a clean
// shutdown.
func (m *APIMetrics) Serve(ctx context.Context, addr string) error {
	return serveHandler(ctx, addr, m.Handler())
}
