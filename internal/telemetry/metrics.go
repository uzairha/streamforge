package telemetry

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the ingester's Prometheus collectors on a private registry, so
// constructing it twice (e.g. in tests) never panics on duplicate registration.
type Metrics struct {
	EventsIngested   *prometheus.CounterVec // by chain, type
	EventsProduced   *prometheus.CounterVec // by topic
	ProduceErrors    *prometheus.CounterVec // by topic
	FlushSeconds     prometheus.Histogram
	SourceUp         prometheus.Gauge
	SourceReconnects prometheus.Counter
	EventsDeduped    prometheus.Counter

	reg *prometheus.Registry
}

// NewMetrics builds and registers the collectors, plus the standard Go and
// process collectors.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	f := promauto.With(reg)

	return &Metrics{
		reg: reg,
		EventsIngested: f.NewCounterVec(prometheus.CounterOpts{
			Name: "streamforge_events_ingested_total",
			Help: "ChainEvents received from the source.",
		}, []string{"chain", "type"}),
		EventsProduced: f.NewCounterVec(prometheus.CounterOpts{
			Name: "streamforge_events_produced_total",
			Help: "Records acknowledged by Kafka.",
		}, []string{"topic"}),
		ProduceErrors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "streamforge_produce_errors_total",
			Help: "Failed produce attempts (enqueue or delivery).",
		}, []string{"topic"}),
		FlushSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "streamforge_producer_flush_seconds",
			Help:    "Time spent flushing the producer on shutdown.",
			Buckets: prometheus.DefBuckets,
		}),
		SourceUp: f.NewGauge(prometheus.GaugeOpts{
			Name: "streamforge_source_up",
			Help: "1 while the event source is connected, 0 otherwise.",
		}),
		SourceReconnects: f.NewCounter(prometheus.CounterOpts{
			Name: "streamforge_source_reconnects_total",
			Help: "Reconnect attempts made by the event source after a dropped connection.",
		}),
		EventsDeduped: f.NewCounter(prometheus.CounterOpts{
			Name: "streamforge_events_deduped_total",
			Help: "Events suppressed by the source as duplicates of one already seen.",
		}),
	}
}

// Handler serves /metrics (Prometheus exposition) and /healthz (liveness).
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	return mux
}

// Serve runs the metrics/health HTTP server until ctx is cancelled, then shuts
// it down within a short grace period. It returns nil on a clean shutdown.
func (m *Metrics) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           m.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
