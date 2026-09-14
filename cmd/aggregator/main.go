// Command aggregator consumes normalized-events, folds each one into an
// event-time tumbling window (see internal/window), and — once a window
// closes — writes it to TimescaleDB and republishes it to the aggregates
// topic. The consumer offset is committed only after both writes succeed,
// so a crash mid-window redelivers the events that closed it rather than
// losing the aggregate; both sinks are safe to redeliver into (an idempotent
// upsert, and an at-least-once topic).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
	"github.com/uzairha/streamforge/internal/config"
	"github.com/uzairha/streamforge/internal/kafka"
	"github.com/uzairha/streamforge/internal/store"
	"github.com/uzairha/streamforge/internal/telemetry"
	"github.com/uzairha/streamforge/internal/window"
)

func main() {
	cfg, err := config.Load("aggregator")
	log := telemetry.NewLogger(cfg.LogLevel, "aggregator")
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metrics := telemetry.NewAggregatorMetrics()
	go func() {
		if serr := metrics.Serve(ctx, cfg.MetricsAddr); serr != nil {
			log.Error("metrics server", "err", serr)
		}
	}()
	log.Info("metrics + health listening", "addr", cfg.MetricsAddr)

	db, err := store.Open(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	if serr := db.EnsureSchema(ctx); serr != nil {
		log.Error("ensure schema", "err", serr)
		os.Exit(1)
	}

	producer, err := kafka.NewAggregateProducer(cfg.KafkaBrokers, cfg.TopicAggregates, func(topic string, perr error) {
		if perr != nil {
			metrics.ProduceErrors.WithLabelValues(topic).Inc()
		}
	})
	if err != nil {
		log.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	perr := producer.Ping(pingCtx)
	cancelPing()
	if perr != nil {
		log.Error("kafka unreachable", "brokers", cfg.KafkaBrokers, "err", perr)
		os.Exit(1)
	}

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.TopicNormalized, cfg.ConsumerGroup)
	if err != nil {
		log.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	defer consumer.Close()

	engine := window.NewEngine(cfg.WindowSize, cfg.AllowedLateness)

	log.Info("aggregator starting", "brokers", cfg.KafkaBrokers, "group", cfg.ConsumerGroup,
		"in", cfg.TopicNormalized, "out", cfg.TopicAggregates,
		"window", cfg.WindowSize.String(), "allowed_lateness", cfg.AllowedLateness.String())

	var consumed, windowsClosed int64
	report := time.NewTicker(10 * time.Second)
	defer report.Stop()
	go func() {
		for range report.C {
			log.Info("progress", "consumed_total", consumed, "windows_closed_total", windowsClosed)
		}
	}()

	runErr := consumer.Run(ctx, func(hctx context.Context, ev *streamforgev1.ChainEvent) error {
		consumed++
		metrics.EventsConsumed.Inc()

		closed, late := engine.Ingest(ev)
		if late {
			metrics.EventsLate.Inc()
		}
		if len(closed) == 0 {
			return nil
		}
		n, cerr := commitWindows(hctx, db, producer, metrics, closed)
		windowsClosed += n
		return cerr
	})
	var commitErr *kafka.CommitError
	if errors.As(runErr, &commitErr) {
		metrics.CommitErrors.Inc()
	}
	if runErr != nil {
		log.Error("consumer stopped with error", "err", runErr)
	}

	// A graceful shutdown still owes downstream the most recent, not-yet-
	// closed windows — otherwise the last WINDOW_SIZE+ALLOWED_LATENESS of
	// activity before every restart would simply vanish.
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	if final := engine.Flush(); len(final) > 0 {
		n, ferr := commitWindows(flushCtx, db, producer, metrics, final)
		windowsClosed += n
		if ferr != nil {
			log.Error("flush", "err", ferr)
		}
	}
	cancelFlush()

	log.Info("shutdown complete", "consumed_total", consumed, "windows_closed_total", windowsClosed)
	if runErr != nil {
		os.Exit(1)
	}
}

// commitWindows persists closed to TimescaleDB and republishes each
// aggregate to the aggregates topic, in that order. It returns how many
// aggregates it fully committed before any error, so a partial failure still
// counts what succeeded.
func commitWindows(
	ctx context.Context,
	db *store.Store,
	producer *kafka.AggregateProducer,
	metrics *telemetry.AggregatorMetrics,
	closed []*streamforgev1.Aggregate,
) (int64, error) {
	start := time.Now()
	err := db.UpsertAggregates(ctx, closed)
	metrics.StoreSeconds.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.StoreErrors.Inc()
		return 0, fmt.Errorf("upsert aggregates: %w", err)
	}

	var n int64
	for _, agg := range closed {
		if perr := producer.ProduceSync(ctx, agg); perr != nil {
			return n, fmt.Errorf("produce aggregate (chain=%s metric=%s): %w", agg.GetChain(), agg.GetMetric(), perr)
		}
		metrics.WindowsClosed.WithLabelValues(agg.GetMetric()).Inc()
		n++
	}
	return n, nil
}
