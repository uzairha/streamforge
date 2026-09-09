// Command ingester subscribes to an event source, decodes each occurrence into
// a canonical ChainEvent, and publishes it to the raw-events topic. It exposes
// Prometheus metrics and a /healthz endpoint on METRICS_ADDR.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
	"github.com/uzairha/streamforge/internal/config"
	"github.com/uzairha/streamforge/internal/kafka"
	"github.com/uzairha/streamforge/internal/source"
	"github.com/uzairha/streamforge/internal/telemetry"
)

func main() {
	cfg, err := config.Load("ingester")
	log := telemetry.NewLogger(cfg.LogLevel, "ingester")
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metrics := telemetry.NewMetrics()
	go func() {
		if serr := metrics.Serve(ctx, cfg.MetricsAddr); serr != nil {
			log.Error("metrics server", "err", serr)
		}
	}()
	log.Info("metrics + health listening", "addr", cfg.MetricsAddr)

	src, err := buildSource(cfg)
	if err != nil {
		log.Error("build source", "err", err)
		os.Exit(1)
	}

	producer, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.TopicRaw, func(topic string, perr error) {
		if perr != nil {
			metrics.ProduceErrors.WithLabelValues(topic).Inc()
			log.Error("produce failed", "topic", topic, "err", perr)
			return
		}
		metrics.EventsProduced.WithLabelValues(topic).Inc()
	})
	if err != nil {
		log.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	if perr := producer.Ping(pingCtx); perr != nil {
		cancelPing()
		log.Error("kafka unreachable", "brokers", cfg.KafkaBrokers, "err", perr)
		os.Exit(1)
	}
	cancelPing()

	log.Info("ingester starting", "source", src.Name(), "topic", cfg.TopicRaw, "brokers", cfg.KafkaBrokers)
	metrics.SourceUp.Set(1)

	events := make(chan *streamforgev1.ChainEvent, 1024)
	srcErr := make(chan error, 1)
	go func() { srcErr <- src.Run(ctx, events) }()

	report := time.NewTicker(10 * time.Second)
	defer report.Stop()
	var ingested int64

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				metrics.SourceUp.Set(0)
				if rerr := <-srcErr; rerr != nil {
					log.Error("source stopped with error", "err", rerr)
				}
				shutdown(log, producer, metrics, cfg.ShutdownTimeout, ingested)
				return
			}
			ingested++
			metrics.EventsIngested.WithLabelValues(ev.GetChain(), ev.GetType().String()).Inc()
			// Delivery is async; a background context keeps records buffered
			// through shutdown so Flush can drain them.
			if perr := producer.Produce(context.Background(), ev); perr != nil {
				metrics.ProduceErrors.WithLabelValues(cfg.TopicRaw).Inc()
				log.Error("produce enqueue", "err", perr)
			}
		case <-report.C:
			log.Info("progress", "ingested_total", ingested, "in_flight", producer.InFlight())
		}
	}
}

func shutdown(log *slog.Logger, p *kafka.Producer, m *telemetry.Metrics, budget time.Duration, ingested int64) {
	flushCtx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	start := time.Now()
	if ferr := p.Flush(flushCtx); ferr != nil {
		log.Error("flush on shutdown", "err", ferr, "in_flight", p.InFlight())
	}
	m.FlushSeconds.Observe(time.Since(start).Seconds())
	log.Info("shutdown complete", "ingested_total", ingested, "flush_ms", time.Since(start).Milliseconds())
}

func buildSource(cfg config.Config) (source.Source, error) {
	switch cfg.Source {
	case "synthetic":
		return source.NewSynthetic(cfg.SyntheticRate, cfg.SyntheticChains), nil
	default: // "ethereum" is validated by config but implemented in M2
		return nil, &notImplementedError{what: "source " + cfg.Source + " (lands in M2)"}
	}
}

type notImplementedError struct{ what string }

func (e *notImplementedError) Error() string { return "not implemented: " + e.what }
