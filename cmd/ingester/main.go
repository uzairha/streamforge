// Command ingester subscribes to an event source, decodes each occurrence into
// a canonical ChainEvent, and (from M1) publishes it to the raw-events topic.
//
// M0: no Kafka yet. It runs the source and logs throughput so the pipeline is
// observable end to end from day one.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
	"github.com/uzairha/streamforge/internal/config"
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

	src, err := buildSource(cfg)
	if err != nil {
		log.Error("build source", "err", err)
		os.Exit(1)
	}
	log.Info("ingester starting", "source", src.Name(), "rate_per_sec", cfg.SyntheticRate)

	events := make(chan *streamforgev1.ChainEvent, 1024)
	errc := make(chan error, 1)
	go func() { errc <- src.Run(ctx, events) }()

	report := time.NewTicker(5 * time.Second)
	defer report.Stop()
	start := time.Now()
	done := ctx.Done()
	var count int64

	for {
		select {
		case _, ok := <-events:
			if !ok {
				if rerr := <-errc; rerr != nil {
					log.Error("source stopped with error", "err", rerr, "events_total", count)
					os.Exit(1)
				}
				log.Info("source drained, shutdown complete", "events_total", count)
				return
			}
			count++
		case <-report.C:
			elapsed := time.Since(start).Seconds()
			log.Info("throughput", "events_total", count, "events_per_sec", float64(count)/elapsed)
		case <-done:
			log.Info("signal received, draining source")
			done = nil // wait for the source to close the channel
		}
	}
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
