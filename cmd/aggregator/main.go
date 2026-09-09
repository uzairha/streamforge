// Command aggregator consumes normalized-events, computes windowed metrics, and
// writes closed windows to the aggregates topic and TimescaleDB. Skeleton in
// M0; the windowing engine lands in M3.
package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/uzairha/streamforge/internal/config"
	"github.com/uzairha/streamforge/internal/telemetry"
)

func main() {
	cfg, err := config.Load("aggregator")
	log := telemetry.NewLogger(cfg.LogLevel, "aggregator")
	if err != nil {
		log.Error("load config", "err", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("aggregator starting", "brokers", cfg.KafkaBrokers, "group", cfg.ConsumerGroup,
		"in", cfg.TopicNormalized, "out", cfg.TopicAggregates, "window", cfg.WindowSize.String())
	log.Warn("aggregator is an M0 skeleton; windowing engine lands in M3")

	<-ctx.Done()
	log.Info("aggregator stopped")
}
