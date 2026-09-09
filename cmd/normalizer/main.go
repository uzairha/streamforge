// Command normalizer consumes raw-events, validates and enriches each event,
// and republishes to normalized-events. Skeleton in M0; the consumer-group
// pipeline lands in M3.
package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/uzairha/streamforge/internal/config"
	"github.com/uzairha/streamforge/internal/telemetry"
)

func main() {
	cfg, err := config.Load("normalizer")
	log := telemetry.NewLogger(cfg.LogLevel, "normalizer")
	if err != nil {
		log.Error("load config", "err", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("normalizer starting", "brokers", cfg.KafkaBrokers, "group", cfg.ConsumerGroup,
		"in", cfg.TopicRaw, "out", cfg.TopicNormalized)
	log.Warn("normalizer is an M0 skeleton; consumer pipeline lands in M3")

	<-ctx.Done()
	log.Info("normalizer stopped")
}
