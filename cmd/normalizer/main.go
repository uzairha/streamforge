// Command normalizer consumes raw-events, validates and enriches each event,
// and republishes it to normalized-events. Its consumer offset is committed
// only once the enriched record has been durably produced, so a crash
// between consuming and producing redelivers the record rather than losing
// it. An invalid record is dropped (counted, not retried) since retrying
// can't fix a malformed one.
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
	"github.com/uzairha/streamforge/internal/normalize"
	"github.com/uzairha/streamforge/internal/telemetry"
)

func main() {
	cfg, err := config.Load("normalizer")
	log := telemetry.NewLogger(cfg.LogLevel, "normalizer")
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metrics := telemetry.NewNormalizerMetrics()
	go func() {
		if serr := metrics.Serve(ctx, cfg.MetricsAddr); serr != nil {
			log.Error("metrics server", "err", serr)
		}
	}()
	log.Info("metrics + health listening", "addr", cfg.MetricsAddr)

	producer, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.TopicNormalized, func(topic string, perr error) {
		if perr != nil {
			metrics.ProduceErrors.WithLabelValues(topic).Inc()
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
	perr := producer.Ping(pingCtx)
	cancelPing()
	if perr != nil {
		log.Error("kafka unreachable", "brokers", cfg.KafkaBrokers, "err", perr)
		os.Exit(1)
	}

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.TopicRaw, cfg.ConsumerGroup)
	if err != nil {
		log.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	defer consumer.Close()

	log.Info("normalizer starting", "brokers", cfg.KafkaBrokers, "group", cfg.ConsumerGroup,
		"in", cfg.TopicRaw, "out", cfg.TopicNormalized)

	var consumed, produced, invalid int64
	report := time.NewTicker(10 * time.Second)
	defer report.Stop()
	go func() {
		for range report.C {
			log.Info("progress", "consumed_total", consumed, "produced_total", produced, "invalid_total", invalid)
		}
	}()

	runErr := consumer.Run(ctx, func(hctx context.Context, raw *streamforgev1.ChainEvent) error {
		consumed++
		metrics.EventsConsumed.WithLabelValues(cfg.TopicRaw).Inc()

		if verr := normalize.Validate(raw); verr != nil {
			invalid++
			metrics.EventsInvalid.WithLabelValues(invalidReason(verr)).Inc()
			log.Warn("dropping invalid event", "id", raw.GetId(), "err", verr)
			return nil
		}

		enriched := normalize.Enrich(raw)
		if perr := producer.ProduceSync(hctx, enriched); perr != nil {
			return fmt.Errorf("produce normalized event %s: %w", enriched.GetId(), perr)
		}
		produced++
		return nil
	})
	var commitErr *kafka.CommitError
	if errors.As(runErr, &commitErr) {
		metrics.CommitErrors.Inc()
	}
	if runErr != nil {
		log.Error("consumer stopped with error", "err", runErr)
		os.Exit(1)
	}

	log.Info("shutdown complete", "consumed_total", consumed, "produced_total", produced, "invalid_total", invalid)
}

// invalidReason maps a normalize.Validate failure to a short, low-cardinality
// label for the EventsInvalid metric.
func invalidReason(err error) string {
	switch {
	case errors.Is(err, normalize.ErrMissingID):
		return "missing_id"
	case errors.Is(err, normalize.ErrMissingChain):
		return "missing_chain"
	case errors.Is(err, normalize.ErrUnspecifiedType):
		return "unspecified_type"
	case errors.Is(err, normalize.ErrMissingBlockTime):
		return "missing_block_time"
	case errors.Is(err, normalize.ErrMissingTxHash):
		return "missing_tx_hash"
	default:
		return "other"
	}
}
