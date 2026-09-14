package kafka

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// AggregateProducer publishes closed-window Aggregates to a single topic.
// Unlike Producer, it is always synchronous: the aggregator must know a
// window's Kafka delivery succeeded before it commits the normalized-events
// offset that produced it, so there is no async, fire-and-forget mode here.
type AggregateProducer struct {
	client   *kgo.Client
	topic    string
	onResult OnResult
	inFlight atomic.Int64
}

// NewAggregateProducer connects an idempotent, acks=all producer to brokers.
func NewAggregateProducer(brokers []string, topic string, onResult OnResult) (*AggregateProducer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka: no brokers configured")
	}
	if topic == "" {
		return nil, fmt.Errorf("kafka: empty topic")
	}
	if onResult == nil {
		onResult = func(string, error) {}
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.AllowAutoTopicCreation(),
		kgo.ClientID("streamforge-aggregator"),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	return &AggregateProducer{client: client, topic: topic, onResult: onResult}, nil
}

// Ping verifies broker connectivity.
func (p *AggregateProducer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

// Close closes the underlying client.
func (p *AggregateProducer) Close() {
	p.client.Close()
}

// InFlight reports records enqueued but not yet acknowledged.
func (p *AggregateProducer) InFlight() int64 { return p.inFlight.Load() }

// ProduceSync enqueues agg and blocks until Kafka acknowledges or rejects
// it, returning that outcome directly (and, as a side effect, via OnResult).
func (p *AggregateProducer) ProduceSync(ctx context.Context, agg *streamforgev1.Aggregate) error {
	value, err := proto.Marshal(agg)
	if err != nil {
		return fmt.Errorf("kafka: marshal aggregate (chain=%s metric=%s): %w", agg.GetChain(), agg.GetMetric(), err)
	}
	rec := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(agg.GetChain()),
		Value: value,
	}
	p.inFlight.Add(1)
	done := make(chan error, 1)
	p.client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		p.inFlight.Add(-1)
		p.onResult(p.topic, err)
		done <- err
	})
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
