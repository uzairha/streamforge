// Package kafka wraps the franz-go client with StreamForge's conventions:
// protobuf-encoded values, an idempotent acks=all producer, and a partition
// key chosen to spread load while keeping a sender's events ordered.
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

// OnResult is called once per record with the destination topic and the produce
// outcome (nil on success). It runs on a franz-go internal goroutine, so it must
// not block.
type OnResult func(topic string, err error)

// Producer publishes ChainEvents to a single topic. Records are produced
// asynchronously; call Flush before Close to drain in-flight records.
type Producer struct {
	client   *kgo.Client
	topic    string
	onResult OnResult
	inFlight atomic.Int64
}

// NewProducer connects an idempotent, acks=all producer to brokers.
func NewProducer(brokers []string, topic string, onResult OnResult) (*Producer, error) {
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
		kgo.ClientID("streamforge-ingester"),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	return &Producer{client: client, topic: topic, onResult: onResult}, nil
}

// Ping verifies broker connectivity.
func (p *Producer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

// Produce enqueues ev for asynchronous delivery. It returns an error only for
// local failures (marshalling); delivery outcomes arrive via OnResult.
func (p *Producer) Produce(ctx context.Context, ev *streamforgev1.ChainEvent) error {
	value, err := proto.Marshal(ev)
	if err != nil {
		return fmt.Errorf("kafka: marshal event %s: %w", ev.GetId(), err)
	}
	rec := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(partitionKey(ev)),
		Value: value,
	}
	p.inFlight.Add(1)
	p.client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		p.inFlight.Add(-1)
		p.onResult(p.topic, err)
	})
	return nil
}

// InFlight reports records enqueued but not yet acknowledged.
func (p *Producer) InFlight() int64 { return p.inFlight.Load() }

// Flush blocks until every buffered record is acknowledged or ctx is done.
func (p *Producer) Flush(ctx context.Context) error {
	return p.client.Flush(ctx)
}

// Close closes the underlying client. Call Flush first to avoid losing records.
func (p *Producer) Close() {
	p.client.Close()
}

// partitionKey spreads records across partitions by sender address, falling back
// to the block hash and then the chain name. Same-sender transactions keep their
// relative order because they share a key.
func partitionKey(ev *streamforgev1.ChainEvent) string {
	if k := ev.GetFromAddress(); k != "" {
		return k
	}
	if k := ev.GetBlockHash(); k != "" {
		return k
	}
	return ev.GetChain()
}
