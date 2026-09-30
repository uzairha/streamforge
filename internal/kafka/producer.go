// Package kafka wraps the franz-go client with StreamForge's conventions:
// protobuf-encoded values, an idempotent acks=all producer, and a partition
// key chosen to spread load while keeping a sender's events ordered.
package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/protobuf/proto"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// eventAttrs describes a ChainEvent on a span. Chain and type are bounded
// label sets; the event id is not, so it stays off the attribute list.
func eventAttrs(ev *streamforgev1.ChainEvent) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("streamforge.chain", ev.GetChain()),
		attribute.String("streamforge.event_type", ev.GetType().String()),
	}
}

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
func NewProducer(brokers []string, topic string, onResult OnResult, tuning ...Tuning) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka: no brokers configured")
	}
	if topic == "" {
		return nil, fmt.Errorf("kafka: empty topic")
	}
	if onResult == nil {
		onResult = func(string, error) {}
	}

	opts := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
		kgo.ClientID("streamforge-ingester"),
	}, resolveTuning(tuning).producerOpts()...)

	client, err := kgo.NewClient(opts...)
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
//
// The span ends in the delivery callback rather than on return, so it measures
// time to broker acknowledgement instead of time to buffer the record. The
// ingester calls this with a background context (records must outlive the
// shutdown signal), which makes this span the root of the pipeline's trace.
func (p *Producer) Produce(ctx context.Context, ev *streamforgev1.ChainEvent) error {
	return p.produce(ctx, ev, nil)
}

// produce is Produce with an optional extra callback, run after OnResult
// with the same delivery outcome.
func (p *Producer) produce(ctx context.Context, ev *streamforgev1.ChainEvent, then func(error)) error {
	value, err := proto.Marshal(ev)
	if err != nil {
		return fmt.Errorf("kafka: marshal event %s: %w", ev.GetId(), err)
	}
	rec := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(partitionKey(ev)),
		Value: value,
	}

	ctx, span := tracer().Start(ctx, p.topic+" publish",
		producerSpanOpts(p.topic, eventAttrs(ev)...)...)
	injectTrace(ctx, rec)

	p.inFlight.Add(1)
	p.client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		p.inFlight.Add(-1)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "produce failed")
		}
		span.End()
		p.onResult(p.topic, err)
		if then != nil {
			then(err)
		}
	})
	return nil
}

// Batch groups asynchronously produced records so the caller can wait for
// all of them at once. It gives a whole consumer fetch the guarantee
// ProduceSync gives one record — nothing is committed until every record is
// durable — for the cost of a single wait instead of one round trip per
// record. Records keep their per-key order: the producer is idempotent, so
// retries never reorder a partition.
type Batch struct {
	p   *Producer
	wg  sync.WaitGroup
	mu  sync.Mutex
	err error
}

// NewBatch returns an empty Batch that produces through p.
func (p *Producer) NewBatch() *Batch {
	return &Batch{p: p}
}

// Produce enqueues ev like Producer.Produce and adds it to the batch. It
// returns an error only for local failures; delivery failures surface from
// Wait.
func (b *Batch) Produce(ctx context.Context, ev *streamforgev1.ChainEvent) error {
	id := ev.GetId()
	b.wg.Add(1)
	err := b.p.produce(ctx, ev, func(err error) {
		if err != nil {
			b.mu.Lock()
			if b.err == nil {
				b.err = fmt.Errorf("kafka: produce event %s: %w", id, err)
			}
			b.mu.Unlock()
		}
		b.wg.Done()
	})
	if err != nil {
		b.wg.Done()
	}
	return err
}

// Wait blocks until Kafka has acknowledged or rejected every record produced
// since the last Wait, then returns the first delivery failure (nil if all
// succeeded) and resets the batch for reuse. If ctx ends first, Wait returns
// ctx.Err() with records possibly still outstanding; the Batch must not be
// reused after that.
func (b *Batch) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	b.mu.Lock()
	err := b.err
	b.err = nil
	b.mu.Unlock()
	return err
}

// ProduceSync enqueues ev like Produce, but blocks until Kafka acknowledges
// or rejects it and returns that outcome directly, instead of only via
// OnResult. Use it when the caller must not proceed — e.g. commit the
// consumer offset that triggered ev — until this specific record is
// durable.
func (p *Producer) ProduceSync(ctx context.Context, ev *streamforgev1.ChainEvent) error {
	value, err := proto.Marshal(ev)
	if err != nil {
		return fmt.Errorf("kafka: marshal event %s: %w", ev.GetId(), err)
	}
	rec := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(partitionKey(ev)),
		Value: value,
	}

	ctx, span := tracer().Start(ctx, p.topic+" publish",
		producerSpanOpts(p.topic, eventAttrs(ev)...)...)
	defer span.End()
	injectTrace(ctx, rec)

	p.inFlight.Add(1)
	done := make(chan error, 1)
	p.client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		p.inFlight.Add(-1)
		p.onResult(p.topic, err)
		done <- err
	})
	select {
	case err := <-done:
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "produce failed")
		}
		return err
	case <-ctx.Done():
		span.RecordError(ctx.Err())
		span.SetStatus(codes.Error, "context cancelled")
		return ctx.Err()
	}
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
