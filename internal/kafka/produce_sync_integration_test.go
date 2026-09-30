package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// TestProducer_Integration_ProduceSyncIsDurableOnReturn asserts that once
// ProduceSync returns nil, the record is already readable — the guarantee
// the normalizer relies on before committing a raw-events offset.
func TestProducer_Integration_ProduceSyncIsDurableOnReturn(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()
	broker := startRedpanda(ctx, t)

	const topic = "produce-sync-test"
	p, err := NewProducer([]string{broker}, topic, nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	produceCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := p.ProduceSync(produceCtx, &streamforgev1.ChainEvent{Id: "evt:sync:1", Chain: "test"}); err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}

	assertOneRecordReadable(ctx, t, broker, topic)
}

// TestAggregateProducer_Integration_ProduceSyncIsDurableOnReturn is the same
// guarantee for the aggregator's Aggregate topic.
func TestAggregateProducer_Integration_ProduceSyncIsDurableOnReturn(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()
	broker := startRedpanda(ctx, t)

	const topic = "aggregate-produce-sync-test"
	p, err := NewAggregateProducer([]string{broker}, topic, nil)
	if err != nil {
		t.Fatalf("NewAggregateProducer: %v", err)
	}
	defer p.Close()

	produceCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	agg := &streamforgev1.Aggregate{Chain: "test", Metric: "events_total", Value: 42}
	if err := p.ProduceSync(produceCtx, agg); err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}

	assertOneRecordReadable(ctx, t, broker, topic)
}

func assertOneRecordReadable(ctx context.Context, t *testing.T, broker, topic string) {
	t.Helper()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	readCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	fetches := consumer.PollFetches(readCtx)
	if err := fetches.Err(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	var n int
	fetches.EachRecord(func(r *kgo.Record) {
		n++
		if len(r.Value) == 0 {
			t.Error("empty record value")
		}
	})
	if n == 0 {
		t.Fatal("no records readable after ProduceSync returned")
	}
}

// TestBatch_Integration_RecordsAreDurableOnWait is the batch counterpart of
// ProduceSyncIsDurableOnReturn: once Wait returns nil, every record in the
// batch is readable.
func TestBatch_Integration_RecordsAreDurableOnWait(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()
	broker := startRedpanda(ctx, t)

	const (
		topic = "batch-durable-test"
		want  = 50
	)
	p, err := NewProducer([]string{broker}, topic, nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	b := p.NewBatch()
	for i := range want {
		ev := &streamforgev1.ChainEvent{Id: fmt.Sprintf("evt:batch:%d", i), Chain: "test"}
		if err := b.Produce(ctx, ev); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := b.Wait(waitCtx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if n := p.InFlight(); n != 0 {
		t.Fatalf("InFlight after Wait = %d, want 0", n)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	readCtx, cancelRead := context.WithTimeout(ctx, 15*time.Second)
	defer cancelRead()
	var n int
	for n < want && readCtx.Err() == nil {
		fetches := consumer.PollFetches(readCtx)
		if readCtx.Err() == nil {
			if err := fetches.Err(); err != nil {
				t.Fatalf("poll: %v", err)
			}
		}
		n += fetches.NumRecords()
	}
	if n != want {
		t.Fatalf("read %d records after Wait returned, want %d", n, want)
	}
}
