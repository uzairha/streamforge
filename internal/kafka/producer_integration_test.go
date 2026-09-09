package kafka

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// TestProducer_Integration_ProduceAndConsume produces protobuf ChainEvents to a
// real Redpanda container and reads them back, asserting count and decodability.
func TestProducer_Integration_ProduceAndConsume(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()

	container, err := tcredpanda.Run(ctx, "redpandadata/redpanda:v24.2.7",
		tcredpanda.WithAutoCreateTopics())
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})
	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("seed broker: %v", err)
	}

	const (
		topic = "raw-events-test"
		want  = 50
	)

	var acked atomic.Int64
	var lastErr atomic.Value
	p, err := NewProducer([]string{broker}, topic, func(_ string, err error) {
		if err != nil {
			lastErr.Store(err)
			return
		}
		acked.Add(1)
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	for i := range want {
		ev := &streamforgev1.ChainEvent{
			Id:          fmt.Sprintf("evt:test:%d", i),
			Chain:       "test",
			Type:        streamforgev1.EventType_EVENT_TYPE_TRANSACTION,
			BlockNumber: uint64(1000 + i),
			BlockHash:   fmt.Sprintf("0xblock%02d", i),
			FromAddress: fmt.Sprintf("0xfrom%d", i%5),
		}
		if err := p.Produce(ctx, ev); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	flushCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := p.Flush(flushCtx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := acked.Load(); got != want {
		t.Fatalf("acked = %d, want %d (last produce error: %v)", got, want, lastErr.Load())
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

	readCtx, cancelRead := context.WithTimeout(ctx, 30*time.Second)
	defer cancelRead()

	seen := make(map[string]bool, want)
	for len(seen) < want {
		fetches := consumer.PollFetches(readCtx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("poll: %v", err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var ev streamforgev1.ChainEvent
			if err := proto.Unmarshal(r.Value, &ev); err != nil {
				t.Fatalf("unmarshal record: %v", err)
			}
			if ev.GetChain() != "test" {
				t.Fatalf("decoded chain = %q, want test", ev.GetChain())
			}
			seen[ev.GetId()] = true
		})
	}
	if len(seen) != want {
		t.Fatalf("consumed %d distinct events, want %d", len(seen), want)
	}
}
