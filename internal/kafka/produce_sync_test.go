package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// TestProducer_ProduceSync_RespectsContextCancellation asserts ProduceSync
// doesn't block forever if the caller's context ends before Kafka acks the
// record — it points at an address nothing is listening on, so the record
// can never be acked, and only ctx expiring can unblock the call.
func TestProducer_ProduceSync_RespectsContextCancellation(t *testing.T) {
	p, err := NewProducer([]string{"127.0.0.1:1"}, "raw-events", nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = p.ProduceSync(ctx, &streamforgev1.ChainEvent{Id: "evt:1", Chain: "test"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ProduceSync error = %v, want context.DeadlineExceeded", err)
	}
}

func TestAggregateProducer_ProduceSync_RespectsContextCancellation(t *testing.T) {
	p, err := NewAggregateProducer([]string{"127.0.0.1:1"}, "aggregates", nil)
	if err != nil {
		t.Fatalf("NewAggregateProducer: %v", err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = p.ProduceSync(ctx, &streamforgev1.Aggregate{Chain: "test", Metric: "events_total"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ProduceSync error = %v, want context.DeadlineExceeded", err)
	}
}
