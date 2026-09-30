package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

func TestBatch_WaitOnEmptyBatchReturnsNil(t *testing.T) {
	p, err := NewProducer([]string{"127.0.0.1:1"}, "normalized-events", nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.NewBatch().Wait(ctx); err != nil {
		t.Fatalf("Wait on empty batch = %v, want nil", err)
	}
}

// TestBatch_WaitRespectsContextCancellation is ProduceSync's cancellation
// guarantee for a batch: records that can never be acked must not block
// Wait past its context.
func TestBatch_WaitRespectsContextCancellation(t *testing.T) {
	p, err := NewProducer([]string{"127.0.0.1:1"}, "normalized-events", nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	b := p.NewBatch()
	for _, id := range []string{"evt:1", "evt:2", "evt:3"} {
		if err := b.Produce(context.Background(), &streamforgev1.ChainEvent{Id: id, Chain: "test"}); err != nil {
			t.Fatalf("Produce %s: %v", id, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := b.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait error = %v, want context.DeadlineExceeded", err)
	}
}

// TestBatch_WaitReportsDeliveryFailure asserts a record Kafka never accepts
// surfaces from Wait instead of being counted as delivered. The record's own
// context is already cancelled, so franz-go fails it without a broker.
func TestBatch_WaitReportsDeliveryFailure(t *testing.T) {
	p, err := NewProducer([]string{"127.0.0.1:1"}, "normalized-events", nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	recCtx, cancelRec := context.WithCancel(context.Background())
	cancelRec()

	b := p.NewBatch()
	if err := b.Produce(recCtx, &streamforgev1.ChainEvent{Id: "evt:1", Chain: "test"}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = b.Wait(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait error = %v, want the record's context.Canceled", err)
	}
}
