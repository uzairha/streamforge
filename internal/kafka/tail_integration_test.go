package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// TestTailConsumer_Integration_OnlySeesEventsProducedAfterConnecting asserts
// the AtEnd starting position: records already in the topic before Tail
// connects must not be redelivered, matching a live-tail API's semantics.
func TestTailConsumer_Integration_OnlySeesEventsProducedAfterConnecting(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()
	broker := startRedpanda(ctx, t)
	const topic = "tail-atend-test"

	produceN(ctx, t, broker, topic, 5) // pre-existing records, must be skipped

	tail, err := NewTailConsumer([]string{broker}, topic)
	if err != nil {
		t.Fatalf("NewTailConsumer: %v", err)
	}
	defer tail.Close()

	out := make(chan *streamforgev1.ChainEvent, 16)
	tailCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- tail.Tail(tailCtx, out) }()

	// Give the tail a moment to actually reach AtEnd before producing the
	// records it's meant to see.
	time.Sleep(500 * time.Millisecond)

	p, err := NewProducer([]string{broker}, topic, nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	for i := range 3 {
		ev := &streamforgev1.ChainEvent{Id: fmt.Sprintf("evt:new:%d", i), Chain: "test"}
		if perr := p.ProduceSync(ctx, ev); perr != nil {
			t.Fatalf("ProduceSync: %v", perr)
		}
	}

	var seen []string
	timeout := time.After(15 * time.Second)
	for len(seen) < 3 {
		select {
		case ev := <-out:
			seen = append(seen, ev.GetId())
		case <-timeout:
			t.Fatalf("timed out waiting for 3 events, got %v", seen)
		}
	}
	for _, id := range seen {
		if id == "evt:test:0" {
			t.Fatalf("saw a pre-existing record %q; AtEnd should have skipped it", id)
		}
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Tail: %v", err)
	}
}

// TestTailConsumer_Integration_TwoTailsBothSeeEverything asserts the
// fan-out property: TailConsumer does not join a consumer group, so two
// independent tails on the same topic each get every record, unlike two
// members of one group splitting the partitions between them.
func TestTailConsumer_Integration_TwoTailsBothSeeEverything(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()
	broker := startRedpanda(ctx, t)
	const topic = "tail-fanout-test"

	// The topic must exist before a TailConsumer's metadata refresh can find
	// it; produce and discard one throwaway record to create it (mirroring
	// how, in production, the ingester's AllowAutoTopicCreation producer
	// always creates a topic well before any API server tails it).
	produceN(ctx, t, broker, topic, 1)

	tailA, err := NewTailConsumer([]string{broker}, topic)
	if err != nil {
		t.Fatalf("NewTailConsumer A: %v", err)
	}
	defer tailA.Close()
	tailB, err := NewTailConsumer([]string{broker}, topic)
	if err != nil {
		t.Fatalf("NewTailConsumer B: %v", err)
	}
	defer tailB.Close()

	outA := make(chan *streamforgev1.ChainEvent, 16)
	outB := make(chan *streamforgev1.ChainEvent, 16)
	ctxA, cancelA := context.WithCancel(ctx)
	defer cancelA()
	ctxB, cancelB := context.WithCancel(ctx)
	defer cancelB()
	go func() { _ = tailA.Tail(ctxA, outA) }()
	go func() { _ = tailB.Tail(ctxB, outB) }()

	time.Sleep(500 * time.Millisecond)
	produceN(ctx, t, broker, topic, 5)

	countA := drainN(t, outA, 5, 15*time.Second)
	countB := drainN(t, outB, 5, 15*time.Second)
	if countA != 5 {
		t.Errorf("tail A saw %d events, want 5", countA)
	}
	if countB != 5 {
		t.Errorf("tail B saw %d events, want 5", countB)
	}
}

func drainN(t *testing.T, ch <-chan *streamforgev1.ChainEvent, n int, timeout time.Duration) int {
	t.Helper()
	deadline := time.After(timeout)
	count := 0
	for count < n {
		select {
		case <-ch:
			count++
		case <-deadline:
			return count
		}
	}
	return count
}
