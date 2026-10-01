package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// TestConsumer_Integration_CommitsOnlyAfterHandlerSucceeds asserts the core
// checkpointing guarantee: if the handler fails partway through a fetch, Run
// stops without committing, so a fresh consumer in the same group sees every
// record again rather than losing the ones after the failure point.
func TestConsumer_Integration_CommitsOnlyAfterHandlerSucceeds(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()
	broker := startRedpanda(ctx, t)

	const (
		topic = "consumer-commit-test"
		group = "consumer-commit-test-group"
		want  = 20
	)
	produceN(ctx, t, broker, topic, want)

	failAt := 5
	var handled int
	c1, err := NewConsumer([]string{broker}, topic, group)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sentinel := errors.New("boom")
	runErr := c1.Run(ctx, func(_ context.Context, _ *streamforgev1.ChainEvent) error {
		handled++
		if handled == failAt {
			return sentinel
		}
		return nil
	})
	c1.Close()
	if !errors.Is(runErr, sentinel) {
		t.Fatalf("first Run error = %v, want sentinel", runErr)
	}

	c2, err := NewConsumer([]string{broker}, topic, group)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer c2.Close()

	var mu sync.Mutex
	seen := make(map[string]bool)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	err = c2.Run(runCtx, func(_ context.Context, ev *streamforgev1.ChainEvent) error {
		mu.Lock()
		seen[ev.GetId()] = true
		n := len(seen)
		mu.Unlock()
		if n == want {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if len(seen) != want {
		t.Fatalf("redelivered %d records, want %d (nothing should have been committed by the failed run)", len(seen), want)
	}
}

// TestConsumer_Integration_CommitAdvancesPastHandledRecords asserts the other
// half: once a fetch is fully handled and committed, a fresh consumer in the
// same group does not see those records again.
func TestConsumer_Integration_CommitAdvancesPastHandledRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()
	broker := startRedpanda(ctx, t)

	const (
		topic = "consumer-advance-test"
		group = "consumer-advance-test-group"
		want  = 10
	)
	produceN(ctx, t, broker, topic, want)

	c1, err := NewConsumer([]string{broker}, topic, group)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	var n1 int
	runCtx1, cancel1 := context.WithCancel(ctx)
	err = c1.Run(runCtx1, func(_ context.Context, _ *streamforgev1.ChainEvent) error {
		n1++
		if n1 == want {
			cancel1()
		}
		return nil
	})
	c1.Close()
	cancel1()
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if n1 != want {
		t.Fatalf("first run handled %d, want %d", n1, want)
	}

	c2, err := NewConsumer([]string{broker}, topic, group)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer c2.Close()

	pollCtx, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	var n2 int
	err = c2.Run(pollCtx, func(_ context.Context, _ *streamforgev1.ChainEvent) error {
		n2++
		return nil
	})
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("redelivered %d records after a full commit, want 0", n2)
	}
}

// TestConsumer_Integration_BeforeCommitFailureSkipsCommit asserts a failing
// WithBeforeCommit hook is treated like a handler failure: the fetch's
// records were all handled, but none are committed, so they are redelivered.
func TestConsumer_Integration_BeforeCommitFailureSkipsCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()
	broker := startRedpanda(ctx, t)

	const (
		topic = "consumer-before-commit-test"
		group = "consumer-before-commit-test-group"
		want  = 10
	)
	produceN(ctx, t, broker, topic, want)

	c1, err := NewConsumer([]string{broker}, topic, group)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sentinel := errors.New("batch not durable")
	var handled int
	runErr := c1.Run(ctx, func(_ context.Context, _ *streamforgev1.ChainEvent) error {
		handled++
		return nil
	}, WithBeforeCommit(func(context.Context) error { return sentinel }))
	c1.Close()
	if !errors.Is(runErr, sentinel) {
		t.Fatalf("first Run error = %v, want sentinel", runErr)
	}
	if handled == 0 {
		t.Fatal("handler never ran before the hook")
	}

	c2, err := NewConsumer([]string{broker}, topic, group)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer c2.Close()

	seen := make(map[string]bool)
	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	err = c2.Run(runCtx, func(_ context.Context, ev *streamforgev1.ChainEvent) error {
		seen[ev.GetId()] = true
		if len(seen) == want {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if len(seen) != want {
		t.Fatalf("redelivered %d records, want %d (the failed hook should have blocked the commit)", len(seen), want)
	}
}

// TestConsumer_Integration_ShutdownMidFetchIsClean asserts that a shutdown
// landing while a fetch is being handled stops Run with nil rather than the
// cancellation error, and still leaves that fetch uncommitted for the next
// run. It covers cancellation surfacing from the handler (the aggregator's
// ProduceSync) and from the before-commit hook (the normalizer's Batch.Wait).
func TestConsumer_Integration_ShutdownMidFetchIsClean(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()
	broker := startRedpanda(ctx, t)

	const want = 10
	cases := []struct {
		name string
		run  func(c *Consumer, runCtx context.Context, cancel context.CancelFunc) error
	}{
		{"handler", func(c *Consumer, runCtx context.Context, cancel context.CancelFunc) error {
			return c.Run(runCtx, func(hctx context.Context, _ *streamforgev1.ChainEvent) error {
				cancel()
				return fmt.Errorf("produce: %w", hctx.Err())
			})
		}},
		{"before_commit", func(c *Consumer, runCtx context.Context, cancel context.CancelFunc) error {
			return c.Run(runCtx, func(context.Context, *streamforgev1.ChainEvent) error {
				return nil
			}, WithBeforeCommit(func(wctx context.Context) error {
				cancel()
				return fmt.Errorf("produce batch: %w", wctx.Err())
			}))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			topic := "consumer-shutdown-" + tc.name
			group := topic + "-group"
			produceN(ctx, t, broker, topic, want)

			c1, err := NewConsumer([]string{broker}, topic, group)
			if err != nil {
				t.Fatalf("NewConsumer: %v", err)
			}
			runCtx, cancel := context.WithCancel(ctx)
			runErr := tc.run(c1, runCtx, cancel)
			cancel()
			c1.Close()
			if runErr != nil {
				t.Fatalf("Run after mid-fetch shutdown = %v, want nil", runErr)
			}

			c2, err := NewConsumer([]string{broker}, topic, group)
			if err != nil {
				t.Fatalf("NewConsumer: %v", err)
			}
			defer c2.Close()
			seen := make(map[string]bool)
			readCtx, stop := context.WithTimeout(ctx, 15*time.Second)
			defer stop()
			err = c2.Run(readCtx, func(_ context.Context, ev *streamforgev1.ChainEvent) error {
				seen[ev.GetId()] = true
				if len(seen) == want {
					stop()
				}
				return nil
			})
			if err != nil {
				t.Fatalf("second Run: %v", err)
			}
			if len(seen) != want {
				t.Fatalf("redelivered %d records, want %d (the interrupted fetch must not be committed)", len(seen), want)
			}
		})
	}
}

func startRedpanda(ctx context.Context, t *testing.T) string {
	t.Helper()
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
	return broker
}

func produceN(ctx context.Context, t *testing.T, broker, topic string, n int) {
	t.Helper()
	p, err := NewProducer([]string{broker}, topic, nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	for i := range n {
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
}
