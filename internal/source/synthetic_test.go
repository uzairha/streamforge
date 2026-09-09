package source

import (
	"context"
	"math/big"
	"math/rand/v2"
	"testing"
	"time"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

func drain(t *testing.T, rate float64, chains []string, d time.Duration) []*streamforgev1.ChainEvent {
	t.Helper()
	s := newSynthetic(rate, chains, rand.NewPCG(1, 2))

	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	out := make(chan *streamforgev1.ChainEvent, 4096)
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx, out) }()

	var got []*streamforgev1.ChainEvent
	for ev := range out {
		got = append(got, ev)
	}
	if err := <-errc; err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	return got
}

func TestSynthetic_ClosesChannelOnCancel(t *testing.T) {
	s := newSynthetic(500, nil, rand.NewPCG(1, 2))
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *streamforgev1.ChainEvent)
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx, out) }()

	<-out // prove it is producing
	cancel()

	// Drain until closed; the whole thing must settle quickly.
	done := make(chan struct{})
	go func() {
		for range out { //nolint:revive // intentional drain
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed within 2s of cancel")
	}
	if err := <-errc; err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
}

func TestSynthetic_ApproxRate(t *testing.T) {
	got := drain(t, 200, nil, 300*time.Millisecond)
	// 200/s over ~300ms is ~60 events; allow a wide band for scheduler jitter.
	if len(got) < 15 || len(got) > 150 {
		t.Fatalf("got %d events, want roughly 60 (15..150)", len(got))
	}
}

func TestSynthetic_EventsWellFormed(t *testing.T) {
	got := drain(t, 1000, []string{"synthetic"}, 200*time.Millisecond)
	if len(got) == 0 {
		t.Fatal("no events produced")
	}
	for i, ev := range got {
		if ev.GetId() == "" || ev.GetChain() == "" || ev.GetBlockHash() == "" {
			t.Fatalf("event %d missing core fields: %+v", i, ev)
		}
		switch ev.GetType() {
		case streamforgev1.EventType_EVENT_TYPE_BLOCK:
			// block-level: no tx fields expected
		case streamforgev1.EventType_EVENT_TYPE_TRANSACTION:
			if ev.GetTxHash() == "" || ev.GetFromAddress() == "" || ev.GetToAddress() == "" {
				t.Fatalf("tx event %d missing tx fields: %+v", i, ev)
			}
			if _, ok := new(big.Int).SetString(ev.GetValueWei(), 10); !ok {
				t.Fatalf("tx event %d has unparseable value_wei %q", i, ev.GetValueWei())
			}
		default:
			t.Fatalf("event %d has unexpected type %v", i, ev.GetType())
		}
	}
}

func TestSynthetic_BlockNumberMonotonic(t *testing.T) {
	got := drain(t, 1000, nil, 200*time.Millisecond)
	var prev uint64
	for i, ev := range got {
		if ev.GetBlockNumber() < prev {
			t.Fatalf("event %d block %d < previous %d", i, ev.GetBlockNumber(), prev)
		}
		prev = ev.GetBlockNumber()
	}
}
