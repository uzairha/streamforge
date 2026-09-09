package source

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand/v2"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// Synthetic emits a plausible ChainEvent stream at a fixed rate with no network
// access. It backs local demos, CI, and load tests, and lets every downstream
// stage be exercised without a live chain.
type Synthetic struct {
	rate   float64
	chains []string
	rng    *rand.Rand
	block  uint64
}

// NewSynthetic builds a generator emitting rate events/sec across the given
// chains (defaulting to a single "synthetic" chain).
func NewSynthetic(rate float64, chains []string) *Synthetic {
	return newSynthetic(rate, chains, rand.NewPCG(uint64(time.Now().UnixNano()), 0x9E3779B97F4A7C15))
}

func newSynthetic(rate float64, chains []string, src rand.Source) *Synthetic {
	if len(chains) == 0 {
		chains = []string{"synthetic"}
	}
	return &Synthetic{
		rate:   rate,
		chains: chains,
		rng:    rand.New(src),
		block:  20_000_000,
	}
}

// Name implements Source.
func (s *Synthetic) Name() string { return "synthetic" }

// Run implements Source.
func (s *Synthetic) Run(ctx context.Context, out chan<- *streamforgev1.ChainEvent) error {
	defer close(out)

	interval := time.Duration(float64(time.Second) / s.rate)
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			select {
			case out <- s.next():
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (s *Synthetic) next() *streamforgev1.ChainEvent {
	chain := s.chains[s.rng.IntN(len(s.chains))]
	now := time.Now()

	typ := streamforgev1.EventType_EVENT_TYPE_TRANSACTION
	if s.rng.IntN(12) == 0 { // ~1 block per 12 events
		s.block++
		typ = streamforgev1.EventType_EVENT_TYPE_BLOCK
	}

	ev := &streamforgev1.ChainEvent{
		Id:          fmt.Sprintf("evt:%s:%s", chain, s.randHex(16)),
		Chain:       chain,
		Type:        typ,
		BlockNumber: s.block,
		BlockHash:   s.randHex(32),
		ObservedAt:  timestamppb.New(now),
		BlockTime:   timestamppb.New(now.Add(-time.Duration(s.rng.IntN(12)) * time.Second)),
		Attributes:  map[string]string{},
	}

	if typ == streamforgev1.EventType_EVENT_TYPE_TRANSACTION {
		ev.TxHash = s.randHex(32)
		ev.FromAddress = s.randHex(20)
		ev.ToAddress = s.randHex(20)
		ev.ValueWei = new(big.Int).SetUint64(s.rng.Uint64() % 5_000_000_000_000_000_000).String()
		ev.GasUsed = 21_000 + uint64(s.rng.IntN(180_000))
	}
	return ev
}

func (s *Synthetic) randHex(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(s.rng.UintN(256))
	}
	return "0x" + hex.EncodeToString(b)
}
