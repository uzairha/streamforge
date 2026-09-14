// Package window computes event-time tumbling windows over ChainEvents. A
// window is keyed by each event's own block_time, not by when the aggregator
// happened to receive it, so a burst of delayed delivery or a brief
// reordering of the stream doesn't split one block's worth of activity
// across two emitted windows.
package window

import (
	"math/big"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// Engine tracks open windows per chain and closes them as each chain's
// watermark advances. It is not safe for concurrent use — call Ingest and
// Flush from a single goroutine, as the aggregator does between consuming a
// record and committing its offset.
type Engine struct {
	size            time.Duration
	allowedLateness time.Duration
	chains          map[string]*chainState
}

type chainState struct {
	watermark  time.Time              // latest block_time seen for this chain
	closedUpTo time.Time              // furthest window end already emitted
	open       map[int64]*accumulator // window start (UnixNano) -> accumulator
}

// NewEngine builds an Engine with the given tumbling window size and allowed
// lateness — how far past a window's end an event with a block_time inside
// that window is still accepted before the window is closed and emitted.
func NewEngine(size, allowedLateness time.Duration) *Engine {
	return &Engine{
		size:            size,
		allowedLateness: allowedLateness,
		chains:          make(map[string]*chainState),
	}
}

// Ingest folds ev into the window its block_time belongs to and returns any
// windows (for ev's chain) that closed as a result — usually none, since a
// window only closes once a later event pushes the watermark past it. late
// reports whether ev's window had already closed by the time ev arrived, in
// which case ev was dropped rather than accumulated.
func (e *Engine) Ingest(ev *streamforgev1.ChainEvent) (closed []*streamforgev1.Aggregate, late bool) {
	t := ev.GetBlockTime().AsTime().UTC()
	chain := ev.GetChain()

	cs, ok := e.chains[chain]
	if !ok {
		cs = &chainState{open: make(map[int64]*accumulator)}
		e.chains[chain] = cs
	}
	if t.After(cs.watermark) {
		cs.watermark = t
	}

	start := t.Truncate(e.size)
	end := start.Add(e.size)

	if !cs.closedUpTo.IsZero() && !end.After(cs.closedUpTo) {
		// This event's window has already been closed and emitted.
		return e.closeReady(chain, cs), true
	}

	acc, ok := cs.open[start.UnixNano()]
	if !ok {
		acc = newAccumulator(start, end)
		cs.open[start.UnixNano()] = acc
	}
	acc.add(ev)

	return e.closeReady(chain, cs), false
}

// closeReady emits and removes every open window for chain whose end has
// fallen at or behind the chain's watermark (less allowedLateness), and
// advances closedUpTo past them so later stragglers are recognized as late.
func (e *Engine) closeReady(chain string, cs *chainState) []*streamforgev1.Aggregate {
	deadline := cs.watermark.Add(-e.allowedLateness)

	var starts []int64
	for start, acc := range cs.open {
		if !acc.end.After(deadline) {
			starts = append(starts, start)
		}
	}
	return e.drain(chain, cs, starts)
}

// Flush closes every remaining open window across all chains regardless of
// the watermark, so a graceful shutdown doesn't lose the most recent,
// still-open windows. The Engine should be discarded afterward.
func (e *Engine) Flush() []*streamforgev1.Aggregate {
	var out []*streamforgev1.Aggregate
	for chain, cs := range e.chains {
		starts := make([]int64, 0, len(cs.open))
		for start := range cs.open {
			starts = append(starts, start)
		}
		out = append(out, e.drain(chain, cs, starts)...)
	}
	return out
}

func (e *Engine) drain(chain string, cs *chainState, starts []int64) []*streamforgev1.Aggregate {
	if len(starts) == 0 {
		return nil
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })

	out := make([]*streamforgev1.Aggregate, 0, len(starts))
	for _, start := range starts {
		acc := cs.open[start]
		out = append(out, acc.aggregates(chain)...)
		delete(cs.open, start)
		if acc.end.After(cs.closedUpTo) {
			cs.closedUpTo = acc.end
		}
	}
	return out
}

// accumulator holds one chain's in-progress totals for a single window.
type accumulator struct {
	start, end    time.Time
	eventsByType  map[string]int64
	gasUsedTotal  uint64
	valueWeiTotal *big.Int
	addresses     map[string]struct{}
}

func newAccumulator(start, end time.Time) *accumulator {
	return &accumulator{
		start:         start,
		end:           end,
		eventsByType:  make(map[string]int64),
		valueWeiTotal: new(big.Int),
		addresses:     make(map[string]struct{}),
	}
}

func (a *accumulator) add(ev *streamforgev1.ChainEvent) {
	a.eventsByType[ev.GetType().String()]++
	a.gasUsedTotal += ev.GetGasUsed()
	if v, ok := new(big.Int).SetString(ev.GetValueWei(), 10); ok {
		a.valueWeiTotal.Add(a.valueWeiTotal, v)
	}
	if from := ev.GetFromAddress(); from != "" {
		a.addresses[from] = struct{}{}
	}
}

// aggregates renders the accumulator into one Aggregate per metric: an
// events_total per event type plus its sum, gas_used_total, value_wei_total
// (float64-approximated — the schema's Aggregate.value is a double, so very
// large wei sums lose precision beyond 2^53; exact-value auditing should read
// value_wei on the underlying ChainEvents, not this aggregate), and
// active_addresses (distinct from_address).
func (a *accumulator) aggregates(chain string) []*streamforgev1.Aggregate {
	start := timestamppb.New(a.start)
	end := timestamppb.New(a.end)

	types := make([]string, 0, len(a.eventsByType))
	for typ := range a.eventsByType {
		types = append(types, typ)
	}
	sort.Strings(types)

	out := make([]*streamforgev1.Aggregate, 0, len(types)+4)
	var total int64
	for _, typ := range types {
		n := a.eventsByType[typ]
		total += n
		out = append(out, &streamforgev1.Aggregate{
			Chain: chain, Metric: "events_total",
			WindowStart: start, WindowEnd: end,
			Value:  float64(n),
			Labels: map[string]string{"type": typ},
		})
	}
	out = append(out,
		&streamforgev1.Aggregate{
			Chain: chain, Metric: "events_total",
			WindowStart: start, WindowEnd: end,
			Value: float64(total),
		},
		&streamforgev1.Aggregate{
			Chain: chain, Metric: "gas_used_total",
			WindowStart: start, WindowEnd: end,
			Value: float64(a.gasUsedTotal),
		},
		&streamforgev1.Aggregate{
			Chain: chain, Metric: "value_wei_total",
			WindowStart: start, WindowEnd: end,
			Value: valueWeiFloat(a.valueWeiTotal),
		},
		&streamforgev1.Aggregate{
			Chain: chain, Metric: "active_addresses",
			WindowStart: start, WindowEnd: end,
			Value: float64(len(a.addresses)),
		},
	)
	return out
}

func valueWeiFloat(v *big.Int) float64 {
	f, _ := new(big.Float).SetInt(v).Float64()
	return f
}
