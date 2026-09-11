package source

import (
	"context"
	"math/rand/v2"
	"time"
)

// backoffBase is the smallest reconnect delay; duration() grows exponentially
// from here up to a caller-supplied ceiling.
const backoffBase = 250 * time.Millisecond

// backoff computes reconnect delays with exponential growth and full jitter
// (a delay is drawn uniformly from [0, ceiling], not just added around it),
// capped at max. reset() drops the exponent back to zero once a connection
// has proven stable.
type backoff struct {
	base time.Duration
	max  time.Duration
	n    int
	rng  *rand.Rand
}

func newBackoff(maxDelay time.Duration) *backoff {
	return newBackoffFrom(maxDelay, rand.NewPCG(uint64(time.Now().UnixNano()), 0xD1B54A32D192ED03))
}

func newBackoffFrom(maxDelay time.Duration, src rand.Source) *backoff {
	if maxDelay <= 0 {
		maxDelay = backoffBase
	}
	return &backoff{base: backoffBase, max: maxDelay, rng: rand.New(src)}
}

func (b *backoff) reset() { b.n = 0 }

// duration returns the next delay and advances the exponent.
func (b *backoff) duration() time.Duration {
	shift := b.n
	if shift > 20 { // cap so base<<shift can never overflow int64
		shift = 20
	}
	ceil := b.base * time.Duration(int64(1)<<uint(shift))
	if ceil <= 0 || ceil > b.max {
		ceil = b.max
	}
	b.n++
	return time.Duration(b.rng.Int64N(int64(ceil) + 1))
}

// sleep waits out one backoff interval, returning false if ctx is cancelled
// first.
func (b *backoff) sleep(ctx context.Context) bool {
	t := time.NewTimer(b.duration())
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
