package source

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"
)

func TestBackoff_NeverExceedsMax(t *testing.T) {
	b := newBackoffFrom(2*time.Second, rand.NewPCG(1, 2))
	for i := 0; i < 50; i++ {
		if d := b.duration(); d < 0 || d > 2*time.Second {
			t.Fatalf("duration() = %v, want in [0, 2s]", d)
		}
	}
}

func TestBackoff_CeilingGrowsThenCaps(t *testing.T) {
	b := newBackoffFrom(time.Hour, rand.NewPCG(1, 2))
	// Jitter makes any single draw noisy, so compare against the deterministic
	// ceiling each call is drawn under (mirroring duration()'s own math),
	// which must be non-decreasing until it hits the cap.
	var prevCeil time.Duration
	for i := 0; i < 10; i++ {
		shift := i
		if shift > 20 {
			shift = 20
		}
		ceil := backoffBase * time.Duration(int64(1)<<uint(shift))
		if ceil > time.Hour {
			ceil = time.Hour
		}
		b.duration()
		if ceil < prevCeil {
			t.Fatalf("ceiling decreased at i=%d: %v < %v", i, ceil, prevCeil)
		}
		prevCeil = ceil
	}
}

func TestBackoff_ResetReturnsToBase(t *testing.T) {
	b := newBackoffFrom(time.Hour, rand.NewPCG(1, 2))
	for i := 0; i < 30; i++ {
		b.duration()
	}
	b.reset()
	if d := b.duration(); d > backoffBase {
		t.Fatalf("duration() right after reset = %v, want <= base %v", d, backoffBase)
	}
}

func TestBackoff_SleepRespectsCancellation(t *testing.T) {
	b := newBackoffFrom(time.Hour, rand.NewPCG(1, 2))
	for i := 0; i < 40; i++ {
		b.duration() // push the ceiling out far past the test timeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if b.sleep(ctx) {
		t.Fatal("sleep() = true, want false on ctx cancellation")
	}
}

func TestBackoff_SleepReturnsTrueOnElapse(t *testing.T) {
	b := newBackoffFrom(5*time.Millisecond, rand.NewPCG(1, 2))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if !b.sleep(ctx) {
		t.Fatal("sleep() = false, want true when the delay elapses before ctx does")
	}
}
