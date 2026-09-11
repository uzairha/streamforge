package source

import "testing"

func TestSeenSet_FirstOccurrenceIsNew(t *testing.T) {
	s := newSeenSet(4)
	if s.seen("a") {
		t.Fatal("seen(a) = true on first call, want false")
	}
}

func TestSeenSet_RepeatIsSeen(t *testing.T) {
	s := newSeenSet(4)
	s.seen("a")
	if !s.seen("a") {
		t.Fatal("seen(a) = false on repeat, want true")
	}
}

func TestSeenSet_EvictsOldestBeyondLimit(t *testing.T) {
	s := newSeenSet(2)
	s.seen("a")
	s.seen("b")
	s.seen("c") // evicts "a"

	// Checking a repeat doesn't itself evict anything, so check the survivors
	// before the evicted key — re-checking "a" is itself a fresh insert.
	if !s.seen("b") {
		t.Fatal("seen(b) = false, want true (still within window)")
	}
	if !s.seen("c") {
		t.Fatal("seen(c) = false, want true (still within window)")
	}
	if s.seen("a") {
		t.Fatal("seen(a) = true, want false (should have been evicted)")
	}
}

func TestSeenSet_ZeroOrNegativeLimitStillWorks(t *testing.T) {
	s := newSeenSet(0)
	if s.seen("a") {
		t.Fatal("seen(a) = true on first call, want false")
	}
	if !s.seen("a") {
		t.Fatal("seen(a) = false on repeat, want true")
	}
	if s.seen("b") {
		t.Fatal("seen(b) = true on first call, want false")
	}
	if !s.seen("b") {
		t.Fatal("seen(b) = false on repeat, want true")
	}
}
