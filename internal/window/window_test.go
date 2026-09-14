package window

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func ev(chain string, at time.Time, typ streamforgev1.EventType) *streamforgev1.ChainEvent {
	return &streamforgev1.ChainEvent{
		Id:        "evt:" + chain,
		Chain:     chain,
		Type:      typ,
		BlockTime: timestamppb.New(at),
	}
}

func txEv(chain string, at time.Time, gas uint64, valueWei, from string) *streamforgev1.ChainEvent {
	e := ev(chain, at, streamforgev1.EventType_EVENT_TYPE_TRANSACTION)
	e.GasUsed = gas
	e.ValueWei = valueWei
	e.FromAddress = from
	return e
}

func metric(aggs []*streamforgev1.Aggregate, name string, labels map[string]string) (*streamforgev1.Aggregate, bool) {
	for _, a := range aggs {
		if a.GetMetric() != name {
			continue
		}
		if labels == nil && len(a.GetLabels()) == 0 {
			return a, true
		}
		if labels != nil && mapsEqual(a.GetLabels(), labels) {
			return a, true
		}
	}
	return nil, false
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestEngine_WindowStaysOpenUntilWatermarkPasses(t *testing.T) {
	e := NewEngine(time.Minute, 0)

	closed, late := e.Ingest(ev("eth", epoch.Add(10*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	if late {
		t.Fatal("first event in a fresh chain must never be late")
	}
	if len(closed) != 0 {
		t.Fatalf("window closed prematurely: %v", closed)
	}

	// Same window (still < 60s), watermark unchanged relative to window end.
	closed, late = e.Ingest(ev("eth", epoch.Add(30*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	if late || len(closed) != 0 {
		t.Fatalf("window closed prematurely: closed=%v late=%v", closed, late)
	}
}

func TestEngine_ClosesOnceWatermarkPassesWindowEnd(t *testing.T) {
	e := NewEngine(time.Minute, 0)

	_, _ = e.Ingest(ev("eth", epoch.Add(10*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	_, _ = e.Ingest(ev("eth", epoch.Add(45*time.Second), streamforgev1.EventType_EVENT_TYPE_TRANSACTION))

	// Pushes the watermark to epoch+90s, past the first window's end (60s).
	closed, late := e.Ingest(ev("eth", epoch.Add(90*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	if late {
		t.Fatal("the event that advances the watermark is never itself late")
	}

	total, ok := metric(closed, "events_total", nil)
	if !ok {
		t.Fatalf("no events_total in closed windows: %v", closed)
	}
	if total.GetValue() != 2 {
		t.Errorf("events_total = %v, want 2 (only the first window's two events)", total.GetValue())
	}
	if got := total.GetWindowStart().AsTime(); !got.Equal(epoch) {
		t.Errorf("closed window start = %v, want %v", got, epoch)
	}
}

func TestEngine_LateEventAfterCloseIsDroppedNotDoubleCounted(t *testing.T) {
	e := NewEngine(time.Minute, 0)

	_, _ = e.Ingest(ev("eth", epoch.Add(10*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	closed, _ := e.Ingest(ev("eth", epoch.Add(90*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	if len(closed) == 0 {
		t.Fatal("setup: expected the first window to have closed")
	}

	// A straggler for the already-closed [0s,60s) window.
	closed, late := e.Ingest(ev("eth", epoch.Add(20*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	if !late {
		t.Error("straggler for an already-closed window should be reported late")
	}
	if len(closed) != 0 {
		t.Errorf("late event unexpectedly triggered another close: %v", closed)
	}
}

func TestEngine_AllowedLatenessAcceptsStragglers(t *testing.T) {
	e := NewEngine(time.Minute, 30*time.Second)

	_, _ = e.Ingest(ev("eth", epoch.Add(10*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	// Watermark now at 65s; deadline = 65s - 30s = 35s, still before the
	// first window's end (60s), so it must stay open.
	closed, late := e.Ingest(ev("eth", epoch.Add(65*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	if late || len(closed) != 0 {
		t.Fatalf("window closed too early under allowed lateness: closed=%v late=%v", closed, late)
	}

	// A straggler landing back in the first window is still accepted.
	closed, late = e.Ingest(ev("eth", epoch.Add(50*time.Second), streamforgev1.EventType_EVENT_TYPE_TRANSACTION))
	if late {
		t.Error("straggler within allowed lateness should not be marked late")
	}
	if len(closed) != 0 {
		t.Fatalf("unexpected close: %v", closed)
	}

	// Now push the watermark far enough that the deadline (120s-30s=90s)
	// passes the first window's end (60s). This event itself lands in a
	// later window (its own start is 120s), so the first window's total
	// reflects only its own two events (10s and the 50s straggler).
	closed, _ = e.Ingest(ev("eth", epoch.Add(120*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	total, ok := metric(closed, "events_total", nil)
	if !ok {
		t.Fatalf("expected the first window to close: %v", closed)
	}
	if total.GetValue() != 2 {
		t.Errorf("events_total = %v, want 2 (the 10s event plus the 50s straggler)", total.GetValue())
	}
}

func TestEngine_ChainsAreIndependent(t *testing.T) {
	e := NewEngine(time.Minute, 0)

	_, _ = e.Ingest(ev("eth", epoch.Add(10*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	closedEth, _ := e.Ingest(ev("eth", epoch.Add(90*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	if len(closedEth) == 0 {
		t.Fatal("eth window should have closed")
	}

	// A "sol" event at an early timestamp must not be considered late just
	// because eth's watermark has advanced far ahead.
	closedSol, late := e.Ingest(ev("sol", epoch.Add(5*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	if late {
		t.Error("a fresh chain's first window must never be late, regardless of other chains")
	}
	if len(closedSol) != 0 {
		t.Errorf("sol window closed unexpectedly: %v", closedSol)
	}
}

func TestEngine_Flush_EmitsRemainingOpenWindows(t *testing.T) {
	e := NewEngine(time.Minute, 0)

	_, _ = e.Ingest(ev("eth", epoch.Add(10*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))
	_, _ = e.Ingest(ev("sol", epoch.Add(5*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))

	flushed := e.Flush()

	byChain := map[string]bool{}
	for _, a := range flushed {
		if a.GetMetric() == "events_total" && len(a.GetLabels()) == 0 {
			byChain[a.GetChain()] = true
		}
	}
	if !byChain["eth"] || !byChain["sol"] {
		t.Fatalf("Flush() did not emit both open chains' windows: %v", flushed)
	}
}

func TestAccumulator_AggregatesContent(t *testing.T) {
	e := NewEngine(time.Minute, 0)

	_, _ = e.Ingest(txEv("eth", epoch.Add(5*time.Second), 21000, "1000000000000000000", "0xalice"))
	_, _ = e.Ingest(txEv("eth", epoch.Add(6*time.Second), 21000, "2000000000000000000", "0xbob"))
	_, _ = e.Ingest(txEv("eth", epoch.Add(7*time.Second), 50000, "500000000000000000", "0xalice")) // repeat sender
	_, _ = e.Ingest(ev("eth", epoch.Add(8*time.Second), streamforgev1.EventType_EVENT_TYPE_BLOCK))

	closed := e.Flush()

	if total, ok := metric(closed, "events_total", nil); !ok || total.GetValue() != 4 {
		t.Errorf("events_total = %v, ok=%v, want 4", total, ok)
	}
	if txCount, ok := metric(closed, "events_total", map[string]string{"type": "EVENT_TYPE_TRANSACTION"}); !ok || txCount.GetValue() != 3 {
		t.Errorf("events_total{type=TRANSACTION} = %v, ok=%v, want 3", txCount, ok)
	}
	if gas, ok := metric(closed, "gas_used_total", nil); !ok || gas.GetValue() != 92000 {
		t.Errorf("gas_used_total = %v, ok=%v, want 92000", gas, ok)
	}
	if val, ok := metric(closed, "value_wei_total", nil); !ok || val.GetValue() != 3.5e18 {
		t.Errorf("value_wei_total = %v, ok=%v, want 3.5e18", val, ok)
	}
	if addr, ok := metric(closed, "active_addresses", nil); !ok || addr.GetValue() != 2 {
		t.Errorf("active_addresses = %v, ok=%v, want 2 (alice, bob)", addr, ok)
	}
}

func TestAccumulator_MalformedValueWeiIsIgnoredNotFatal(t *testing.T) {
	e := NewEngine(time.Minute, 0)
	_, _ = e.Ingest(txEv("eth", epoch.Add(5*time.Second), 0, "not-a-number", "0xalice"))

	closed := e.Flush()
	if val, ok := metric(closed, "value_wei_total", nil); !ok || val.GetValue() != 0 {
		t.Errorf("value_wei_total = %v, ok=%v, want 0 for an unparseable value", val, ok)
	}
}
