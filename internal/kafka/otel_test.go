package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func init() {
	// The carrier is only meaningful with a propagator installed; the global
	// default is a no-op that writes nothing.
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

func sampledContext(t *testing.T) (context.Context, trace.TraceID) {
	t.Helper()
	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatalf("parse trace id: %v", err)
	}
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatalf("parse span id: %v", err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	return trace.ContextWithSpanContext(context.Background(), sc), traceID
}

func TestRecordCarrierRoundTrip(t *testing.T) {
	ctx, want := sampledContext(t)

	rec := &kgo.Record{Topic: "raw-events"}
	injectTrace(ctx, rec)

	if len(rec.Headers) == 0 {
		t.Fatal("inject wrote no headers; a trace cannot cross the topic")
	}

	got := trace.SpanContextFromContext(extractTrace(context.Background(), rec))
	if !got.IsValid() {
		t.Fatal("extracted span context is invalid")
	}
	if got.TraceID() != want {
		t.Errorf("trace id = %s, want %s", got.TraceID(), want)
	}
	if !got.IsSampled() {
		t.Error("sampling decision was not carried across the record")
	}
}

// Re-injecting must overwrite, not append: two traceparent headers on one
// record would leave the consumer's choice of parent undefined.
func TestRecordCarrierSetReplaces(t *testing.T) {
	ctx, _ := sampledContext(t)

	rec := &kgo.Record{Topic: "raw-events"}
	injectTrace(ctx, rec)
	injectTrace(ctx, rec)

	seen := map[string]int{}
	for _, h := range rec.Headers {
		seen[h.Key]++
	}
	for key, n := range seen {
		if n != 1 {
			t.Errorf("header %q appears %d times, want 1", key, n)
		}
	}
}

func TestRecordCarrierKeysAndGet(t *testing.T) {
	rec := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: "traceparent", Value: []byte("abc")},
		{Key: "other", Value: []byte("def")},
	}}
	c := recordCarrier{rec}

	if got := c.Get("traceparent"); got != "abc" {
		t.Errorf("Get(traceparent) = %q, want %q", got, "abc")
	}
	if got := c.Get("missing"); got != "" {
		t.Errorf("Get(missing) = %q, want empty", got)
	}
	if got := len(c.Keys()); got != 2 {
		t.Errorf("len(Keys()) = %d, want 2", got)
	}
}

// A record with no trace headers must still yield a usable context rather than
// failing — events produced before tracing was switched on still flow through.
func TestExtractWithoutHeaders(t *testing.T) {
	rec := &kgo.Record{Topic: "raw-events"}
	ctx := extractTrace(context.Background(), rec)
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("expected no valid span context from a record with no headers")
	}
}
