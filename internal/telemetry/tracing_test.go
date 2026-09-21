package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

// Tracing off must cost nothing and touch no global state: call sites start
// spans unconditionally, so a disabled build has to yield non-recording spans
// rather than requiring a guard at every span.
func TestInitTracingDisabled(t *testing.T) {
	shutdown, err := InitTracing(context.Background(), TracingConfig{Enabled: false})
	if err != nil {
		t.Fatalf("InitTracing(disabled) returned error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown func is nil; callers defer it unconditionally")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown returned error: %v", err)
	}

	_, span := Tracer("test").Start(context.Background(), "noop")
	defer span.End()
	if span.IsRecording() {
		t.Error("span is recording with tracing disabled")
	}
}

func TestTracerAlwaysReturnsUsableTracer(t *testing.T) {
	tr := Tracer("github.com/uzairha/streamforge/internal/telemetry")
	if tr == nil {
		t.Fatal("Tracer returned nil")
	}
	ctx, span := tr.Start(context.Background(), "unit")
	span.End()
	if _, ok := ctx.Deadline(); ok {
		t.Error("tracer unexpectedly attached a deadline to the context")
	}
	if !span.SpanContext().IsValid() && span.IsRecording() {
		t.Error("recording span has an invalid span context")
	}
}

func TestEventAttributes(t *testing.T) {
	attrs := EventAttributes("ethereum", "TRANSACTION")
	if len(attrs) != 2 {
		t.Fatalf("len(attrs) = %d, want 2", len(attrs))
	}
	if got := string(attrs[0].Key); got != "streamforge.chain" {
		t.Errorf("attrs[0].Key = %q", got)
	}
	if got := attrs[1].Value.AsString(); got != "TRANSACTION" {
		t.Errorf("attrs[1] value = %q", got)
	}
}

var _ trace.Tracer = Tracer("compile-time check")
