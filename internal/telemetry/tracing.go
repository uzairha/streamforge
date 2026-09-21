package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// TracingConfig configures InitTracing.
type TracingConfig struct {
	Enabled     bool
	Endpoint    string // OTLP/gRPC collector, host:port
	ServiceName string // shows up as the service in Jaeger
	SampleRatio float64
}

// ShutdownFunc flushes and releases tracing resources. It is always safe to
// call, including when tracing was never enabled.
type ShutdownFunc func(context.Context) error

// InitTracing installs a global TracerProvider exporting spans over OTLP/gRPC,
// plus the W3C trace-context propagator that carries a trace across process
// boundaries (including Kafka records — see internal/kafka).
//
// When cfg.Enabled is false it installs nothing and returns a no-op shutdown.
// That is deliberate: OpenTelemetry's default global provider already returns
// non-recording spans, so call sites can start spans unconditionally instead
// of guarding every one with a config check.
//
// The returned ShutdownFunc must run *after* the service has drained its work,
// and on a fresh context — not the cancelled one that triggered shutdown —
// or the last spans of a run (an aggregator's final window flush, say) are
// dropped before they can be exported.
func InitTracing(ctx context.Context, cfg TracingConfig) (ShutdownFunc, error) {
	noop := func(context.Context) error { return nil }
	if !cfg.Enabled {
		return noop, nil
	}

	// No WithBlock: the exporter connects lazily so an unreachable collector
	// degrades to dropped spans rather than blocking service startup.
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return noop, fmt.Errorf("telemetry: otlp exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		return noop, fmt.Errorf("telemetry: build resource: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		// Batched, not synchronous: a SimpleSpanProcessor would export on the
		// hot path of every span.
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		// ParentBased is what keeps a pipeline trace whole. Without it each
		// service samples independently and a trace that the ingester kept
		// would vanish at the normalizer, leaving unreadable fragments.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return provider.Shutdown, nil
}

// Tracer returns a named tracer from the global provider. Before InitTracing
// runs — or when tracing is disabled — this is the no-op tracer.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// EventAttributes describes a ChainEvent on a span without pulling the
// protobuf types into this package.
func EventAttributes(chain, eventType string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("streamforge.chain", chain),
		attribute.String("streamforge.event_type", eventType),
	}
}
