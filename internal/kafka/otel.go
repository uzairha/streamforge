package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// tracerName identifies spans produced by this package in the exported data.
const tracerName = "github.com/uzairha/streamforge/internal/kafka"

func tracer() trace.Tracer { return otel.Tracer(tracerName) }

// recordCarrier adapts a Kafka record's headers to the OpenTelemetry
// TextMapCarrier interface, which is how a trace crosses a topic: the producer
// writes W3C traceparent into the record headers and the consumer reads it
// back out, so the consuming span joins the producing trace instead of
// starting an unrelated one.
type recordCarrier struct{ rec *kgo.Record }

func (c recordCarrier) Get(key string) string {
	for _, h := range c.rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set replaces an existing header rather than appending, so re-injecting into
// the same record can't leave two conflicting traceparent values behind.
func (c recordCarrier) Set(key, value string) {
	for i := range c.rec.Headers {
		if c.rec.Headers[i].Key == key {
			c.rec.Headers[i].Value = []byte(value)
			return
		}
	}
	c.rec.Headers = append(c.rec.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c recordCarrier) Keys() []string {
	keys := make([]string, 0, len(c.rec.Headers))
	for _, h := range c.rec.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// injectTrace writes the trace context in ctx into rec's headers.
func injectTrace(ctx context.Context, rec *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, recordCarrier{rec})
}

// extractTrace returns ctx with any trace context found in rec's headers, so
// a span started from it continues the producer's trace.
func extractTrace(ctx context.Context, rec *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, recordCarrier{rec})
}

// producerSpanOpts describes a send to topic, following the OpenTelemetry
// messaging semantic conventions.
func producerSpanOpts(topic string, extra ...attribute.KeyValue) []trace.SpanStartOption {
	attrs := append([]attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingDestinationName(topic),
		semconv.MessagingOperationTypeSend,
	}, extra...)
	return []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	}
}

// consumerSpanOpts describes handling one record from topic.
func consumerSpanOpts(topic string, extra ...attribute.KeyValue) []trace.SpanStartOption {
	attrs := append([]attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingDestinationName(topic),
		semconv.MessagingOperationTypeProcess,
	}, extra...)
	return []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	}
}

var _ propagation.TextMapCarrier = recordCarrier{}
