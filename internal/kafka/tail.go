package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// TailConsumer reads ChainEvents from a topic starting at the current end —
// only events produced after it connects. Unlike Consumer, it never joins a
// consumer group: each TailConsumer gets its own full copy of the stream, as
// suits a broadcast live tail (e.g. one per StreamEvents API call) rather
// than a shared work queue where records must be split across workers.
type TailConsumer struct {
	client *kgo.Client
}

// NewTailConsumer connects to brokers and seeks topic to its current end.
func NewTailConsumer(brokers []string, topic string) (*TailConsumer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka: no brokers configured")
	}
	if topic == "" {
		return nil, fmt.Errorf("kafka: empty topic")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.ClientID("streamforge-api-tail"),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	return &TailConsumer{client: client}, nil
}

// Close closes the underlying client.
func (t *TailConsumer) Close() { t.client.Close() }

// Tail decodes records onto out until ctx is cancelled or a fatal error
// occurs. It always closes out before returning; a nil return means a
// clean, context-driven shutdown.
func (t *TailConsumer) Tail(ctx context.Context, out chan<- *streamforgev1.ChainEvent) error {
	defer close(out)
	for {
		fetches := t.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("kafka: poll fetches (topic %s): %w", errs[0].Topic, errs[0].Err)
		}

		var decodeErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if decodeErr != nil {
				return
			}
			var ev streamforgev1.ChainEvent
			if err := proto.Unmarshal(r.Value, &ev); err != nil {
				decodeErr = fmt.Errorf("kafka: unmarshal record at offset %d: %w", r.Offset, err)
				return
			}
			select {
			case out <- &ev:
			case <-ctx.Done():
			}
		})
		if decodeErr != nil {
			return decodeErr
		}
	}
}
