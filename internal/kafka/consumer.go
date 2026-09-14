package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// commitTimeout bounds the detached context used to commit offsets after a
// fetch is fully handled. It's independent of the caller's ctx so that a
// shutdown signal arriving right after a successful handle can't prevent the
// already-earned commit — see Run.
const commitTimeout = 10 * time.Second

// CommitError wraps a failure to commit offsets after a fetch was fully
// handled, distinguishing it from a decode or handler failure so callers can
// react differently (e.g. a dedicated metric) via errors.As.
type CommitError struct{ Err error }

func (e *CommitError) Error() string { return fmt.Sprintf("kafka: commit offsets: %v", e.Err) }
func (e *CommitError) Unwrap() error { return e.Err }

// Handler processes one decoded ChainEvent. Returning an error stops Run
// before the offset is committed, so the record (and the rest of its fetch)
// is redelivered after a restart instead of being silently skipped.
type Handler func(ctx context.Context, ev *streamforgev1.ChainEvent) error

// Consumer reads ChainEvents from a topic as part of a consumer group and
// commits offsets manually, only once every record in a fetch has been
// handled successfully.
type Consumer struct {
	client *kgo.Client
	topic  string
}

// NewConsumer joins group on topic with cooperative-sticky rebalancing and
// auto-commit disabled. Call Run to process records and commit offsets.
func NewConsumer(brokers []string, topic, group string) (*Consumer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka: no brokers configured")
	}
	if topic == "" {
		return nil, fmt.Errorf("kafka: empty topic")
	}
	if group == "" {
		return nil, fmt.Errorf("kafka: empty consumer group")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ClientID("streamforge-"+group),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	return &Consumer{client: client, topic: topic}, nil
}

// Ping verifies broker connectivity.
func (c *Consumer) Ping(ctx context.Context) error {
	return c.client.Ping(ctx)
}

// Close closes the underlying client without committing. Records handled but
// not yet committed will be redelivered to the group after this instance
// leaves.
func (c *Consumer) Close() {
	c.client.Close()
}

// Run polls fetches until ctx is cancelled, decoding each record and passing
// it to handle in order. Once every record in a fetch has been handled
// without error, Run commits that fetch's offsets before polling again. A
// decode or handler failure stops Run immediately with that error and
// commits nothing for the in-flight fetch, so the failing record (and any
// after it in the same fetch) is redelivered on the next run.
//
// Run returns nil when ctx is cancelled during a clean poll. A fetch that
// was already fully handled is still committed even if ctx is cancelled in
// that instant (e.g. by a shutdown signal arriving right after the last
// handle call) — the commit itself runs on a short-lived detached context,
// not ctx, so already-earned work is never lost to the same cancellation
// that's telling Run to stop.
func (c *Consumer) Run(ctx context.Context, handle Handler) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("kafka: poll fetches (topic %s): %w", errs[0].Topic, errs[0].Err)
		}

		var handleErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if handleErr != nil {
				return
			}
			var ev streamforgev1.ChainEvent
			if err := proto.Unmarshal(r.Value, &ev); err != nil {
				handleErr = fmt.Errorf("kafka: unmarshal record at offset %d: %w", r.Offset, err)
				return
			}
			if err := handle(ctx, &ev); err != nil {
				handleErr = err
			}
		})
		if handleErr != nil {
			return handleErr
		}

		commitCtx, cancel := context.WithTimeout(context.Background(), commitTimeout)
		err := c.client.CommitUncommittedOffsets(commitCtx)
		cancel()
		if err != nil {
			return &CommitError{Err: err}
		}
	}
}
