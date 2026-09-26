package kafka

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Tuning carries the producer and consumer knobs M6 exposes.
//
// Passed as a struct rather than a widening list of parameters: every new knob
// would otherwise change the signature of NewProducer, NewAggregateProducer,
// NewConsumer and NewTailConsumer at once, and callers would be positional
// argument lists nobody can read.
type Tuning struct {
	ProducerLinger      time.Duration
	ProducerMaxBatch    int32
	ProducerCompression string

	FetchMaxBytes int32
	FetchMaxWait  time.Duration
	FetchMinBytes int32
}

// resolveTuning picks the caller's tuning or the default.
//
// Constructors take `tuning ...Tuning` rather than a required parameter so the
// existing call sites — nearly all of them tests that have no opinion about
// batching — stay unchanged, and only the service entrypoints that actually
// tune anything have to say so.
func resolveTuning(tuning []Tuning) Tuning {
	if len(tuning) > 0 {
		return tuning[0]
	}
	return DefaultTuning()
}

// DefaultTuning reproduces the hardcoded behaviour of M1-M5, so a caller that
// does not care about tuning gets exactly what the pipeline had before.
func DefaultTuning() Tuning {
	return Tuning{
		ProducerLinger:      5 * time.Millisecond,
		ProducerMaxBatch:    1_000_012,
		ProducerCompression: "none",
		FetchMaxBytes:       52_428_800,
		FetchMaxWait:        5 * time.Second,
		FetchMinBytes:       1,
	}
}

// producerOpts renders the producer half of the tuning as franz-go options.
func (t Tuning) producerOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.ProducerLinger(t.ProducerLinger),
		kgo.ProducerBatchMaxBytes(t.ProducerMaxBatch),
	}
	if codec, ok := compressionCodec(t.ProducerCompression); ok {
		opts = append(opts, kgo.ProducerBatchCompression(codec))
	}
	return opts
}

// consumerOpts renders the consumer half.
func (t Tuning) consumerOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.FetchMaxBytes(t.FetchMaxBytes),
		kgo.FetchMaxWait(t.FetchMaxWait),
		kgo.FetchMinBytes(t.FetchMinBytes),
	}
}

// compressionCodec maps the config string to a franz-go codec.
//
// Returns ok=false for "none" rather than a NoCompression codec, so the option
// is omitted entirely and franz-go keeps its own default behaviour instead of
// being explicitly told not to compress.
func compressionCodec(name string) (kgo.CompressionCodec, bool) {
	switch name {
	case "gzip":
		return kgo.GzipCompression(), true
	case "snappy":
		return kgo.SnappyCompression(), true
	case "lz4":
		return kgo.Lz4Compression(), true
	case "zstd":
		return kgo.ZstdCompression(), true
	default:
		return kgo.CompressionCodec{}, false
	}
}

// String renders the tuning for a startup log line, so a load-test run's
// configuration is recorded alongside its results.
func (t Tuning) String() string {
	return fmt.Sprintf(
		"linger=%s batch_max=%d compression=%s fetch_max=%d fetch_min=%d fetch_wait=%s",
		t.ProducerLinger, t.ProducerMaxBatch, t.ProducerCompression,
		t.FetchMaxBytes, t.FetchMinBytes, t.FetchMaxWait)
}
