package apiserver

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
	"github.com/uzairha/streamforge/internal/store"
)

// EventTailer is the live-tail dependency StreamEvents reads from — the
// shape of *kafka.TailConsumer, kept as an interface so tests can supply a
// fake instead of a real Kafka broker.
type EventTailer interface {
	Tail(ctx context.Context, out chan<- *streamforgev1.ChainEvent) error
	Close()
}

// AggregateStore is the read dependency GetAggregates uses — the shape of
// *store.Store's QueryAggregates method.
type AggregateStore interface {
	QueryAggregates(ctx context.Context, q store.AggregateQuery) ([]*streamforgev1.Aggregate, error)
}

// Server implements streamforgev1.StreamForgeServiceServer.
type Server struct {
	streamforgev1.UnimplementedStreamForgeServiceServer

	// newTailer opens a fresh EventTailer for one StreamEvents call. A new
	// tail is opened per call (see kafka.TailConsumer) rather than sharing
	// one, since each caller wants its own full copy of the stream from
	// wherever it connects, not to split records with other callers.
	newTailer func() (EventTailer, error)
	store     AggregateStore
	prom      PrometheusQuerier
	startedAt time.Time
}

// NewServer builds a Server. startedAt is reported as GetStats' Since field.
func NewServer(newTailer func() (EventTailer, error), st AggregateStore, prom PrometheusQuerier, startedAt time.Time) *Server {
	return &Server{newTailer: newTailer, store: st, prom: prom, startedAt: startedAt}
}

// StreamEvents tails normalized-events from wherever the underlying
// EventTailer starts (its current end, for kafka.TailConsumer) and forwards
// records matching req until the client disconnects or the tail ends.
func (s *Server) StreamEvents(req *streamforgev1.StreamEventsRequest, stream streamforgev1.StreamForgeService_StreamEventsServer) error {
	tailer, err := s.newTailer()
	if err != nil {
		return status.Errorf(codes.Unavailable, "connect event tail: %v", err)
	}
	defer tailer.Close()

	ctx := stream.Context()
	out := make(chan *streamforgev1.ChainEvent, 64)
	tailErr := make(chan error, 1)
	go func() { tailErr <- tailer.Tail(ctx, out) }()

	for {
		select {
		case ev, ok := <-out:
			if !ok {
				return <-tailErr
			}
			if !matchesFilter(req, ev) {
				continue
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// matchesFilter reports whether ev satisfies every filter set on req; an
// unset field (empty chain/address, empty types) matches everything.
func matchesFilter(req *streamforgev1.StreamEventsRequest, ev *streamforgev1.ChainEvent) bool {
	if chain := req.GetChain(); chain != "" && !strings.EqualFold(chain, ev.GetChain()) {
		return false
	}
	if types := req.GetTypes(); len(types) > 0 {
		matched := false
		for _, t := range types {
			if t == ev.GetType() {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if addr := req.GetAddress(); addr != "" {
		if !strings.EqualFold(addr, ev.GetFromAddress()) && !strings.EqualFold(addr, ev.GetToAddress()) {
			return false
		}
	}
	return true
}

// GetAggregates queries the aggregate store for windows matching req. An
// unset Since/Until leaves that end of the range unbounded; an unset
// chain/metric matches every chain/metric.
func (s *Server) GetAggregates(ctx context.Context, req *streamforgev1.GetAggregatesRequest) (*streamforgev1.GetAggregatesResponse, error) {
	q := store.AggregateQuery{
		Chain:  req.GetChain(),
		Metric: req.GetMetric(),
	}
	if req.GetSince() != nil {
		q.Since = req.GetSince().AsTime()
	}
	if req.GetUntil() != nil {
		q.Until = req.GetUntil().AsTime()
	}

	aggs, err := s.store.QueryAggregates(ctx, q)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query aggregates: %v", err)
	}
	return &streamforgev1.GetAggregatesResponse{Aggregates: aggs}, nil
}

// GetStats reports a pipeline throughput snapshot sourced from Prometheus.
// Every counter is a live sum across every running instance of a service —
// it is not itself cumulative across process restarts beyond what
// Prometheus already retains, and events_per_second is a 1-minute rate, not
// an instantaneous one.
func (s *Server) GetStats(ctx context.Context, _ *streamforgev1.GetStatsRequest) (*streamforgev1.GetStatsResponse, error) {
	ingested, err := s.queryFloat(ctx, "sum(streamforge_events_ingested_total)")
	if err != nil {
		return nil, err
	}
	normalized, err := s.queryFloat(ctx, "sum(streamforge_normalizer_events_produced_total)")
	if err != nil {
		return nil, err
	}
	aggregatesEmitted, err := s.queryFloat(ctx, "sum(streamforge_aggregator_windows_closed_total)")
	if err != nil {
		return nil, err
	}
	rate, err := s.queryFloat(ctx, "sum(rate(streamforge_events_ingested_total[1m]))")
	if err != nil {
		return nil, err
	}

	return &streamforgev1.GetStatsResponse{
		EventsIngested:    uint64(ingested),
		EventsNormalized:  uint64(normalized),
		AggregatesEmitted: uint64(aggregatesEmitted),
		EventsPerSecond:   rate,
		Since:             timestamppb.New(s.startedAt),
	}, nil
}

func (s *Server) queryFloat(ctx context.Context, promql string) (float64, error) {
	v, err := s.prom.InstantQuery(ctx, promql)
	if err != nil {
		return 0, status.Errorf(codes.Unavailable, "query prometheus: %v", err)
	}
	return v, nil
}
