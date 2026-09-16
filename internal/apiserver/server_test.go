package apiserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
	"github.com/uzairha/streamforge/internal/store"
)

func TestMatchesFilter(t *testing.T) {
	ev := &streamforgev1.ChainEvent{
		Chain:       "ethereum",
		Type:        streamforgev1.EventType_EVENT_TYPE_TRANSACTION,
		FromAddress: "0xalice",
		ToAddress:   "0xbob",
	}
	cases := []struct {
		name string
		req  *streamforgev1.StreamEventsRequest
		want bool
	}{
		{"empty filter matches everything", &streamforgev1.StreamEventsRequest{}, true},
		{"matching chain", &streamforgev1.StreamEventsRequest{Chain: "ethereum"}, true},
		{"chain case-insensitive", &streamforgev1.StreamEventsRequest{Chain: "ETHEREUM"}, true},
		{"non-matching chain", &streamforgev1.StreamEventsRequest{Chain: "solana"}, false},
		{"matching type", &streamforgev1.StreamEventsRequest{Types: []streamforgev1.EventType{streamforgev1.EventType_EVENT_TYPE_TRANSACTION}}, true},
		{"non-matching type", &streamforgev1.StreamEventsRequest{Types: []streamforgev1.EventType{streamforgev1.EventType_EVENT_TYPE_BLOCK}}, false},
		{"one of several types matches", &streamforgev1.StreamEventsRequest{Types: []streamforgev1.EventType{
			streamforgev1.EventType_EVENT_TYPE_BLOCK, streamforgev1.EventType_EVENT_TYPE_TRANSACTION,
		}}, true},
		{"matching from address", &streamforgev1.StreamEventsRequest{Address: "0xalice"}, true},
		{"matching to address", &streamforgev1.StreamEventsRequest{Address: "0xbob"}, true},
		{"address case-insensitive", &streamforgev1.StreamEventsRequest{Address: "0xALICE"}, true},
		{"non-matching address", &streamforgev1.StreamEventsRequest{Address: "0xcarol"}, false},
		{"all filters match", &streamforgev1.StreamEventsRequest{Chain: "ethereum", Address: "0xbob", Types: []streamforgev1.EventType{streamforgev1.EventType_EVENT_TYPE_TRANSACTION}}, true},
		{"one non-matching filter fails the rest", &streamforgev1.StreamEventsRequest{Chain: "ethereum", Address: "0xcarol"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchesFilter(tc.req, ev); got != tc.want {
				t.Errorf("matchesFilter() = %v, want %v", got, tc.want)
			}
		})
	}
}

// fakeTailer feeds a fixed slice of events onto out, then blocks until ctx
// is cancelled — mirroring kafka.TailConsumer's contract of running until
// context cancellation or a fatal error.
type fakeTailer struct {
	events  []*streamforgev1.ChainEvent
	closed  bool
	tailErr error
}

func (f *fakeTailer) Tail(ctx context.Context, out chan<- *streamforgev1.ChainEvent) error {
	defer close(out)
	for _, ev := range f.events {
		select {
		case out <- ev:
		case <-ctx.Done():
			return nil
		}
	}
	if f.tailErr != nil {
		return f.tailErr
	}
	<-ctx.Done()
	return nil
}

func (f *fakeTailer) Close() { f.closed = true }

// fakeStreamEventsServer implements streamforgev1.StreamForgeService_StreamEventsServer
// enough to drive StreamEvents in tests: it records every sent event and
// stops the RPC (via ctx cancellation) once it has collected `stopAfter`.
type fakeStreamEventsServer struct {
	grpc.ServerStream
	ctx       context.Context
	cancel    context.CancelFunc
	stopAfter int
	sent      []*streamforgev1.ChainEvent
}

func newFakeStreamEventsServer(stopAfter int) *fakeStreamEventsServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &fakeStreamEventsServer{ctx: ctx, cancel: cancel, stopAfter: stopAfter}
}

func (f *fakeStreamEventsServer) Context() context.Context { return f.ctx }

func (f *fakeStreamEventsServer) Send(ev *streamforgev1.ChainEvent) error {
	f.sent = append(f.sent, ev)
	if f.stopAfter > 0 && len(f.sent) >= f.stopAfter {
		f.cancel()
	}
	return nil
}

func TestServer_StreamEvents_FiltersAndForwards(t *testing.T) {
	events := []*streamforgev1.ChainEvent{
		{Id: "1", Chain: "ethereum"},
		{Id: "2", Chain: "solana"},
		{Id: "3", Chain: "ethereum"},
	}
	tailer := &fakeTailer{events: events}
	srv := NewServer(func() (EventTailer, error) { return tailer, nil }, nil, nil, time.Now())

	stream := newFakeStreamEventsServer(2)
	err := srv.StreamEvents(&streamforgev1.StreamEventsRequest{Chain: "ethereum"}, stream)
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	if len(stream.sent) != 2 {
		t.Fatalf("sent %d events, want 2 (only the ethereum ones)", len(stream.sent))
	}
	for _, ev := range stream.sent {
		if ev.GetChain() != "ethereum" {
			t.Errorf("sent a non-matching event: %v", ev)
		}
	}
	if !tailer.closed {
		t.Error("StreamEvents must Close() the tailer before returning")
	}
}

func TestServer_StreamEvents_TailerConstructionError(t *testing.T) {
	wantErr := errors.New("kafka unreachable")
	srv := NewServer(func() (EventTailer, error) { return nil, wantErr }, nil, nil, time.Now())

	stream := newFakeStreamEventsServer(0)
	err := srv.StreamEvents(&streamforgev1.StreamEventsRequest{}, stream)
	if err == nil {
		t.Fatal("want an error when the tailer can't be constructed")
	}
}

type fakeAggregateStore struct {
	got    store.AggregateQuery
	result []*streamforgev1.Aggregate
	err    error
}

func (f *fakeAggregateStore) QueryAggregates(_ context.Context, q store.AggregateQuery) ([]*streamforgev1.Aggregate, error) {
	f.got = q
	return f.result, f.err
}

func TestServer_GetAggregates(t *testing.T) {
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	want := []*streamforgev1.Aggregate{{Chain: "ethereum", Metric: "events_total", Value: 5}}
	st := &fakeAggregateStore{result: want}
	srv := NewServer(nil, st, nil, time.Now())

	resp, err := srv.GetAggregates(context.Background(), &streamforgev1.GetAggregatesRequest{
		Chain: "ethereum", Metric: "events_total", Since: timestamppb.New(since),
	})
	if err != nil {
		t.Fatalf("GetAggregates: %v", err)
	}
	if len(resp.GetAggregates()) != 1 || resp.GetAggregates()[0] != want[0] {
		t.Errorf("GetAggregates() = %v, want %v", resp.GetAggregates(), want)
	}
	if st.got.Chain != "ethereum" || st.got.Metric != "events_total" || !st.got.Since.Equal(since) {
		t.Errorf("QueryAggregates called with %+v", st.got)
	}
	if !st.got.Until.IsZero() {
		t.Errorf("Until = %v, want zero (unset in the request)", st.got.Until)
	}
}

func TestServer_GetAggregates_StoreError(t *testing.T) {
	st := &fakeAggregateStore{err: errors.New("db down")}
	srv := NewServer(nil, st, nil, time.Now())

	if _, err := srv.GetAggregates(context.Background(), &streamforgev1.GetAggregatesRequest{}); err == nil {
		t.Fatal("want an error when the store fails")
	}
}

type fakePrometheusQuerier struct {
	values map[string]float64
	err    error
}

func (f *fakePrometheusQuerier) InstantQuery(_ context.Context, promql string) (float64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.values[promql], nil
}

func TestServer_GetStats(t *testing.T) {
	startedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	prom := &fakePrometheusQuerier{values: map[string]float64{
		"sum(streamforge_events_ingested_total)":            1000,
		"sum(streamforge_normalizer_events_produced_total)": 990,
		"sum(streamforge_aggregator_windows_closed_total)":  50,
		"sum(rate(streamforge_events_ingested_total[1m]))":  12.5,
	}}
	srv := NewServer(nil, nil, prom, startedAt)

	resp, err := srv.GetStats(context.Background(), &streamforgev1.GetStatsRequest{})
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if resp.GetEventsIngested() != 1000 {
		t.Errorf("EventsIngested = %d, want 1000", resp.GetEventsIngested())
	}
	if resp.GetEventsNormalized() != 990 {
		t.Errorf("EventsNormalized = %d, want 990", resp.GetEventsNormalized())
	}
	if resp.GetAggregatesEmitted() != 50 {
		t.Errorf("AggregatesEmitted = %d, want 50", resp.GetAggregatesEmitted())
	}
	if resp.GetEventsPerSecond() != 12.5 {
		t.Errorf("EventsPerSecond = %v, want 12.5", resp.GetEventsPerSecond())
	}
	if !resp.GetSince().AsTime().Equal(startedAt) {
		t.Errorf("Since = %v, want %v", resp.GetSince().AsTime(), startedAt)
	}
}

func TestServer_GetStats_PrometheusError(t *testing.T) {
	prom := &fakePrometheusQuerier{err: errors.New("prometheus down")}
	srv := NewServer(nil, nil, prom, time.Now())

	if _, err := srv.GetStats(context.Background(), &streamforgev1.GetStatsRequest{}); err == nil {
		t.Fatal("want an error when prometheus is unreachable")
	}
}
