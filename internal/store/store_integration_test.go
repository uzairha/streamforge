package store

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/types/known/timestamppb"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

func startStore(ctx context.Context, t *testing.T) *Store {
	t.Helper()
	container, err := tcpostgres.Run(ctx, "timescale/timescaledb:2.17.2-pg16",
		tcpostgres.WithDatabase("streamforge"),
		tcpostgres.WithUsername("streamforge"),
		tcpostgres.WithPassword("streamforge"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start timescaledb: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(s.Close)

	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	return s
}

func aggregate(chain, metric string, start time.Time, value float64, labels map[string]string) *streamforgev1.Aggregate {
	return &streamforgev1.Aggregate{
		Chain:       chain,
		Metric:      metric,
		WindowStart: timestamppb.New(start),
		WindowEnd:   timestamppb.New(start.Add(time.Minute)),
		Value:       value,
		Labels:      labels,
	}
}

func TestStore_Integration_UpsertIsIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()
	s := startStore(ctx, t)

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	first := aggregate("ethereum", "events_total", start, 10, nil)

	if err := s.UpsertAggregates(ctx, []*streamforgev1.Aggregate{first}); err != nil {
		t.Fatalf("first UpsertAggregates: %v", err)
	}

	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM aggregates`).Scan(&count); err != nil {
		t.Fatalf("count after first upsert: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count = %d, want 1", count)
	}

	// Redeliver the same window with an updated value: must overwrite, not
	// duplicate.
	updated := aggregate("ethereum", "events_total", start, 42, nil)
	if err := s.UpsertAggregates(ctx, []*streamforgev1.Aggregate{updated}); err != nil {
		t.Fatalf("second UpsertAggregates: %v", err)
	}

	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM aggregates`).Scan(&count); err != nil {
		t.Fatalf("count after second upsert: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count after redelivery = %d, want 1 (upsert should overwrite)", count)
	}

	var value float64
	if err := s.pool.QueryRow(ctx, `SELECT value FROM aggregates WHERE chain = 'ethereum'`).Scan(&value); err != nil {
		t.Fatalf("read back value: %v", err)
	}
	if value != 42 {
		t.Errorf("value = %v, want 42 (the redelivered value)", value)
	}
}

func TestStore_Integration_DistinctLabelsAreDistinctRows(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()
	s := startStore(ctx, t)

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	aggs := []*streamforgev1.Aggregate{
		aggregate("ethereum", "events_total", start, 3, map[string]string{"type": "EVENT_TYPE_BLOCK"}),
		aggregate("ethereum", "events_total", start, 7, map[string]string{"type": "EVENT_TYPE_TRANSACTION"}),
		aggregate("ethereum", "events_total", start, 10, nil),
	}
	if err := s.UpsertAggregates(ctx, aggs); err != nil {
		t.Fatalf("UpsertAggregates: %v", err)
	}

	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM aggregates`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Fatalf("row count = %d, want 3 (one per distinct label set)", count)
	}
}

func TestStore_Integration_QueryAggregatesFilters(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker; skipped in -short mode")
	}
	ctx := context.Background()
	s := startStore(ctx, t)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	aggs := []*streamforgev1.Aggregate{
		aggregate("ethereum", "events_total", base, 10, nil),
		aggregate("ethereum", "gas_used_total", base, 500, nil),
		aggregate("ethereum", "events_total", base.Add(time.Minute), 20, nil),
		aggregate("solana", "events_total", base, 99, nil),
	}
	if err := s.UpsertAggregates(ctx, aggs); err != nil {
		t.Fatalf("UpsertAggregates: %v", err)
	}

	t.Run("no filter returns everything", func(t *testing.T) {
		got, err := s.QueryAggregates(ctx, AggregateQuery{})
		if err != nil {
			t.Fatalf("QueryAggregates: %v", err)
		}
		if len(got) != 4 {
			t.Fatalf("len = %d, want 4", len(got))
		}
	})

	t.Run("chain filter", func(t *testing.T) {
		got, err := s.QueryAggregates(ctx, AggregateQuery{Chain: "solana"})
		if err != nil {
			t.Fatalf("QueryAggregates: %v", err)
		}
		if len(got) != 1 || got[0].GetChain() != "solana" {
			t.Fatalf("got = %v, want exactly the solana row", got)
		}
	})

	t.Run("metric filter", func(t *testing.T) {
		got, err := s.QueryAggregates(ctx, AggregateQuery{Chain: "ethereum", Metric: "gas_used_total"})
		if err != nil {
			t.Fatalf("QueryAggregates: %v", err)
		}
		if len(got) != 1 || got[0].GetValue() != 500 {
			t.Fatalf("got = %v, want exactly the gas_used_total row", got)
		}
	})

	t.Run("time range excludes the later window", func(t *testing.T) {
		got, err := s.QueryAggregates(ctx, AggregateQuery{
			Chain: "ethereum", Metric: "events_total",
			Until: base.Add(30 * time.Second),
		})
		if err != nil {
			t.Fatalf("QueryAggregates: %v", err)
		}
		if len(got) != 1 || got[0].GetValue() != 10 {
			t.Fatalf("got = %v, want only the base-window row (value 10)", got)
		}
	})

	t.Run("time range excludes the earlier window", func(t *testing.T) {
		got, err := s.QueryAggregates(ctx, AggregateQuery{
			Chain: "ethereum", Metric: "events_total",
			Since: base.Add(30 * time.Second),
		})
		if err != nil {
			t.Fatalf("QueryAggregates: %v", err)
		}
		if len(got) != 1 || got[0].GetValue() != 20 {
			t.Fatalf("got = %v, want only the later-window row (value 20)", got)
		}
	})
}
