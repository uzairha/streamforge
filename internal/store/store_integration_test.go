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
