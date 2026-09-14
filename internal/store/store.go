// Package store persists closed aggregation windows to TimescaleDB.
package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

//go:embed schema.sql
var schema string

// Store writes Aggregates to TimescaleDB.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects a pool to dsn and verifies connectivity. Call EnsureSchema
// before first use.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: new pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// EnsureSchema creates the aggregates hypertable if it doesn't already exist.
func (s *Store) EnsureSchema(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("store: ensure schema: %w", err)
	}
	return nil
}

// Close closes the underlying pool.
func (s *Store) Close() { s.pool.Close() }

const upsertSQL = `
INSERT INTO aggregates (chain, metric, window_start, window_end, value, labels, labels_key)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (chain, metric, window_start, labels_key)
DO UPDATE SET window_end = EXCLUDED.window_end, value = EXCLUDED.value, labels = EXCLUDED.labels
`

// UpsertAggregates writes aggs in one batched round trip. Each row is keyed
// by (chain, metric, window_start, labels): redelivering an aggregate for a
// window already stored overwrites its value instead of duplicating the
// row, so replaying the aggregator's input after a crash is safe.
func (s *Store) UpsertAggregates(ctx context.Context, aggs []*streamforgev1.Aggregate) error {
	if len(aggs) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, a := range aggs {
		labelsJSON, key := encodeLabels(a.GetLabels())
		batch.Queue(upsertSQL,
			a.GetChain(), a.GetMetric(), a.GetWindowStart().AsTime(), a.GetWindowEnd().AsTime(),
			a.GetValue(), labelsJSON, key,
		)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()
	for range aggs {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("store: upsert aggregate: %w", err)
		}
	}
	return nil
}

// encodeLabels renders labels as JSON for storage and a deterministic,
// sorted "k=v,k=v" string used as part of the uniqueness key (see schema.sql
// for why jsonb itself can't fill that role).
func encodeLabels(labels map[string]string) (json.RawMessage, string) {
	if len(labels) == 0 {
		return json.RawMessage("{}"), ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + labels[k]
	}
	b, err := json.Marshal(labels)
	if err != nil {
		// labels is map[string]string; every key and value is a valid JSON
		// string, so Marshal cannot fail.
		panic(fmt.Sprintf("store: marshal labels: %v", err))
	}
	return b, strings.Join(parts, ",")
}
