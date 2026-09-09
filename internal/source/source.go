// Package source produces canonical ChainEvents from some origin — a synthetic
// generator, a live chain RPC, or a replay file. Everything downstream of the
// ingester consumes this one shape, so a new origin is just a new Source.
package source

import (
	"context"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

// Source is a cancellable stream of ChainEvents.
type Source interface {
	// Name identifies the source in logs and metrics.
	Name() string
	// Run emits events on out until ctx is cancelled or a fatal error occurs.
	// Implementations MUST close out before returning. A nil return means a
	// clean, context-driven shutdown.
	Run(ctx context.Context, out chan<- *streamforgev1.ChainEvent) error
}
