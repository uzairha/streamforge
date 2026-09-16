// Package apiserver implements the gRPC StreamForgeService: a live tail of
// normalized events, aggregate window queries against TimescaleDB, and a
// pipeline throughput snapshot sourced from Prometheus.
package apiserver

import (
	"context"
	"crypto/subtle"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// apiKeyHeader is the (lowercase, per grpc metadata convention) key every
// request must carry. The REST gateway maps the client-facing "X-Api-Key"
// HTTP header onto this same gRPC metadata key (see cmd/api), so both
// transports use one code path.
const apiKeyHeader = "x-api-key"

// checkAPIKey reports whether ctx's incoming metadata carries key. An empty
// key disables auth entirely (every request passes) — the caller is
// responsible for logging that at startup so it's never silent in
// production.
func checkAPIKey(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing API key")
	}
	got := md.Get(apiKeyHeader)
	if len(got) != 1 || subtle.ConstantTimeCompare([]byte(got[0]), []byte(key)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid or missing API key")
	}
	return nil
}

// UnaryAuthInterceptor rejects a unary call whose metadata doesn't carry a
// valid API key. Passing an empty key disables auth (see checkAPIKey).
func UnaryAuthInterceptor(key string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := checkAPIKey(ctx, key); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamAuthInterceptor is UnaryAuthInterceptor's streaming-RPC equivalent.
func StreamAuthInterceptor(key string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := checkAPIKey(ss.Context(), key); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}
