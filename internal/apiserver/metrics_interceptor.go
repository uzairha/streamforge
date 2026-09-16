package apiserver

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
	"github.com/uzairha/streamforge/internal/telemetry"
)

// UnaryMetricsInterceptor records every unary RPC's method and resulting
// grpc status code, plus a dedicated counter for auth failures (identified
// by the Unauthenticated code, however it was produced — this interceptor
// doesn't need to know it runs after UnaryAuthInterceptor in the chain).
func UnaryMetricsInterceptor(m *telemetry.APIMetrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		recordResult(m, info.FullMethod, err)
		return resp, err
	}
}

// StreamMetricsInterceptor is UnaryMetricsInterceptor's streaming-RPC
// equivalent, and additionally tracks how many StreamEvents calls are
// currently open.
func StreamMetricsInterceptor(m *telemetry.APIMetrics) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod == streamforgev1.StreamForgeService_StreamEvents_FullMethodName {
			m.StreamEventsActive.Inc()
			defer m.StreamEventsActive.Dec()
		}
		err := handler(srv, ss)
		recordResult(m, info.FullMethod, err)
		return err
	}
}

func recordResult(m *telemetry.APIMetrics, method string, err error) {
	code := status.Code(err) // codes.OK for a nil err
	m.RequestsTotal.WithLabelValues(method, code.String()).Inc()
	if code == codes.Unauthenticated {
		m.AuthFailuresTotal.Inc()
	}
}
