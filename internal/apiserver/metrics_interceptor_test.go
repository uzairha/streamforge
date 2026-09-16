package apiserver

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
	"github.com/uzairha/streamforge/internal/telemetry"
)

func TestUnaryMetricsInterceptor_RecordsCode(t *testing.T) {
	m := telemetry.NewAPIMetrics()
	interceptor := UnaryMetricsInterceptor(m)
	info := &grpc.UnaryServerInfo{FullMethod: "/streamforge.v1.StreamForgeService/GetStats"}

	okHandler := func(_ context.Context, _ any) (any, error) { return "ok", nil }
	if _, err := interceptor(context.Background(), nil, info, okHandler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	failHandler := func(_ context.Context, _ any) (any, error) {
		return nil, status.Error(codes.Unauthenticated, "nope")
	}
	if _, err := interceptor(context.Background(), nil, info, failHandler); err == nil {
		t.Fatal("want error to pass through")
	}

	body := scrape(t, m)
	if !strings.Contains(body, `streamforge_api_requests_total{code="OK",method="/streamforge.v1.StreamForgeService/GetStats"} 1`) {
		t.Errorf("missing OK request count in exposition:\n%s", body)
	}
	if !strings.Contains(body, `streamforge_api_requests_total{code="Unauthenticated",method="/streamforge.v1.StreamForgeService/GetStats"} 1`) {
		t.Errorf("missing Unauthenticated request count in exposition:\n%s", body)
	}
	if !strings.Contains(body, "streamforge_api_auth_failures_total 1") {
		t.Errorf("missing auth failure count in exposition:\n%s", body)
	}
}

func TestStreamMetricsInterceptor_TracksActiveStreamEvents(t *testing.T) {
	m := telemetry.NewAPIMetrics()
	interceptor := StreamMetricsInterceptor(m)
	info := &grpc.StreamServerInfo{FullMethod: streamforgev1.StreamForgeService_StreamEvents_FullMethodName}

	started := make(chan struct{})
	release := make(chan struct{})
	handler := func(_ any, _ grpc.ServerStream) error {
		close(started)
		<-release
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- interceptor(nil, &fakeServerStream{ctx: context.Background()}, info, handler) }()

	<-started
	if body := scrape(t, m); !strings.Contains(body, "streamforge_api_stream_events_active 1") {
		t.Errorf("want active gauge = 1 mid-call:\n%s", body)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if body := scrape(t, m); !strings.Contains(body, "streamforge_api_stream_events_active 0") {
		t.Errorf("want active gauge = 0 after the call ends:\n%s", body)
	}
}

func TestStreamMetricsInterceptor_IgnoresActiveGaugeForOtherMethods(t *testing.T) {
	m := telemetry.NewAPIMetrics()
	interceptor := StreamMetricsInterceptor(m)
	info := &grpc.StreamServerInfo{FullMethod: "/streamforge.v1.StreamForgeService/SomethingElse"}

	err := interceptor(nil, &fakeServerStream{ctx: context.Background()}, info, func(_ any, _ grpc.ServerStream) error {
		body := scrape(t, m)
		if !strings.Contains(body, "streamforge_api_stream_events_active 0") {
			t.Errorf("gauge should stay 0 for a non-StreamEvents method:\n%s", body)
		}
		return errors.New("boom")
	})
	if err == nil {
		t.Fatal("want the handler's error to pass through")
	}
}

func scrape(t *testing.T, m *telemetry.APIMetrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)
	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("read metrics response: %v", err)
	}
	return string(body)
}
