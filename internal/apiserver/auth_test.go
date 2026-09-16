package apiserver

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// fakeServerStream implements grpc.ServerStream just enough to exercise
// StreamAuthInterceptor, which only ever calls Context().
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

func ctxWithKey(key string) context.Context {
	if key == "" {
		return context.Background()
	}
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(apiKeyHeader, key))
}

func TestCheckAPIKey(t *testing.T) {
	cases := []struct {
		name       string
		serverKey  string
		requestCtx context.Context
		wantErr    bool
	}{
		{"auth disabled, no header", "", context.Background(), false},
		{"auth disabled, wrong header ignored", "", ctxWithKey("wrong"), false},
		{"correct key", "secret", ctxWithKey("secret"), false},
		{"wrong key", "secret", ctxWithKey("wrong"), true},
		{"missing key", "secret", context.Background(), true},
		{"empty header value", "secret", ctxWithKey(""), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkAPIKey(tc.requestCtx, tc.serverKey)
			if (err != nil) != tc.wantErr {
				t.Fatalf("checkAPIKey() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil && status.Code(err) != codes.Unauthenticated {
				t.Errorf("code = %v, want Unauthenticated", status.Code(err))
			}
		})
	}
}

func TestUnaryAuthInterceptor(t *testing.T) {
	interceptor := UnaryAuthInterceptor("secret")
	handlerCalled := false
	handler := func(_ context.Context, _ any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	if _, err := interceptor(ctxWithKey("wrong"), nil, nil, handler); err == nil {
		t.Error("wrong key: want error")
	}
	if handlerCalled {
		t.Error("handler must not run when auth fails")
	}

	resp, err := interceptor(ctxWithKey("secret"), nil, nil, handler)
	if err != nil {
		t.Fatalf("correct key: unexpected error %v", err)
	}
	if resp != "ok" || !handlerCalled {
		t.Error("handler must run and its result must pass through when auth succeeds")
	}
}

func TestStreamAuthInterceptor(t *testing.T) {
	interceptor := StreamAuthInterceptor("secret")
	handlerCalled := false
	handler := func(_ any, _ grpc.ServerStream) error {
		handlerCalled = true
		return nil
	}

	err := interceptor(nil, &fakeServerStream{ctx: ctxWithKey("wrong")}, nil, handler)
	if err == nil {
		t.Error("wrong key: want error")
	}
	if handlerCalled {
		t.Error("handler must not run when auth fails")
	}

	err = interceptor(nil, &fakeServerStream{ctx: ctxWithKey("secret")}, nil, handler)
	if err != nil {
		t.Fatalf("correct key: unexpected error %v", err)
	}
	if !handlerCalled {
		t.Error("handler must run when auth succeeds")
	}
}
