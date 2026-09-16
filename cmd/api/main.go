// Command api serves the StreamForge gRPC API (with a REST/JSON gateway and
// a minimal demo dashboard) over the live event stream and the aggregate
// store. Every RPC — over gRPC directly or through the REST gateway —
// requires an API key when one is configured (see internal/apiserver).
package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
	"github.com/uzairha/streamforge/internal/apiserver"
	"github.com/uzairha/streamforge/internal/config"
	"github.com/uzairha/streamforge/internal/kafka"
	"github.com/uzairha/streamforge/internal/store"
	"github.com/uzairha/streamforge/internal/telemetry"
)

func main() {
	cfg, err := config.Load("api")
	log := telemetry.NewLogger(cfg.LogLevel, "api")
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metrics := telemetry.NewAPIMetrics()
	go func() {
		if serr := metrics.Serve(ctx, cfg.MetricsAddr); serr != nil {
			log.Error("metrics server", "err", serr)
		}
	}()
	log.Info("metrics + health listening", "addr", cfg.MetricsAddr)

	if cfg.APIKey == "" {
		log.Warn("API_KEY is unset; every request will be accepted without authentication")
	}

	db, err := store.Open(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	if serr := db.EnsureSchema(ctx); serr != nil {
		log.Error("ensure schema", "err", serr)
		os.Exit(1)
	}

	prom := apiserver.NewPrometheusClient(cfg.PrometheusURL)

	newTailer := func() (apiserver.EventTailer, error) {
		return kafka.NewTailConsumer(cfg.KafkaBrokers, cfg.TopicNormalized)
	}
	srv := apiserver.NewServer(newTailer, db, prom, time.Now())

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(apiserver.UnaryAuthInterceptor(cfg.APIKey), apiserver.UnaryMetricsInterceptor(metrics)),
		grpc.ChainStreamInterceptor(apiserver.StreamAuthInterceptor(cfg.APIKey), apiserver.StreamMetricsInterceptor(metrics)),
	)
	streamforgev1.RegisterStreamForgeServiceServer(grpcServer, srv)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Error("grpc listen", "addr", cfg.GRPCAddr, "err", err)
		os.Exit(1)
	}
	go func() {
		if serr := grpcServer.Serve(lis); serr != nil && !errors.Is(serr, grpc.ErrServerStopped) {
			log.Error("grpc serve", "err", serr)
		}
	}()
	log.Info("grpc listening", "addr", cfg.GRPCAddr)

	// The REST gateway dials the gRPC server above over loopback, so both
	// transports run through the exact same interceptor chain (auth,
	// metrics) rather than the gateway bypassing it via an in-process call.
	gwMux := runtime.NewServeMux(runtime.WithIncomingHeaderMatcher(headerMatcher))
	// ctx (not a short-lived dial timeout) is what RegisterStreamForgeServiceHandlerFromEndpoint
	// ties the gateway's underlying connection to: it closes that
	// connection when this ctx is Done, so passing anything shorter-lived
	// would sever the gateway from its backend the moment that timeout
	// elapsed, well before the server actually shuts down.
	if gwErr := streamforgev1.RegisterStreamForgeServiceHandlerFromEndpoint(
		ctx, gwMux, cfg.GRPCAddr, []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	); gwErr != nil {
		log.Error("register rest gateway", "err", gwErr)
		os.Exit(1)
	}

	httpMux := http.NewServeMux()
	httpMux.Handle("/v1/", gwMux)
	httpMux.Handle("/", apiserver.DashboardHandler())

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = httpServer.Shutdown(shutCtx)
	}()

	log.Info("api starting", "grpc", cfg.GRPCAddr, "http", cfg.HTTPAddr)

	if serr := httpServer.ListenAndServe(); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
		log.Error("http serve", "err", serr)
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelStop()
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-stopCtx.Done():
		grpcServer.Stop()
	}

	log.Info("api stopped")
}

// headerMatcher forwards the client-facing X-Api-Key REST header onto the
// gRPC metadata key the auth interceptor checks, in addition to the
// gateway's default forwarding rules.
func headerMatcher(key string) (string, bool) {
	if strings.EqualFold(key, "X-Api-Key") {
		return "x-api-key", true
	}
	return runtime.DefaultHeaderMatcher(key)
}
