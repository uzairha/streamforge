// Command api serves the StreamForge gRPC API (with a REST/JSON gateway) over
// the live event stream and the aggregate store. Skeleton in M0; the gRPC
// server lands in M4.
package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/uzairha/streamforge/internal/config"
	"github.com/uzairha/streamforge/internal/telemetry"
)

func main() {
	cfg, err := config.Load("api")
	log := telemetry.NewLogger(cfg.LogLevel, "api")
	if err != nil {
		log.Error("load config", "err", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("api starting", "grpc", cfg.GRPCAddr, "http", cfg.HTTPAddr)
	log.Warn("api is an M0 skeleton; gRPC server lands in M4")

	<-ctx.Done()
	log.Info("api stopped")
}
