// Package telemetry holds cross-cutting observability helpers shared by every
// service. M0 ships structured logging; Prometheus metrics and OpenTelemetry
// tracing land in later milestones.
package telemetry

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger returns a JSON slog.Logger tagged with the service name. level is
// one of debug|info|warn|error and defaults to info.
func NewLogger(level, service string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("service", service)
}
