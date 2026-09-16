// Package config loads StreamForge service settings from the environment.
//
// Every binary shares one Config struct and reads only the fields it needs;
// unused fields keep their defaults. Load fails fast on malformed or
// out-of-range values so a service never starts half-configured.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the union of settings across all StreamForge services.
type Config struct {
	// Common
	ServiceName string
	LogLevel    string

	// Kafka / Redpanda
	KafkaBrokers    []string
	TopicRaw        string
	TopicNormalized string
	TopicAggregates string
	ConsumerGroup   string

	// Event source (ingester)
	Source          string // "synthetic" | "ethereum"
	SyntheticRate   float64
	SyntheticChains []string
	EthWSURL        string
	EthMaxBackoff   time.Duration // ceiling for the reconnect backoff
	EthFetchBodies  bool          // fetch full blocks and emit one event per tx
	EthDedupWindow  int           // recent event IDs kept to suppress replays

	// Graceful shutdown budget for draining in-flight work.
	ShutdownTimeout time.Duration

	// Aggregator
	WindowSize      time.Duration
	AllowedLateness time.Duration // watermark lag before a window closes

	// Store (aggregator, api)
	PostgresDSN string

	// API
	GRPCAddr      string
	HTTPAddr      string
	APIKey        string // empty disables auth — fine for local dev, required otherwise
	PrometheusURL string // queried by GetStats for pipeline throughput

	// Telemetry
	MetricsAddr    string
	OTLPEndpoint   string
	TracingEnabled bool
}

// Load reads configuration for the named service.
func Load(service string) (Config, error) {
	c := Config{
		ServiceName:     service,
		LogLevel:        env("LOG_LEVEL", "info"),
		KafkaBrokers:    envList("KAFKA_BROKERS", []string{"localhost:19092"}),
		TopicRaw:        env("TOPIC_RAW", "raw-events"),
		TopicNormalized: env("TOPIC_NORMALIZED", "normalized-events"),
		TopicAggregates: env("TOPIC_AGGREGATES", "aggregates"),
		ConsumerGroup:   env("CONSUMER_GROUP", "streamforge-"+service),
		Source:          env("SOURCE", "synthetic"),
		SyntheticChains: envList("SYNTHETIC_CHAINS", []string{"synthetic"}),
		EthWSURL:        env("ETH_WS_URL", "wss://ethereum.publicnode.com"),
		PostgresDSN:     env("POSTGRES_DSN", "postgres://streamforge:streamforge@localhost:5432/streamforge?sslmode=disable"),
		GRPCAddr:        env("GRPC_ADDR", ":9090"),
		HTTPAddr:        env("HTTP_ADDR", ":8080"),
		APIKey:          env("API_KEY", ""),
		PrometheusURL:   env("PROMETHEUS_URL", "http://localhost:9091"),
		MetricsAddr:     env("METRICS_ADDR", ":2112"),
		OTLPEndpoint:    env("OTLP_ENDPOINT", "localhost:4317"),
	}

	var err error
	if c.SyntheticRate, err = envFloat("SYNTHETIC_RATE", 25); err != nil {
		return Config{}, err
	}
	if c.ShutdownTimeout, err = envDuration("SHUTDOWN_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if c.WindowSize, err = envDuration("WINDOW_SIZE", time.Minute); err != nil {
		return Config{}, err
	}
	if c.AllowedLateness, err = envDuration("ALLOWED_LATENESS", 30*time.Second); err != nil {
		return Config{}, err
	}
	if c.TracingEnabled, err = envBool("TRACING_ENABLED", false); err != nil {
		return Config{}, err
	}
	if c.EthMaxBackoff, err = envDuration("ETH_MAX_BACKOFF", 30*time.Second); err != nil {
		return Config{}, err
	}
	if c.EthFetchBodies, err = envBool("ETH_FETCH_BODIES", true); err != nil {
		return Config{}, err
	}
	if c.EthDedupWindow, err = envInt("ETH_DEDUP_WINDOW", 8192); err != nil {
		return Config{}, err
	}

	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) validate() error {
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS must not be empty")
	}
	if c.Source != "synthetic" && c.Source != "ethereum" {
		return fmt.Errorf("SOURCE must be 'synthetic' or 'ethereum', got %q", c.Source)
	}
	if c.SyntheticRate <= 0 {
		return fmt.Errorf("SYNTHETIC_RATE must be > 0, got %v", c.SyntheticRate)
	}
	if c.WindowSize <= 0 {
		return fmt.Errorf("WINDOW_SIZE must be > 0, got %v", c.WindowSize)
	}
	if c.AllowedLateness < 0 {
		return fmt.Errorf("ALLOWED_LATENESS must be >= 0, got %v", c.AllowedLateness)
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be > 0, got %v", c.ShutdownTimeout)
	}
	if c.Source == "ethereum" {
		if !strings.HasPrefix(c.EthWSURL, "ws://") && !strings.HasPrefix(c.EthWSURL, "wss://") {
			return fmt.Errorf("ETH_WS_URL must be a ws:// or wss:// URL, got %q", c.EthWSURL)
		}
		if c.EthMaxBackoff <= 0 {
			return fmt.Errorf("ETH_MAX_BACKOFF must be > 0, got %v", c.EthMaxBackoff)
		}
		if c.EthDedupWindow <= 0 {
			return fmt.Errorf("ETH_DEDUP_WINDOW must be > 0, got %d", c.EthDedupWindow)
		}
	}
	return nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envList(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func envFloat(key string, def float64) (float64, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return f, nil
}

func envBool(key string, def bool) (bool, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return b, nil
}

func envInt(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
