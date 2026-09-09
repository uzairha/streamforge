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

	// Aggregator
	WindowSize time.Duration

	// Store (aggregator, api)
	PostgresDSN string

	// API
	GRPCAddr string
	HTTPAddr string
	APIKey   string

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
		EthWSURL:        env("ETH_WS_URL", "wss://ethereum-rpc.publicnode.com"),
		PostgresDSN:     env("POSTGRES_DSN", "postgres://streamforge:streamforge@localhost:5432/streamforge?sslmode=disable"),
		GRPCAddr:        env("GRPC_ADDR", ":9090"),
		HTTPAddr:        env("HTTP_ADDR", ":8080"),
		APIKey:          env("API_KEY", ""),
		MetricsAddr:     env("METRICS_ADDR", ":2112"),
		OTLPEndpoint:    env("OTLP_ENDPOINT", "localhost:4317"),
	}

	var err error
	if c.SyntheticRate, err = envFloat("SYNTHETIC_RATE", 25); err != nil {
		return Config{}, err
	}
	if c.WindowSize, err = envDuration("WINDOW_SIZE", time.Minute); err != nil {
		return Config{}, err
	}
	if c.TracingEnabled, err = envBool("TRACING_ENABLED", false); err != nil {
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
