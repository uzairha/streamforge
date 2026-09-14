package config

import (
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load("ingester")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ServiceName != "ingester" {
		t.Errorf("ServiceName = %q, want ingester", cfg.ServiceName)
	}
	if cfg.TopicRaw != "raw-events" {
		t.Errorf("TopicRaw = %q, want raw-events", cfg.TopicRaw)
	}
	if cfg.ConsumerGroup != "streamforge-ingester" {
		t.Errorf("ConsumerGroup = %q, want streamforge-ingester", cfg.ConsumerGroup)
	}
	if cfg.Source != "synthetic" {
		t.Errorf("Source = %q, want synthetic", cfg.Source)
	}
	if cfg.SyntheticRate != 25 {
		t.Errorf("SyntheticRate = %v, want 25", cfg.SyntheticRate)
	}
	if cfg.WindowSize != time.Minute {
		t.Errorf("WindowSize = %v, want 1m", cfg.WindowSize)
	}
	if cfg.AllowedLateness != 30*time.Second {
		t.Errorf("AllowedLateness = %v, want 30s", cfg.AllowedLateness)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 10s", cfg.ShutdownTimeout)
	}
	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:19092" {
		t.Errorf("KafkaBrokers = %v, want [localhost:19092]", cfg.KafkaBrokers)
	}
	if cfg.EthWSURL != "wss://ethereum.publicnode.com" {
		t.Errorf("EthWSURL = %q, want the public keyless endpoint", cfg.EthWSURL)
	}
	if cfg.EthMaxBackoff != 30*time.Second {
		t.Errorf("EthMaxBackoff = %v, want 30s", cfg.EthMaxBackoff)
	}
	if !cfg.EthFetchBodies {
		t.Error("EthFetchBodies = false, want true")
	}
	if cfg.EthDedupWindow != 8192 {
		t.Errorf("EthDedupWindow = %d, want 8192", cfg.EthDedupWindow)
	}
}

func TestLoad_Overrides(t *testing.T) {
	t.Setenv("SYNTHETIC_RATE", "100")
	t.Setenv("WINDOW_SIZE", "30s")
	t.Setenv("ALLOWED_LATENESS", "5s")
	t.Setenv("KAFKA_BROKERS", "a:1, b:2 ,c:3")
	t.Setenv("TRACING_ENABLED", "true")
	t.Setenv("SOURCE", "ethereum")
	t.Setenv("ETH_WS_URL", "ws://localhost:8546")
	t.Setenv("ETH_MAX_BACKOFF", "5s")
	t.Setenv("ETH_FETCH_BODIES", "false")
	t.Setenv("ETH_DEDUP_WINDOW", "256")

	cfg, err := Load("aggregator")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.SyntheticRate != 100 {
		t.Errorf("SyntheticRate = %v, want 100", cfg.SyntheticRate)
	}
	if cfg.WindowSize != 30*time.Second {
		t.Errorf("WindowSize = %v, want 30s", cfg.WindowSize)
	}
	if cfg.AllowedLateness != 5*time.Second {
		t.Errorf("AllowedLateness = %v, want 5s", cfg.AllowedLateness)
	}
	if got, want := cfg.KafkaBrokers, []string{"a:1", "b:2", "c:3"}; !equal(got, want) {
		t.Errorf("KafkaBrokers = %v, want %v", got, want)
	}
	if !cfg.TracingEnabled {
		t.Error("TracingEnabled = false, want true")
	}
	if cfg.EthWSURL != "ws://localhost:8546" {
		t.Errorf("EthWSURL = %q, want ws://localhost:8546", cfg.EthWSURL)
	}
	if cfg.EthMaxBackoff != 5*time.Second {
		t.Errorf("EthMaxBackoff = %v, want 5s", cfg.EthMaxBackoff)
	}
	if cfg.EthFetchBodies {
		t.Error("EthFetchBodies = true, want false")
	}
	if cfg.EthDedupWindow != 256 {
		t.Errorf("EthDedupWindow = %d, want 256", cfg.EthDedupWindow)
	}
}

func TestLoad_InvalidValues(t *testing.T) {
	cases := map[string]map[string]string{
		"negative rate":      {"SYNTHETIC_RATE": "-5"},
		"zero rate":          {"SYNTHETIC_RATE": "0"},
		"unparseable rate":   {"SYNTHETIC_RATE": "fast"},
		"unknown source":     {"SOURCE": "dogecoin"},
		"bad duration":       {"WINDOW_SIZE": "10 fortnights"},
		"negative lateness":  {"ALLOWED_LATENESS": "-1s"},
		"bad lateness value": {"ALLOWED_LATENESS": "soon"},
		"bad bool":           {"TRACING_ENABLED": "maybe"},
		"negative shutdown":  {"SHUTDOWN_TIMEOUT": "-1s"},
		"bad shutdown value": {"SHUTDOWN_TIMEOUT": "soon"},
		"bad eth backoff":    {"SOURCE": "ethereum", "ETH_MAX_BACKOFF": "not-a-duration"},
		"non-positive eth backoff": {
			"SOURCE": "ethereum", "ETH_MAX_BACKOFF": "0s",
		},
		"bad eth dedup window": {
			"SOURCE": "ethereum", "ETH_DEDUP_WINDOW": "-1",
		},
		"eth ws url missing scheme": {
			"SOURCE": "ethereum", "ETH_WS_URL": "ethereum-rpc.publicnode.com",
		},
		"bad eth fetch bodies bool": {
			"SOURCE": "ethereum", "ETH_FETCH_BODIES": "sure",
		},
	}
	for name, envs := range cases {
		t.Run(name, func(t *testing.T) {
			for k, v := range envs {
				t.Setenv(k, v)
			}
			if _, err := Load("ingester"); err == nil {
				t.Fatalf("Load succeeded, want error for %v", envs)
			}
		})
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
