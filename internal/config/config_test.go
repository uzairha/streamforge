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
	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:19092" {
		t.Errorf("KafkaBrokers = %v, want [localhost:19092]", cfg.KafkaBrokers)
	}
}

func TestLoad_Overrides(t *testing.T) {
	t.Setenv("SYNTHETIC_RATE", "100")
	t.Setenv("WINDOW_SIZE", "30s")
	t.Setenv("KAFKA_BROKERS", "a:1, b:2 ,c:3")
	t.Setenv("TRACING_ENABLED", "true")

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
	if got, want := cfg.KafkaBrokers, []string{"a:1", "b:2", "c:3"}; !equal(got, want) {
		t.Errorf("KafkaBrokers = %v, want %v", got, want)
	}
	if !cfg.TracingEnabled {
		t.Error("TracingEnabled = false, want true")
	}
}

func TestLoad_InvalidValues(t *testing.T) {
	cases := map[string]map[string]string{
		"negative rate":    {"SYNTHETIC_RATE": "-5"},
		"zero rate":        {"SYNTHETIC_RATE": "0"},
		"unparseable rate": {"SYNTHETIC_RATE": "fast"},
		"unknown source":   {"SOURCE": "dogecoin"},
		"bad duration":     {"WINDOW_SIZE": "10 fortnights"},
		"bad bool":         {"TRACING_ENABLED": "maybe"},
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
