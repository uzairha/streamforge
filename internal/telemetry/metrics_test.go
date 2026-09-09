package telemetry

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetrics_HealthzAndMetrics(t *testing.T) {
	m := NewMetrics()
	m.EventsIngested.WithLabelValues("synthetic", "EVENT_TYPE_TRANSACTION").Inc()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	t.Run("healthz", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), `"status":"ok"`) {
			t.Fatalf("body = %q", body)
		}
	})

	t.Run("metrics exposition", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		s := string(body)
		for _, want := range []string{
			"streamforge_events_ingested_total",
			"go_goroutines",              // Go collector registered
			"process_start_time_seconds", // process collector registered
		} {
			if !strings.Contains(s, want) {
				t.Errorf("exposition missing %q", want)
			}
		}
	})
}

func TestMetrics_IndependentRegistries(t *testing.T) {
	// Constructing twice must not panic, and each registry must be isolated.
	a := NewMetrics()
	b := NewMetrics()
	a.EventsProduced.WithLabelValues("raw-events").Add(3)

	srv := httptest.NewServer(b.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), `streamforge_events_produced_total{topic="raw-events"} 3`) {
		t.Fatal("registry b exposed a metric written only to registry a")
	}
}
