package telemetry

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAggregatorMetrics_HealthzAndMetrics(t *testing.T) {
	m := NewAggregatorMetrics()
	m.WindowsClosed.WithLabelValues("events_total").Inc()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", resp.StatusCode)
	}

	resp2, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	body, _ := io.ReadAll(resp2.Body)
	s := string(body)
	for _, want := range []string{
		"streamforge_aggregator_windows_closed_total",
		"streamforge_aggregator_events_late_total",
		"go_goroutines",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("exposition missing %q", want)
		}
	}
}

func TestAggregatorMetrics_IndependentRegistries(t *testing.T) {
	a := NewAggregatorMetrics()
	b := NewAggregatorMetrics()
	a.EventsConsumed.Add(3)

	srv := httptest.NewServer(b.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), `streamforge_aggregator_events_consumed_total 3`) {
		t.Fatal("registry b exposed a metric written only to registry a")
	}
}
