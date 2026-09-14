package telemetry

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNormalizerMetrics_HealthzAndMetrics(t *testing.T) {
	m := NewNormalizerMetrics()
	m.EventsConsumed.WithLabelValues("raw-events").Inc()
	m.EventsInvalid.WithLabelValues("missing_id").Inc()
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
		"streamforge_normalizer_events_consumed_total",
		"streamforge_normalizer_events_invalid_total",
		"go_goroutines",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("exposition missing %q", want)
		}
	}
}

func TestNormalizerMetrics_IndependentRegistries(t *testing.T) {
	a := NewNormalizerMetrics()
	b := NewNormalizerMetrics()
	a.EventsProduced.WithLabelValues("normalized-events").Add(3)

	srv := httptest.NewServer(b.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), `streamforge_normalizer_events_produced_total{topic="normalized-events"} 3`) {
		t.Fatal("registry b exposed a metric written only to registry a")
	}
}
