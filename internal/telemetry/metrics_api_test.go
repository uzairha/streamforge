package telemetry

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIMetrics_HealthzAndMetrics(t *testing.T) {
	m := NewAPIMetrics()
	m.RequestsTotal.WithLabelValues("/streamforge.v1.StreamForgeService/GetStats", "OK").Inc()
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
		"streamforge_api_requests_total",
		"streamforge_api_auth_failures_total",
		"streamforge_api_stream_events_active",
		"go_goroutines",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("exposition missing %q", want)
		}
	}
}

func TestAPIMetrics_IndependentRegistries(t *testing.T) {
	a := NewAPIMetrics()
	b := NewAPIMetrics()
	a.AuthFailuresTotal.Add(3)

	srv := httptest.NewServer(b.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "streamforge_api_auth_failures_total 3") {
		t.Fatal("registry b exposed a metric written only to registry a")
	}
}
