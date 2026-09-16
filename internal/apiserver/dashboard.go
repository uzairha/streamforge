package apiserver

import (
	_ "embed"
	"net/http"
)

//go:embed dashboard.html
var dashboardHTML []byte

// DashboardHandler serves the embedded static demo dashboard, which polls
// the REST gateway's /v1/stats and /v1/aggregates endpoints client-side.
func DashboardHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(dashboardHTML)
	})
}
