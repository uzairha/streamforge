package apiserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPrometheusClient_InstantQuery(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		status  int
		want    float64
		wantErr bool
	}{
		{
			name:   "single result",
			body:   `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"42.5"]}]}}`,
			status: http.StatusOK,
			want:   42.5,
		},
		{
			name:   "no matching series",
			body:   `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			status: http.StatusOK,
			want:   0,
		},
		{
			name:    "prometheus reports an error",
			body:    `{"status":"error","error":"bad promql"}`,
			status:  http.StatusOK,
			wantErr: true,
		},
		{
			name:    "non-200 status",
			body:    `internal error`,
			status:  http.StatusInternalServerError,
			wantErr: true,
		},
		{
			name:    "malformed json",
			body:    `not json`,
			status:  http.StatusOK,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.Contains(r.URL.Path, "/api/v1/query") {
					t.Errorf("unexpected path %q", r.URL.Path)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			c := NewPrometheusClient(srv.URL)
			got, err := c.InstantQuery(context.Background(), `sum(streamforge_events_ingested_total)`)
			if (err != nil) != tc.wantErr {
				t.Fatalf("InstantQuery() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Errorf("InstantQuery() = %v, want %v", got, tc.want)
			}
		})
	}
}
