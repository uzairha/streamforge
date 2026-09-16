package apiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// PrometheusQuerier runs a PromQL instant query and returns its scalar
// result. GetStats depends on this interface (not a concrete client) so
// tests can supply canned values instead of running a real Prometheus.
type PrometheusQuerier interface {
	// InstantQuery evaluates promql "now" and returns the single resulting
	// scalar value. A query with no matching series is a valid, expected
	// state (e.g. right after a fresh deployment) and returns 0, not an
	// error.
	InstantQuery(ctx context.Context, promql string) (float64, error)
}

// PrometheusClient queries a real Prometheus server's HTTP API.
type PrometheusClient struct {
	baseURL string
	http    *http.Client
}

// NewPrometheusClient targets the Prometheus server at baseURL (e.g.
// "http://localhost:9091").
func NewPrometheusClient(baseURL string) *PrometheusClient {
	return &PrometheusClient{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 5 * time.Second},
	}
}

// promResponse is the subset of Prometheus's instant-query response schema
// (https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries)
// this client reads.
type promResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Data   struct {
		Result []struct {
			Value [2]any `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// InstantQuery implements PrometheusQuerier.
func (c *PrometheusClient) InstantQuery(ctx context.Context, promql string) (float64, error) {
	reqURL := c.baseURL + "/api/v1/query?" + url.Values{"query": {promql}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, fmt.Errorf("prometheus: build request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("prometheus: query %q: %w", promql, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("prometheus: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("prometheus: query %q: status %d: %s", promql, resp.StatusCode, body)
	}

	var parsed promResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return 0, fmt.Errorf("prometheus: decode response: %w", err)
	}
	if parsed.Status != "success" {
		return 0, fmt.Errorf("prometheus: query %q failed: %s", promql, parsed.Error)
	}
	if len(parsed.Data.Result) == 0 {
		return 0, nil
	}

	// value is a 2-element [timestamp, "stringified float"] pair per the
	// documented schema.
	raw, ok := parsed.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("prometheus: query %q: unexpected value shape %v", promql, parsed.Data.Result[0].Value)
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("prometheus: query %q: parse value %q: %w", promql, raw, err)
	}
	return v, nil
}
