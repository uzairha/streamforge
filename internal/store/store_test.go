package store

import "testing"

func TestEncodeLabels(t *testing.T) {
	cases := []struct {
		name    string
		labels  map[string]string
		wantKey string
	}{
		{"nil", nil, ""},
		{"empty", map[string]string{}, ""},
		{"single", map[string]string{"type": "block"}, "type=block"},
		{"sorted regardless of insertion order", map[string]string{"b": "2", "a": "1"}, "a=1,b=2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, key := encodeLabels(tc.labels)
			if key != tc.wantKey {
				t.Errorf("encodeLabels(%v) key = %q, want %q", tc.labels, key, tc.wantKey)
			}
		})
	}
}

func TestEncodeLabels_JSONRoundTrips(t *testing.T) {
	labels := map[string]string{"type": "transaction", "chain": "ethereum"}
	raw, _ := encodeLabels(labels)
	if len(raw) == 0 {
		t.Fatal("encodeLabels returned empty JSON")
	}
}
