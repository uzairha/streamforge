package kafka

import "testing"

func TestNewConsumer_Validation(t *testing.T) {
	cases := []struct {
		name    string
		brokers []string
		topic   string
		group   string
	}{
		{"no brokers", nil, "raw-events", "g"},
		{"empty topic", []string{"localhost:9092"}, "", "g"},
		{"empty group", []string{"localhost:9092"}, "raw-events", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewConsumer(tc.brokers, tc.topic, tc.group); err == nil {
				t.Errorf("NewConsumer(%v, %q, %q): want error", tc.brokers, tc.topic, tc.group)
			}
		})
	}
}
