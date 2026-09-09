package kafka

import (
	"testing"

	streamforgev1 "github.com/uzairha/streamforge/gen/streamforge/v1"
)

func TestNewProducer_Validation(t *testing.T) {
	if _, err := NewProducer(nil, "raw-events", nil); err == nil {
		t.Error("no brokers: want error")
	}
	if _, err := NewProducer([]string{"localhost:9092"}, "", nil); err == nil {
		t.Error("empty topic: want error")
	}
}

func TestPartitionKey(t *testing.T) {
	cases := []struct {
		name string
		ev   *streamforgev1.ChainEvent
		want string
	}{
		{"from address wins", &streamforgev1.ChainEvent{FromAddress: "0xabc", BlockHash: "0xdef", Chain: "eth"}, "0xabc"},
		{"block hash fallback", &streamforgev1.ChainEvent{BlockHash: "0xdef", Chain: "eth"}, "0xdef"},
		{"chain last resort", &streamforgev1.ChainEvent{Chain: "eth"}, "eth"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := partitionKey(tc.ev); got != tc.want {
				t.Fatalf("partitionKey = %q, want %q", got, tc.want)
			}
		})
	}
}
