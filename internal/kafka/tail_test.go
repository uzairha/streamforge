package kafka

import "testing"

func TestNewTailConsumer_Validation(t *testing.T) {
	if _, err := NewTailConsumer(nil, "normalized-events"); err == nil {
		t.Error("no brokers: want error")
	}
	if _, err := NewTailConsumer([]string{"localhost:9092"}, ""); err == nil {
		t.Error("empty topic: want error")
	}
}
