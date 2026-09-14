package kafka

import "testing"

func TestNewAggregateProducer_Validation(t *testing.T) {
	if _, err := NewAggregateProducer(nil, "aggregates", nil); err == nil {
		t.Error("no brokers: want error")
	}
	if _, err := NewAggregateProducer([]string{"localhost:9092"}, "", nil); err == nil {
		t.Error("empty topic: want error")
	}
}
