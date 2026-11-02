package kafka

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFetchErrorSkipsDataLoss(t *testing.T) {
	consumer := &Consumer{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	fetches := kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "observations", Partitions: []kgo.FetchPartition{
		{Partition: 1, Err: &kgo.ErrDataLoss{Topic: "observations", Partition: 1, ConsumedTo: 40, ResetTo: 0}},
	}}}}}
	if err := consumer.fetchError(fetches); err != nil {
		t.Fatalf("data loss stopped the consumer: %v", err)
	}
	fetches[0].Topics[0].Partitions = append(fetches[0].Topics[0].Partitions,
		kgo.FetchPartition{Partition: 2, Err: errors.New("broker failure")})
	if err := consumer.fetchError(fetches); err == nil {
		t.Fatal("other fetch errors must still stop the consumer")
	}
}
