//go:build integration

package kafka

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pelletier/go-toml/v2"

	"github.com/example/earthquake-service/internal/eventstream"
)

var integrationConfigPath = flag.String("integration-config", "", "path to integration-test TOML configuration")

func TestProduceConsumeAndCommit(t *testing.T) {
	if *integrationConfigPath == "" {
		t.Skip("-integration-config is not set")
	}
	data, err := os.ReadFile(*integrationConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	var targets struct {
		Database struct {
			URL string `toml:"url"`
		} `toml:"database"`
		Kafka struct {
			Broker string `toml:"broker"`
		} `toml:"kafka"`
	}
	decoder := toml.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&targets); err != nil {
		t.Fatal(err)
	}
	if targets.Kafka.Broker == "" {
		t.Fatal("integration configuration kafka.broker is required")
	}
	broker := targets.Kafka.Broker
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	id := uuid.NewString()
	producer, err := NewProducer([]string{broker}, "shaker-kafka-test-producer-"+id, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	consumer, err := NewConsumer([]string{broker}, "shaker-kafka-test-consumer-"+id,
		"shaker-kafka-test-"+id, eventstream.ProviderObservationsTopic, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		consumer.AllowRebalance()
		consumer.Close()
	}()
	if err := producer.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	want := Message{Topic: eventstream.ProviderObservationsTopic, Key: "test:" + id,
		Value: []byte(fmt.Sprintf(`{"message_id":%q}`, id)), Headers: map[string]string{"schema": "test.v1"}}
	if err := producer.Publish(ctx, want); err != nil {
		t.Fatal(err)
	}
	var record Record
	for {
		record, err = consumer.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if record.Key == want.Key {
			break
		}
		consumer.AllowRebalance()
	}
	if record.Key != want.Key || string(record.Value) != string(want.Value) || record.Headers["schema"] != "test.v1" {
		t.Fatalf("record=%+v", record)
	}
	if err := consumer.Commit(ctx, record); err != nil {
		t.Fatal(err)
	}
}
