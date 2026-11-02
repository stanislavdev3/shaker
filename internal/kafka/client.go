package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Message struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

type Record struct {
	Message
	Partition int
	Offset    int64
	Timestamp time.Time
	record    *kgo.Record
}

type Producer struct{ client *kgo.Client }

func NewProducer(brokers []string, clientID string, maxMessageBytes int32) (*Producer, error) {
	if len(brokers) == 0 || clientID == "" || maxMessageBytes <= 0 {
		return nil, errors.New("invalid Kafka producer configuration")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(maxMessageBytes),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Publish(ctx context.Context, message Message) error {
	if message.Topic == "" || message.Key == "" || len(message.Value) == 0 {
		return errors.New("invalid Kafka message")
	}
	record := &kgo.Record{Topic: message.Topic, Key: []byte(message.Key), Value: message.Value}
	for key, value := range message.Headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}
	return p.client.ProduceSync(ctx, record).FirstErr()
}

func (p *Producer) Ping(ctx context.Context) error { return p.client.Ping(ctx) }
func (p *Producer) Close()                         { p.client.Close() }

type Consumer struct {
	client *kgo.Client
	log    *slog.Logger
}

func NewConsumer(brokers []string, clientID, groupID, topic string, maxMessageBytes int32,
	log *slog.Logger,
) (*Consumer, error) {
	if len(brokers) == 0 || clientID == "" || groupID == "" || topic == "" || maxMessageBytes <= 0 || log == nil {
		return nil, errors.New("invalid Kafka consumer configuration")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxBytes(maxMessageBytes),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: client, log: log}, nil
}

// Next returns the next record. With BlockRebalanceOnPoll every poll holds off rebalances, and the LeaveGroup in
// Close, until AllowRebalance: the caller allows them after handling the record, so Next does on every other way
// out, or Close blocks forever.
func (c *Consumer) Next(ctx context.Context) (Record, error) {
	for {
		fetches := c.client.PollRecords(ctx, 1)
		if err := ctx.Err(); err != nil {
			c.client.AllowRebalance()
			return Record{}, err
		}
		if err := c.fetchError(fetches); err != nil {
			c.client.AllowRebalance()
			return Record{}, err
		}
		var fetched *kgo.Record
		fetches.EachRecord(func(record *kgo.Record) { fetched = record })
		if fetched == nil {
			c.client.AllowRebalance()
			continue
		}
		headers := make(map[string]string, len(fetched.Headers))
		for _, header := range fetched.Headers {
			headers[header.Key] = string(header.Value)
		}
		return Record{
			Message:   Message{Topic: fetched.Topic, Key: string(fetched.Key), Value: fetched.Value, Headers: headers},
			Partition: int(fetched.Partition), Offset: fetched.Offset, Timestamp: fetched.Timestamp, record: fetched,
		}, nil
	}
}

// fetchError skips data loss: the broker lost records past the committed offset (an unclean shutdown of the
// single-replica Kafka) and the client has already moved to the new log end, so stopping would only stall the group.
func (c *Consumer) fetchError(fetches kgo.Fetches) error {
	var errs []kgo.FetchError
	for _, fetchErr := range fetches.Errors() {
		var dataLoss *kgo.ErrDataLoss
		if errors.As(fetchErr.Err, &dataLoss) {
			c.log.Warn("Kafka lost committed records, consuming from the new log end", "error", fetchErr.Err)
			continue
		}
		errs = append(errs, fetchErr)
	}
	if len(errs) > 0 {
		return fmt.Errorf("fetch Kafka records: %v", errs)
	}
	return nil
}

func (c *Consumer) Commit(ctx context.Context, record Record) error {
	if record.record == nil {
		return errors.New("kafka record cannot be committed")
	}
	return c.client.CommitRecords(ctx, record.record)
}

func (c *Consumer) AllowRebalance()                { c.client.AllowRebalance() }
func (c *Consumer) Ping(ctx context.Context) error { return c.client.Ping(ctx) }
func (c *Consumer) Close()                         { c.client.Close() }
