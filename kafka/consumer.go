package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/lovoo/goka/multierr"
)

const (
	// size of sarama buffer for consumer and producer
	defaultChannelBufferSize = 256

	// time sarama-cluster assumes the processing of an event may take
	defaultMaxProcessingTime = 1 * time.Second

	// producer flush configuration
	defaultFlushFrequency     = 100 * time.Millisecond
	defaultFlushBytes         = 64 * 1024
	defaultProducerMaxRetries = 10
)

const (
	// OffsetNewest defines the newest offset to read from using the consumer
	OffsetNewest = -1
	// OffsetOldest defines the oldest offset to read from using the consumer
	OffsetOldest = -2
)

// Consumer abstracts a kafka consumer
type Consumer interface {
	Events() <-chan Event

	// group consume assumes co-partioned topics
	Subscribe(topics map[string]int64) error
	AddGroupPartition(partition int32)
	Commit(topic string, partition int32, offset int64) error

	// consume individual topic/partitions
	AddPartition(topic string, partition int32, initialOffset int64) error
	RemovePartition(topic string, partition int32) error

	// Close stops closes the events channel
	Close() error
}

type saramaConsumer struct {
	groupConsumer  *groupConsumer
	simpleConsumer *simpleConsumer
	events         chan Event
}

// NewSaramaConsumer creates a new Consumer using sarama
func NewSaramaConsumer(brokers []string, group string, config *cluster.Config) (Consumer, error) {
	chsize := config.Config.ChannelBufferSize
	if chsize == 0 {
		chsize = defaultChannelBufferSize
	}
	events := make(chan Event, chsize)

	g, err := newGroupConsumer(brokers, group, events, config)
	if err != nil {
		return nil, err
	}

	// since simple consumer only handle tables, be sure to start from oldest
	simpleConfig := config.Config // copy config
	simpleConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	c, err := newSimpleConsumer(brokers, events, &simpleConfig)
	if err != nil {
		return nil, err
	}

	return &saramaConsumer{
		groupConsumer:  g,
		simpleConsumer: c,
		events:         events,
	}, nil
}

func (c *saramaConsumer) Close() error {
	// we want to close the events-channel regardless of any errors closing
	// the consumers
	defer close(c.events)
	var errs multierr.Errors
	if err := c.simpleConsumer.Close(); err != nil {
		errs.Collect(err)
	}
	if err := c.groupConsumer.Close(); err != nil {
		errs.Collect(err)
	}
	return errs.NilOrError()
}

func (c *saramaConsumer) Events() <-chan Event {
	return c.events
}

// group consume assumes co-partioned topics
func (c *saramaConsumer) Subscribe(topics map[string]int64) error {
	return c.groupConsumer.Subscribe(topics)
}
func (c *saramaConsumer) AddGroupPartition(partition int32) {
	c.groupConsumer.AddGroupPartition(partition)
}
func (c *saramaConsumer) Commit(topic string, partition int32, offset int64) error {
	return c.groupConsumer.Commit(topic, partition, offset)
}

func (c *saramaConsumer) AddPartition(topic string, partition int32, initialOffset int64) error {
	return c.simpleConsumer.AddPartition(topic, partition, int64(initialOffset))
}
func (c *saramaConsumer) RemovePartition(topic string, partition int32) error {
	return c.simpleConsumer.RemovePartition(topic, partition)
}
