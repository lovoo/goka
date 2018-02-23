package kafka

import (
	"hash"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	metrics "github.com/rcrowley/go-metrics"
)

// ConsumerBuilder creates a Kafka consumer.
type ConsumerBuilder func(brokers []string, group, clientID string) (Consumer, error)

// DefaultConsumerBuilder creates a Kafka consumer using the Sarama library.
func DefaultConsumerBuilder(brokers []string, group, clientID string) (Consumer, error) {
	config := CreateDefaultSaramaConfig(clientID, nil, metrics.DefaultRegistry)
	return NewSaramaConsumer(brokers, group, config)
}

// ConsumerBuilderWithConfig creates a Kafka consumer using the Sarama library.
func ConsumerBuilderWithConfig(config *cluster.Config) ConsumerBuilder {
	return func(brokers []string, group, clientID string) (Consumer, error) {
		config.ClientID = clientID
		return NewSaramaConsumer(brokers, group, config)
	}
}

// ProducerBuilder create a Kafka producer.
type ProducerBuilder func(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error)

// DefaultProducerBuilder creates a Kafka producer using the Sarama library.
func DefaultProducerBuilder(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error) {
	partitioner := sarama.NewCustomHashPartitioner(hasher)
	config := CreateDefaultSaramaConfig(clientID, partitioner, metrics.DefaultRegistry)
	return NewProducer(brokers, &config.Config)
}

// TopicManagerBuilder creates a TopicManager to check partition counts and
// create tables.
type TopicManagerBuilder func(brokers []string) (TopicManager, error)

// DefaultTopicManagerBuilder creates TopicManager using the Sarama library.
// This topic manager cannot create topics.
func DefaultTopicManagerBuilder(brokers []string) (TopicManager, error) {
	return NewSaramaTopicManager(brokers)
}
