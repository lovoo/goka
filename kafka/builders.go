package kafka

import (
	"hash"

	"github.com/Shopify/sarama"
	metrics "github.com/rcrowley/go-metrics"
)

// DefaultConsumerBuilder creates a Kafka consumer using the Sarama library.
func DefaultConsumerBuilder(brokers []string, group, clientID string) (Consumer, error) {
	config := CreateDefaultSaramaConfig(clientID, nil, metrics.DefaultRegistry)
	return NewSaramaConsumer(brokers, group, config)
}

// DefaultProducerBuilder creates a Kafka producer using the Sarama library.
func DefaultProducerBuilder(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error) {
	partitioner := sarama.NewCustomHashPartitioner(hasher)
	config := CreateDefaultSaramaConfig(clientID, partitioner, metrics.DefaultRegistry)
	return NewProducer(brokers, &config.Config)
}

// DefaultTopicManagerBuilder creates TopicManager using the Sarama library.
// This topic manager cannot create topics.
func DefaultTopicManagerBuilder(brokers []string) (TopicManager, error) {
	return NewSaramaTopicManager(brokers)
}
