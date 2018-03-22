package kafka

import (
	"hash"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// ConsumerBuilder creates a Kafka consumer.
type ConsumerBuilder func(brokers []string, group, clientID string) (Consumer, error)

// DefaultConsumerBuilder creates a Kafka consumer using the Sarama library.
func DefaultConsumerBuilder(brokers []string, group, clientID string) (Consumer, error) {
	config := NewConfig()
	config.ClientID = clientID
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
	config := NewConfig()
	config.ClientID = clientID
	config.Producer.Partitioner = sarama.NewCustomHashPartitioner(hasher)
	return NewProducer(brokers, &config.Config)
}

// ProducerBuilderWithConfig creates a Kafka consumer using the Sarama library.
func ProducerBuilderWithConfig(config *cluster.Config) ProducerBuilder {
	return func(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error) {
		config.ClientID = clientID
		config.Producer.Partitioner = sarama.NewCustomHashPartitioner(hasher)
		return NewProducer(brokers, &config.Config)
	}
}

// TopicManagerBuilder creates a TopicManager to check partition counts and
// create tables.
type TopicManagerBuilder func(brokers []string) (TopicManager, error)

// DefaultTopicManagerBuilder creates TopicManager using the Sarama library.
// This topic manager cannot create topics.
func DefaultTopicManagerBuilder(brokers []string) (TopicManager, error) {
	return NewSaramaTopicManager(brokers, sarama.NewConfig())
}

// TopicManagerBuilderWithConfig creates TopicManager using the Sarama library.
// This topic manager cannot create topics.
func TopicManagerBuilderWithConfig(config *cluster.Config) TopicManagerBuilder {
	return func(brokers []string) (TopicManager, error) {
		return NewSaramaTopicManager(brokers, &config.Config)
	}
}

// ZKTopicManagerBuilder creates a TopicManager that connects with ZooKeeper to
// check partition counts and create tables.
func ZKTopicManagerBuilder(servers []string) TopicManagerBuilder {
	return func([]string) (TopicManager, error) {
		return NewTopicManager(servers, NewTopicManagerConfig())
	}
}

// ZKTopicManagerBuilderWithConfig creates a TopicManager that connects with ZooKeeper to
// check partition counts and create tables given a topic configuration.
func ZKTopicManagerBuilderWithConfig(servers []string, config *TopicManagerConfig) TopicManagerBuilder {
	return func([]string) (TopicManager, error) {
		return NewTopicManager(servers, config)
	}
}
