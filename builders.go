package goka

import (
	"hash"

	"github.com/Shopify/sarama"
)

// ProducerBuilder create a Kafka producer.
type ProducerBuilder func(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error)

// DefaultProducerBuilder creates a Kafka producer using the Sarama library.
func DefaultProducerBuilder(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error) {
	config := globalConfig
	config.ClientID = clientID
	config.Producer.Partitioner = sarama.NewCustomHashPartitioner(hasher)
	return NewProducer(brokers, &config)
}

// ProducerBuilderWithConfig creates a Kafka consumer using the Sarama library.
func ProducerBuilderWithConfig(config *sarama.Config) ProducerBuilder {
	return func(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error) {
		config.ClientID = clientID
		config.Producer.Partitioner = sarama.NewCustomHashPartitioner(hasher)
		return NewProducer(brokers, config)
	}
}

// TopicManagerBuilder creates a TopicManager to check partition counts and
// create tables.
type TopicManagerBuilder func(brokers []string) (TopicManager, error)

// DefaultTopicManagerBuilder creates TopicManager using the Sarama library.
func DefaultTopicManagerBuilder(brokers []string) (TopicManager, error) {
	config := globalConfig
	config.ClientID = "goka-topic-manager"
	return NewTopicManager(brokers, &config, NewTopicManagerConfig())
}

// TopicManagerBuilderWithConfig creates TopicManager using the Sarama library.
func TopicManagerBuilderWithConfig(config *sarama.Config, tmConfig *TopicManagerConfig) TopicManagerBuilder {
	return func(brokers []string) (TopicManager, error) {
		return NewTopicManager(brokers, config, tmConfig)
	}
}

// TopicManagerBuilderWithTopicManagerConfig creates TopicManager using the Sarama library.
func TopicManagerBuilderWithTopicManagerConfig(tmConfig *TopicManagerConfig) TopicManagerBuilder {
	return func(brokers []string) (TopicManager, error) {
		config := globalConfig
		config.ClientID = "goka-topic-manager"
		return NewTopicManager(brokers, &config, tmConfig)
	}
}

// ConsumerGroupBuilder creates a `sarama.ConsumerGroup`
type ConsumerGroupBuilder func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error)

// DefaultConsumerGroupBuilder creates a Kafka consumer using the Sarama library.
func DefaultConsumerGroupBuilder(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) {
	config := globalConfig
	config.ClientID = clientID
	return sarama.NewConsumerGroup(brokers, group, &config)
}

// ConsumerGroupBuilderWithConfig creates a sarama consumergroup using passed config
func ConsumerGroupBuilderWithConfig(config *sarama.Config) ConsumerGroupBuilder {
	return func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) {
		config.ClientID = clientID
		return sarama.NewConsumerGroup(brokers, group, config)
	}
}

// SaramaConsumerBuilder creates a `sarama.Consumer`
type SaramaConsumerBuilder func(brokers []string, clientID string) (sarama.Consumer, error)

// DefaultSaramaConsumerBuilder creates a Kafka consumer using the Sarama library.
func DefaultSaramaConsumerBuilder(brokers []string, clientID string) (sarama.Consumer, error) {
	config := globalConfig
	config.ClientID = clientID
	return sarama.NewConsumer(brokers, &config)
}

// SaramaConsumerBuilderWithConfig creates a sarama consumer using passed config
func SaramaConsumerBuilderWithConfig(config *sarama.Config) SaramaConsumerBuilder {
	return func(brokers []string, clientID string) (sarama.Consumer, error) {
		config.ClientID = clientID
		return sarama.NewConsumer(brokers, config)
	}
}

// BackoffBuilder creates a backoff
type BackoffBuilder func() (Backoff, error)

// DefaultBackoffBuilder returnes a simpleBackoff with 10 seconds step increase and 2 minutes max wait
func DefaultBackoffBuilder() (Backoff, error) {
	return NewSimpleBackoff(defaultBackoffStep, defaultBackoffMax), nil
}
