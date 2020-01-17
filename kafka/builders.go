package kafka

import (
	"hash"

	"github.com/Shopify/sarama"
)

// ProducerBuilder create a Kafka producer.
type ProducerBuilder func(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error)

// DefaultProducerBuilder creates a Kafka producer using the Sarama library.
func DefaultProducerBuilder(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error) {
	config := NewSaramaConfig()
	config.ClientID = clientID
	config.Producer.Partitioner = sarama.NewCustomHashPartitioner(hasher)
	return NewProducer(brokers, config)
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
	return NewSaramaTopicManager(brokers, sarama.NewConfig(), NewTopicManagerConfig())
}

// TopicManagerBuilderWithConfig creates TopicManager using the Sarama library.
// This topic manager cannot create topics.
func TopicManagerBuilderWithConfig(config *sarama.Config, tmConfig *TopicManagerConfig) TopicManagerBuilder {
	return func(brokers []string) (TopicManager, error) {
		return NewSaramaTopicManager(brokers, config, tmConfig)
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

type ConsumerGroupBuilder func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error)

// DefaultConsumerGroupBuilder creates a Kafka consumer using the Sarama library.
func DefaultConsumerGroupBuilder(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) {

	config := sarama.NewConfig()
	config.ClientID = clientID
	return sarama.NewConsumerGroup(brokers, group, config)
}

// ConsumerGroupBuilderWithConfig creates a sarama consumergroup using passed config
func ConsumerGroupBuilderWithConfig(config *sarama.Config) ConsumerGroupBuilder {
	return func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) {
		config.ClientID = clientID
		return sarama.NewConsumerGroup(brokers, group, config)
	}
}

type SaramaConsumerBuilder func(brokers []string, clientID string) (sarama.Consumer, error)

// DefaultSaramaConsumerBuilder creates a Kafka consumer using the Sarama library.
func DefaultSaramaConsumerBuilder(brokers []string, clientID string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.ClientID = clientID
	return sarama.NewConsumer(brokers, config)
}

// ConsumerBuilderWithConfig creates a sarama consumergroup using passed config
func SaramaConsumerBuilderWithConfig(config *sarama.Config) SaramaConsumerBuilder {
	return func(brokers []string, clientID string) (sarama.Consumer, error) {
		config.ClientID = clientID
		return sarama.NewConsumer(brokers, config)
	}
}
