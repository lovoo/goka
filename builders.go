package goka

import (
	"github.com/Shopify/sarama"
)

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

type ConsumerBuilder func(brokers []string, clientID string) (sarama.Consumer, error)

// DefaultConsumerBuilder creates a Kafka consumer using the Sarama library.
func DefaultConsumerBuilder(brokers []string, clientID string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.ClientID = clientID
	return sarama.NewConsumer(brokers, config)
}

// ConsumerBuilderWithConfig creates a sarama consumergroup using passed config
func ConsumerBuilderWithConfig(config *sarama.Config) ConsumerBuilder {
	return func(brokers []string, clientID string) (sarama.Consumer, error) {
		config.ClientID = clientID
		return sarama.NewConsumer(brokers, config)
	}
}
