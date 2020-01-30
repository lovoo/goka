package goka

import "github.com/Shopify/sarama"

// Broker is an interface for the sarama broker
type Broker interface {
	Addr() string
	Connected() (bool, error)
	CreateTopics(request *sarama.CreateTopicsRequest) (*sarama.CreateTopicsResponse, error)
	Open(conf *sarama.Config) error
}
