package kafka

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// NewConfig creates a (bsm) sarama configuration with default values.
func NewConfig() *cluster.Config {
	config := cluster.NewConfig()
	config.Version = sarama.V0_10_1_0

	// consumer configuration
	config.Consumer.Return.Errors = true
	config.Consumer.MaxProcessingTime = defaultMaxProcessingTime
	// this configures the initial offset for streams. Tables are always
	// consumed from OffsetOldest.
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// producer configuration
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = defaultFlushFrequency
	config.Producer.Flush.Bytes = defaultFlushBytes
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = defaultProducerMaxRetries

	// consumer group configuration
	config.Group.Return.Notifications = true

	return config
}
