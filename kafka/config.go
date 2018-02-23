package kafka

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// CreateDefaultConfig creates a (bsm) sarama configuration with default values.
func CreateDefaultConfig(clientID string) *cluster.Config {
	config := cluster.NewConfig()

	config.Version = sarama.V0_10_1_0
	config.ClientID = clientID

	// consumer configuration
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.MaxProcessingTime = defaultMaxProcessingTime

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
