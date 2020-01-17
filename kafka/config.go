package kafka

import (
	"time"

	"github.com/Shopify/sarama"
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

func NewSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0

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
	return config
}
