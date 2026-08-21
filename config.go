package goka

import (
	"time"

	"github.com/IBM/sarama"
)

var (
	globalConfig = *DefaultConfig()
)

const (

	// time sarama-cluster assumes the processing of an event may take
	defaultMaxProcessingTime = 1 * time.Second

	// producer flush configuration
	defaultFlushFrequency       = time.Second
	defaultFlushBytes           = 1024 * 1024
	defaultProducerMaxRetries   = 20
	defaultProducerRetryBackoff = 500 * time.Millisecond
)

// DefaultConfig creates a new config used by goka per default
// Use it to modify and pass to `goka.ReplaceGlobalConifg(...)` to modify
// goka's global config
func DefaultConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0

	// consumer configuration
	config.Consumer.Return.Errors = true
	config.Consumer.MaxProcessingTime = defaultMaxProcessingTime
	// this configures the initial offset for streams. Tables are always
	// consumed from OffsetOldest.
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Group.Rebalance.Strategy = CopartitioningStrategy
	// producer configuration
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = defaultFlushFrequency
	config.Producer.Flush.Bytes = defaultFlushBytes
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	// retry budget must outlast a broker leader election / rolling restart, or
	// a transient ErrNotLeaderForPartition will be treated as fatal.
	config.Producer.Retry.Max = defaultProducerMaxRetries
	config.Producer.Retry.Backoff = defaultProducerRetryBackoff
	return config
}

// ReplaceGlobalConfig registeres a standard config used during building if no
// other config is specified
func ReplaceGlobalConfig(config *sarama.Config) {
	if config == nil {
		panic("nil config registered as global config")
	}
	globalConfig = *config
}
