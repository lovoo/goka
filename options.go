package goka

import (
	"fmt"
	"hash"
	"hash/fnv"
	"path/filepath"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/storage"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/syndtr/goleveldb/leveldb"
)

// UpdateCallback is invoked upon arrival of a message for a table partition.
// The partition storage shall be updated in the callback.
type UpdateCallback func(s storage.Storage, partition int32, key string, value []byte) error

// StorageBuilder creates a local storage (a persistent cache) for a topic
// table. StorageBuilder creates one storage for each partition of the topic.
type StorageBuilder func(topic string, partition int32, codec Codec, reg metrics.Registry) (storage.Storage, error)

///////////////////////////////////////////////////////////////////////////////
// default values
///////////////////////////////////////////////////////////////////////////////

const (
	defaultBaseStoragePath = "/tmp/goka"

	defaultClientID = "goka"
)

// DefaultProcessorStoragePath is the default path where processor state
// will be stored.
func DefaultProcessorStoragePath(group Group) string {
	return filepath.Join(defaultBaseStoragePath, "processor", string(group))
}

// DefaultViewStoragePath returns the default path where view state will be stored.
func DefaultViewStoragePath() string {
	return filepath.Join(defaultBaseStoragePath, "view")
}

// DefaultUpdate is the default callback used to update the local storage with
// from the table topic in Kafka. It is called for every message received
// during recovery of processors and during the normal operation of views.
// DefaultUpdate can be used in the function passed to WithUpdateCallback and
// WithViewCallback.
func DefaultUpdate(s storage.Storage, partition int32, key string, value []byte) error {
	if value == nil {
		return s.Delete(key)
	}

	return s.SetEncoded(key, value)
}

// DefaultStorageBuilder builds a LevelDB storage with default configuration.
// The database will be stored in the given path.
func DefaultStorageBuilder(path string) StorageBuilder {
	return func(topic string, partition int32, codec Codec, reg metrics.Registry) (storage.Storage, error) {
		fp := filepath.Join(path, fmt.Sprintf("%s.%d", topic, partition))
		db, err := leveldb.OpenFile(fp, nil)
		if err != nil {
			return nil, fmt.Errorf("error opening leveldb: %v", err)
		}

		return storage.New(db, codec)
	}
}

// DefaultHasher returns an FNV hasher builder to assign keys to partitions.
func DefaultHasher() func() hash.Hash32 {
	return func() hash.Hash32 {
		return fnv.New32a()
	}

}

type consumerBuilder func(brokers []string, group string, clientID string, registry metrics.Registry) (kafka.Consumer, error)
type producerBuilder func(brokers []string, registry metrics.Registry) (kafka.Producer, error)
type topicmgrBuilder func(brokers []string) (kafka.TopicManager, error)

func defaultConsumerBuilder(brokers []string, group string, clientID string, registry metrics.Registry) (kafka.Consumer, error) {
	config := kafka.CreateDefaultSaramaConfig(clientID, nil, registry)
	return kafka.NewSaramaConsumer(brokers, group, config, registry)
}

func defaultProducerBuilder(clientID string, hasher func() hash.Hash32, log logger.Logger) producerBuilder {
	return func(brokers []string, registry metrics.Registry) (kafka.Producer, error) {
		partitioner := sarama.NewCustomHashPartitioner(hasher)
		config := kafka.CreateDefaultSaramaConfig(clientID, partitioner, registry)
		return kafka.NewProducer(brokers, &config.Config, registry, log)
	}
}

func defaultTopicManagerBuilder(brokers []string) (kafka.TopicManager, error) {
	return kafka.NewSaramaTopicManager(brokers)
}

///////////////////////////////////////////////////////////////////////////////
// processor options
///////////////////////////////////////////////////////////////////////////////

// ProcessorOption defines a configuration option to be used when creating a processor.
type ProcessorOption func(*poptions)

// processor options
type poptions struct {
	log      logger.Logger
	clientID string

	updateCallback       UpdateCallback
	registry             metrics.Registry
	partitionChannelSize int
	hasher               func() hash.Hash32
	nilHandling          NilHandling

	builders struct {
		storage  StorageBuilder
		consumer consumerBuilder
		producer producerBuilder
		topicmgr topicmgrBuilder
	}
	gokaRegistry  metrics.Registry
	kafkaRegistry metrics.Registry
}

// WithUpdateCallback defines the callback called upon recovering a message
// from the log.
func WithUpdateCallback(cb UpdateCallback) ProcessorOption {
	return func(o *poptions) {
		o.updateCallback = cb
	}
}

// WithClientID defines the client ID used to identify with Kafka.
func WithClientID(clientID string) ProcessorOption {
	return func(o *poptions) {
		o.clientID = clientID
	}
}

// WithStorageBuilder defines a builder for the storage of each partition.
func WithStorageBuilder(sb StorageBuilder) ProcessorOption {
	return func(o *poptions) {
		o.builders.storage = sb
	}
}

// WithTopicManager defines a topic manager.
func WithTopicManager(tm kafka.TopicManager) ProcessorOption {
	return func(o *poptions) {
		o.builders.topicmgr = func(brokers []string) (kafka.TopicManager, error) {
			if tm == nil {
				return nil, fmt.Errorf("TopicManager cannot be nil")
			}
			return tm, nil
		}
	}
}

// WithConsumer replaces goka's default consumer.
func WithConsumer(c kafka.Consumer) ProcessorOption {
	return func(o *poptions) {
		o.builders.consumer = func(brokers []string, group string, clientID string, registry metrics.Registry) (kafka.Consumer, error) {
			if c == nil {
				return nil, fmt.Errorf("consumer cannot be nil")
			}
			return c, nil
		}
	}
}

// WithProducer replaces goka's default producer.
func WithProducer(p kafka.Producer) ProcessorOption {
	return func(o *poptions) {
		o.builders.producer = func(brokers []string, registry metrics.Registry) (kafka.Producer, error) {
			if p == nil {
				return nil, fmt.Errorf("producer cannot be nil")
			}
			return p, nil
		}
	}
}

// WithKafkaMetrics sets a go-metrics registry to collect Kafka metrics.
// The metric-points are https://godoc.org/github.com/Shopify/sarama
func WithKafkaMetrics(registry metrics.Registry) ProcessorOption {
	return func(o *poptions) {
		o.kafkaRegistry = registry
	}
}

// WithPartitionChannelSize replaces the default partition channel size.
// This is mostly used for testing by setting it to 0 to have synchronous behavior
// of goka.
func WithPartitionChannelSize(size int) ProcessorOption {
	return func(o *poptions) {
		o.partitionChannelSize = size
	}
}

// WithLogger sets the logger the processor should use. By default, processors
// use the standard library logger.
func WithLogger(log logger.Logger) ProcessorOption {
	return func(o *poptions) {
		o.log = log
	}
}

// WithRegistry sets the metrics registry the processor should use. The registry
// should not be prefixed, otherwise the monitor web view won't work as it
// expects metrics of certain name.
func WithRegistry(r metrics.Registry) ProcessorOption {
	return func(o *poptions) {
		o.registry = r
	}
}

// WithHasher sets the hash function that assigns keys to partitions.
func WithHasher(hasher func() hash.Hash32) ProcessorOption {
	return func(o *poptions) {
		o.hasher = hasher
	}
}

type NilHandling int

const (
	// NilIgnore drops any message with nil value.
	NilIgnore NilHandling = 0 + iota
	// NilProcess passes the nil value to ProcessCallback.
	NilProcess
	// NilDecode passes the nil value to decoder before calling ProcessCallback.
	NilDecode
)

// WithNilHandling configures how the processor should handle messages with nil
// value. By default the processor ignores nil messages.
func WithNilHandling(nh NilHandling) ProcessorOption {
	return func(o *poptions) {
		o.nilHandling = nh
	}
}

func (opt *poptions) applyOptions(group string, opts ...ProcessorOption) error {
	opt.clientID = defaultClientID
	opt.log = logger.Default()
	opt.hasher = DefaultHasher()

	for _, o := range opts {
		o(opt)
	}

	// StorageBuilder should always be set as a default option in NewProcessor
	if opt.builders.storage == nil {
		return fmt.Errorf("StorageBuilder not set")
	}
	if opt.builders.consumer == nil {
		opt.builders.consumer = defaultConsumerBuilder
	}
	if opt.builders.producer == nil {
		opt.builders.producer = defaultProducerBuilder(opt.clientID, opt.hasher, opt.log)
	}
	if opt.builders.topicmgr == nil {
		opt.builders.topicmgr = defaultTopicManagerBuilder
	}

	// prefix registry
	opt.gokaRegistry = metrics.NewPrefixedChildRegistry(opt.registry, fmt.Sprintf("goka.processor-%s.", group))

	// Set a default registry to pass it to the kafka producer/consumer builders.
	if opt.kafkaRegistry == nil {
		opt.kafkaRegistry = metrics.NewPrefixedChildRegistry(opt.registry, fmt.Sprintf("kafka.processor-%s.", group))
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////
// view options
///////////////////////////////////////////////////////////////////////////////

// ViewOption defines a configuration option to be used when creating a view.
type ViewOption func(*voptions)

type voptions struct {
	log                  logger.Logger
	clientID             string
	tableCodec           Codec
	updateCallback       UpdateCallback
	registry             metrics.Registry
	partitionChannelSize int
	hasher               func() hash.Hash32

	builders struct {
		storage  StorageBuilder
		consumer consumerBuilder
		topicmgr topicmgrBuilder
	}
	gokaRegistry  metrics.Registry
	kafkaRegistry metrics.Registry
}

// WithViewRegistry sets the metrics registry the processor should use. The
// registry should not be prefixed, otherwise the monitor web view won't work as
// it expects metrics of certain name.
func WithViewRegistry(r metrics.Registry) ViewOption {
	return func(o *voptions) {
		o.registry = r
	}
}

// WithViewLogger sets the logger the view should use. By default, views
// use the standard library logger.
func WithViewLogger(log logger.Logger) ViewOption {
	return func(o *voptions) {
		o.log = log
	}
}

// WithViewCallback defines the callback called upon recovering a message
// from the log.
func WithViewCallback(cb UpdateCallback) ViewOption {
	return func(o *voptions) {
		o.updateCallback = cb
	}
}

// WithViewStorageBuilder defines a builder for the storage of each partition.
func WithViewStorageBuilder(sb StorageBuilder) ViewOption {
	return func(o *voptions) {
		o.builders.storage = sb
	}
}

// WithViewConsumer replaces goka's default view consumer. Mainly for testing.
func WithViewConsumer(c kafka.Consumer) ViewOption {
	return func(o *voptions) {
		o.builders.consumer = func(brokers []string, group string, clientID string, registry metrics.Registry) (kafka.Consumer, error) {
			if c == nil {
				return nil, fmt.Errorf("consumer cannot be nil")
			}
			return c, nil
		}
	}
}

// WithViewTopicManager defines a topic manager.
func WithViewTopicManager(tm kafka.TopicManager) ViewOption {
	return func(o *voptions) {
		o.builders.topicmgr = func(brokers []string) (kafka.TopicManager, error) {
			if tm == nil {
				return nil, fmt.Errorf("TopicManager cannot be nil")
			}
			return tm, nil
		}
	}
}

// WithViewKafkaMetrics sets a go-metrics registry to collect Kafka metrics.
// The metric-points are https://godoc.org/github.com/Shopify/sarama
func WithViewKafkaMetrics(registry metrics.Registry) ViewOption {
	return func(o *voptions) {
		o.kafkaRegistry = registry
	}
}

// WithViewPartitionChannelSize replaces the default partition channel size.
// This is mostly used for testing by setting it to 0 to have synchronous behavior
// of goka.
func WithViewPartitionChannelSize(size int) ViewOption {
	return func(o *voptions) {
		o.partitionChannelSize = size
	}
}

// WithViewHasher sets the hash function that assigns keys to partitions.
func WithViewHasher(hasher func() hash.Hash32) ViewOption {
	return func(o *voptions) {
		o.hasher = hasher
	}
}

// WithViewClientID defines the client ID used to identify with Kafka.
func WithViewClientID(clientID string) ViewOption {
	return func(o *voptions) {
		o.clientID = clientID
	}
}

func (opt *voptions) applyOptions(topic Table, opts ...ViewOption) error {
	opt.clientID = defaultClientID
	opt.log = logger.Default()
	opt.hasher = DefaultHasher()

	for _, o := range opts {
		o(opt)
	}

	// StorageBuilder should always be set as a default option in NewView
	if opt.builders.storage == nil {
		return fmt.Errorf("StorageBuilder not set")
	}
	if opt.builders.consumer == nil {
		opt.builders.consumer = defaultConsumerBuilder
	}
	if opt.builders.topicmgr == nil {
		opt.builders.topicmgr = defaultTopicManagerBuilder
	}

	// Set a default registry to pass it to the kafka consumer builders.
	if opt.kafkaRegistry == nil {
		opt.kafkaRegistry = metrics.NewPrefixedChildRegistry(opt.registry, fmt.Sprintf("kafka.view-%s.", topic))
	}

	// prefix registry
	opt.gokaRegistry = metrics.NewPrefixedChildRegistry(opt.registry, fmt.Sprintf("goka.view-%s.", topic))

	return nil
}

///////////////////////////////////////////////////////////////////////////////
// emitter options
///////////////////////////////////////////////////////////////////////////////

// EmitterOption defines a configuration option to be used when creating an
// emitter.
type EmitterOption func(*eoptions)

// emitter options
type eoptions struct {
	log      logger.Logger
	clientID string

	registry metrics.Registry
	codec    Codec
	hasher   func() hash.Hash32

	builders struct {
		topicmgr topicmgrBuilder
		producer producerBuilder
	}
	kafkaRegistry metrics.Registry
}

// WithEmitterLogger sets the logger the emitter should use. By default,
// emitters use the standard library logger.
func WithEmitterLogger(log logger.Logger) EmitterOption {
	return func(o *eoptions) {
		o.log = log
	}
}

// WithEmitterClientID defines the client ID used to identify with kafka.
func WithEmitterClientID(clientID string) EmitterOption {
	return func(o *eoptions) {
		o.clientID = clientID
	}
}

// WithEmitterTopicManager defines a topic manager.
func WithEmitterTopicManager(tm kafka.TopicManager) EmitterOption {
	return func(o *eoptions) {
		o.builders.topicmgr = func(brokers []string) (kafka.TopicManager, error) {
			if tm == nil {
				return nil, fmt.Errorf("TopicManager cannot be nil")
			}
			return tm, nil
		}
	}
}

// WithEmitterProducer replaces goka's default producer. Mainly for testing.
func WithEmitterProducer(p kafka.Producer) EmitterOption {
	return func(o *eoptions) {
		o.builders.producer = func(brokers []string, registry metrics.Registry) (kafka.Producer, error) {
			if p == nil {
				return nil, fmt.Errorf("producer cannot be nil")
			}
			return p, nil
		}
	}
}

// WithEmitterKafkaMetrics sets a go-metrics registry to collect
// kafka metrics.
// The metric-points are https://godoc.org/github.com/Shopify/sarama
func WithEmitterKafkaMetrics(registry metrics.Registry) EmitterOption {
	return func(o *eoptions) {
		o.kafkaRegistry = registry
	}
}

// WithEmitterHasher sets the hash function that assigns keys to partitions.
func WithEmitterHasher(hasher func() hash.Hash32) EmitterOption {
	return func(o *eoptions) {
		o.hasher = hasher
	}
}

func (opt *eoptions) applyOptions(opts ...EmitterOption) error {
	opt.clientID = defaultClientID
	opt.log = logger.Default()
	opt.hasher = DefaultHasher()

	for _, o := range opts {
		o(opt)
	}

	// config not set, use default one
	if opt.builders.producer == nil {
		opt.builders.producer = defaultProducerBuilder(opt.clientID, opt.hasher, opt.log)
	}
	if opt.builders.topicmgr == nil {
		opt.builders.topicmgr = defaultTopicManagerBuilder
	}

	// Set a default registry to pass it to the kafka producer/consumer builders.
	if opt.kafkaRegistry == nil {
		opt.kafkaRegistry = metrics.NewPrefixedRegistry("goka.kafka.producer.")
	}

	// prefix registry
	opt.registry = metrics.NewPrefixedRegistry("goka.producer.")

	return nil
}
