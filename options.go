package goka

import (
	"fmt"
	"path/filepath"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"stash.lvint.de/lab/goka/codec"
	"stash.lvint.de/lab/goka/kafka"
	"stash.lvint.de/lab/goka/storage"
)

// UpdateCallback is invoked upon arrival of a message for a table partition.
// The partition storage shall be updated in the callback.
type UpdateCallback func(s storage.Storage, partition int32, key string, value []byte) error

// StorageBuilder creates a local storage (a persistent cache) for a topic
// table. StorageBuilder creates one storage for each partition of the topic.
type StorageBuilder func(topic string, partition int32, codec codec.Codec, reg metrics.Registry) (storage.Storage, error)

///////////////////////////////////////////////////////////////////////////////
// default values
///////////////////////////////////////////////////////////////////////////////

const (
	defaultClientID = "goka"
)

// DefaultUpdate is the default callback used to update the local storage with
// from the table topic in Kafka. It is called for every message received
// during recovery of processors and during the normal operation of views.
// DefaultUpdate can be used in the function passed to WithUpdateCallback and
// WithViewCallback.
func DefaultUpdate(s storage.Storage, partition int32, key string, value []byte) error {
	return s.SetEncoded(key, value)
}

type consumerBuilder func(brokers []string, group string, registry metrics.Registry) (kafka.Consumer, error)
type producerBuilder func(brokers []string, registry metrics.Registry) (kafka.Producer, error)
type topicmgrBuilder func(brokers []string) (kafka.TopicManager, error)

func defaultConsumerBuilder(brokers []string, group string, registry metrics.Registry) (kafka.Consumer, error) {
	return kafka.NewSaramaConsumer(brokers, group, registry)
}
func defaultProducerBuilder(brokers []string, registry metrics.Registry) (kafka.Producer, error) {
	return kafka.NewProducer(brokers, registry)
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
	clientID string

	tableEnabled            bool
	tableCodec              codec.Codec
	updateCallback          UpdateCallback
	storagePath             string
	storageSnapshotInterval time.Duration
	registry                metrics.Registry
	partitionChannelSize    int

	builders struct {
		storage  StorageBuilder
		consumer consumerBuilder
		producer producerBuilder
		topicmgr topicmgrBuilder
	}
	gokaRegistry  metrics.Registry
	kafkaRegistry metrics.Registry
}

// WithGroupTable enables the group table and defines a codec.
func WithGroupTable(codec codec.Codec) ProcessorOption {
	return func(o *poptions) {
		o.tableEnabled = true
		o.tableCodec = codec
	}
}

// WithUpdateCallback defines the callback called upon recovering a message
// from the log.
func WithUpdateCallback(cb UpdateCallback) ProcessorOption {
	return func(o *poptions) {
		o.updateCallback = cb
	}
}

// WithClientID defines the client ID used to identify with kafka.
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

// WithConsumer replaces goka's default consumer. Mainly for testing.
func WithConsumer(c kafka.Consumer) ProcessorOption {
	return func(o *poptions) {
		o.builders.consumer = func(brokers []string, group string, registry metrics.Registry) (kafka.Consumer, error) {
			if c == nil {
				return nil, fmt.Errorf("consumer cannot be nil")
			}
			return c, nil
		}
	}
}

// WithProducer replaces goka'S default producer. Mainly for testing.
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

// WithStoragePath defines the base path for the local storage on disk
func WithStoragePath(storagePath string) ProcessorOption {
	return func(o *poptions) {
		o.storagePath = storagePath
	}
}

// WithKafkaMetrics sets a go-metrics registry to collect
// kafka metrics.
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

// WithStorageSnapshotInterval sets the interval in which the storage will snapshot to disk (if it is supported by the storage at all)
// Greater interval -> less writes to disk, more memory usage
// Smaller interval -> more writes to disk, less memory usage
func WithStorageSnapshotInterval(interval time.Duration) ProcessorOption {
	return func(o *poptions) {
		o.storageSnapshotInterval = interval
	}
}

func (opt *poptions) applyOptions(group string, opts ...ProcessorOption) error {
	opt.clientID = defaultClientID

	for _, o := range opts {
		o(opt)
	}

	// config not set, use default one
	if opt.builders.storage == nil {
		opt.builders.storage = opt.defaultStorageBuilder
	}
	if opt.builders.consumer == nil {
		opt.builders.consumer = defaultConsumerBuilder
	}
	if opt.builders.producer == nil {
		opt.builders.producer = defaultProducerBuilder
	}
	if opt.builders.topicmgr == nil {
		opt.builders.topicmgr = defaultTopicManagerBuilder
	}
	if opt.storageSnapshotInterval == 0 {
		opt.storageSnapshotInterval = storage.DefaultStorageSnapshotInterval
	}

	opt.registry = metrics.NewRegistry()

	// prefix registry
	opt.gokaRegistry = metrics.NewPrefixedChildRegistry(opt.registry, fmt.Sprintf("goka.processor-%s.", group))

	// Set a default registry to pass it to the kafka producer/consumer builders.
	if opt.kafkaRegistry == nil {
		opt.kafkaRegistry = metrics.NewPrefixedChildRegistry(opt.registry, fmt.Sprintf("kafka.processor-%s.", group))
	}

	return nil
}

func (opt *poptions) storagePathForPartition(topic string, partitionID int32) string {
	return filepath.Join(opt.storagePath, "processor", fmt.Sprintf("%s.%d", topic, partitionID))
}

func (opt *poptions) tableTopic(group string) Subscription {
	if !opt.tableEnabled {
		return Subscription{}
	}
	return Subscription{Name: tableName(group)}
}

func (opt *poptions) defaultStorageBuilder(topic string, partition int32, codec codec.Codec, reg metrics.Registry) (storage.Storage, error) {
	return storage.New(opt.storagePathForPartition(topic, partition), codec, reg, opt.storageSnapshotInterval)
}

///////////////////////////////////////////////////////////////////////////////
// view options
///////////////////////////////////////////////////////////////////////////////

// ViewOption defines a configuration option to be used when creating a view.
type ViewOption func(*voptions)

type voptions struct {
	tableCodec              codec.Codec
	updateCallback          UpdateCallback
	storagePath             string
	storageSnapshotInterval time.Duration
	registry                metrics.Registry
	partitionChannelSize    int

	builders struct {
		storage  StorageBuilder
		consumer consumerBuilder
		topicmgr topicmgrBuilder
	}
	gokaRegistry  metrics.Registry
	kafkaRegistry metrics.Registry
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
		o.builders.consumer = func(brokers []string, group string, registry metrics.Registry) (kafka.Consumer, error) {
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

// WithViewStoragePath defines the base path for the local storage on disk
func WithViewStoragePath(storagePath string) ViewOption {
	return func(o *voptions) {
		o.storagePath = storagePath
	}
}

// WithViewStorageSnapshotInterval sets the interval in which the storage will snapshot to disk (if it is supported by the storage at all)
// Greater interval -> less writes to disk, more memory usage
// Smaller interval -> more writes to disk, less memory usage
func WithViewStorageSnapshotInterval(interval time.Duration) ViewOption {
	return func(o *voptions) {
		o.storageSnapshotInterval = interval
	}
}

// WithViewKafkaMetrics sets a go-metrics registry to collect
// kafka metrics.
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

func (opt *voptions) applyOptions(group string, opts ...ViewOption) error {
	for _, o := range opts {
		o(opt)
	}

	// config not set, use default one
	if opt.builders.storage == nil {
		opt.builders.storage = opt.defaultStorageBuilder
	}
	if opt.builders.consumer == nil {
		opt.builders.consumer = defaultConsumerBuilder
	}
	if opt.builders.topicmgr == nil {
		opt.builders.topicmgr = defaultTopicManagerBuilder
	}

	opt.registry = metrics.NewRegistry()

	// Set a default registry to pass it to the kafka consumer builders.
	if opt.kafkaRegistry == nil {
		opt.kafkaRegistry = metrics.NewPrefixedChildRegistry(opt.registry, fmt.Sprintf("kafka.view-%s.", group))
	}

	// prefix registry
	opt.gokaRegistry = metrics.NewPrefixedChildRegistry(opt.registry, fmt.Sprintf("goka.view-%s.", group))

	if opt.storageSnapshotInterval == 0 {
		opt.storageSnapshotInterval = storage.DefaultStorageSnapshotInterval
	}

	return nil
}

func (opt *voptions) storagePathForPartition(topic string, partitionID int32) string {
	return filepath.Join(opt.storagePath, "view", fmt.Sprintf("%s.%d", topic, partitionID))
}

func (opt *voptions) tableTopic(group string) Subscription {
	return Subscription{Name: tableName(group)}
}

func (opt *voptions) defaultStorageBuilder(topic string, partition int32, codec codec.Codec, reg metrics.Registry) (storage.Storage, error) {
	return storage.New(opt.storagePathForPartition(topic, partition), codec, reg, opt.storageSnapshotInterval)
}

///////////////////////////////////////////////////////////////////////////////
// producer options
///////////////////////////////////////////////////////////////////////////////

// ProducerOption defines a configuration option to be used when creating a producer
type ProducerOption func(*proptions)

// producer options
type proptions struct {
	clientID string

	registry metrics.Registry
	codec    codec.Codec

	builders struct {
		topicmgr topicmgrBuilder
		producer producerBuilder
	}
	kafkaRegistry metrics.Registry
}

// WithProducerClientID defines the client ID used to identify with kafka.
func WithProducerClientID(clientID string) ProducerOption {
	return func(o *proptions) {
		o.clientID = clientID
	}
}

// WithProducerTopicManager defines a topic manager.
func WithProducerTopicManager(tm kafka.TopicManager) ProducerOption {
	return func(o *proptions) {
		o.builders.topicmgr = func(brokers []string) (kafka.TopicManager, error) {
			if tm == nil {
				return nil, fmt.Errorf("TopicManager cannot be nil")
			}
			return tm, nil
		}
	}
}

// WithProducerProducer replaces goka's default producer. Mainly for testing.
func WithProducerProducer(p kafka.Producer) ProducerOption {
	return func(o *proptions) {
		o.builders.producer = func(brokers []string, registry metrics.Registry) (kafka.Producer, error) {
			if p == nil {
				return nil, fmt.Errorf("producer cannot be nil")
			}
			return p, nil
		}
	}
}

// WithProducerKafkaMetrics sets a go-metrics registry to collect
// kafka metrics.
// The metric-points are https://godoc.org/github.com/Shopify/sarama
func WithProducerKafkaMetrics(registry metrics.Registry) ProducerOption {
	return func(o *proptions) {
		o.kafkaRegistry = registry
	}
}

func (opt *proptions) applyOptions(opts ...ProducerOption) error {
	opt.clientID = defaultClientID

	for _, o := range opts {
		o(opt)
	}

	// config not set, use default one
	if opt.builders.producer == nil {
		opt.builders.producer = defaultProducerBuilder
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
