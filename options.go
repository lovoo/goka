package goka

import (
	"fmt"
	"hash"
	"hash/fnv"
	"path/filepath"

	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/storage"
)

// UpdateCallback is invoked upon arrival of a message for a table partition.
// The partition storage shall be updated in the callback.
type UpdateCallback func(s storage.Storage, partition int32, key string, value []byte) error

///////////////////////////////////////////////////////////////////////////////
// default values
///////////////////////////////////////////////////////////////////////////////

const (
	defaultBaseStoragePath = "/tmp/goka"
	defaultClientID        = "goka"
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

	return s.Set(key, value)
}

// DefaultHasher returns an FNV hasher builder to assign keys to partitions.
func DefaultHasher() func() hash.Hash32 {
	return func() hash.Hash32 {
		return fnv.New32a()
	}

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
	partitionChannelSize int
	hasher               func() hash.Hash32
	nilHandling          NilHandling

	builders struct {
		storage  storage.Builder
		consumer kafka.ConsumerBuilder
		producer kafka.ProducerBuilder
		topicmgr kafka.TopicManagerBuilder
	}
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
func WithStorageBuilder(sb storage.Builder) ProcessorOption {
	return func(o *poptions) {
		o.builders.storage = sb
	}
}

// WithTopicManagerBuilder replaces the default topic manager builder.
func WithTopicManagerBuilder(tmb kafka.TopicManagerBuilder) ProcessorOption {
	return func(o *poptions) {
		o.builders.topicmgr = tmb
	}
}

// WithConsumerBuilder replaces the default consumer builder.
func WithConsumerBuilder(cb kafka.ConsumerBuilder) ProcessorOption {
	return func(o *poptions) {
		o.builders.consumer = cb
	}
}

// WithProducerBuilder replaces the default producer builder.
func WithProducerBuilder(pb kafka.ProducerBuilder) ProcessorOption {
	return func(o *poptions) {
		o.builders.producer = pb
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

// WithHasher sets the hash function that assigns keys to partitions.
func WithHasher(hasher func() hash.Hash32) ProcessorOption {
	return func(o *poptions) {
		o.hasher = hasher
	}
}

// NilHandling defines how nil messages should be handled by the processor.
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

type Tester interface {
	StorageBuilder() storage.Builder
	ConsumerBuilder() kafka.ConsumerBuilder
	ProducerBuilder() kafka.ProducerBuilder
	TopicManagerBuilder() kafka.TopicManagerBuilder
}

// WithTester configures all external connections of a processor, ie, storage,
// consumer and producer
func WithTester(t Tester) ProcessorOption {
	return func(o *poptions) {
		o.builders.storage = t.StorageBuilder()
		o.builders.consumer = t.ConsumerBuilder()
		o.builders.producer = t.ProducerBuilder()
		o.builders.topicmgr = t.TopicManagerBuilder()
		o.partitionChannelSize = 0
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
		opt.builders.consumer = kafka.DefaultConsumerBuilder
	}
	if opt.builders.producer == nil {
		opt.builders.producer = kafka.DefaultProducerBuilder
	}
	if opt.builders.topicmgr == nil {
		opt.builders.topicmgr = kafka.DefaultTopicManagerBuilder
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
	partitionChannelSize int
	hasher               func() hash.Hash32
	restartable          bool

	builders struct {
		storage  storage.Builder
		consumer kafka.ConsumerBuilder
		topicmgr kafka.TopicManagerBuilder
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
func WithViewStorageBuilder(sb storage.Builder) ViewOption {
	return func(o *voptions) {
		o.builders.storage = sb
	}
}

// WithViewConsumerBuilder replaces default view consumer.
func WithViewConsumerBuilder(cb kafka.ConsumerBuilder) ViewOption {
	return func(o *voptions) {
		o.builders.consumer = cb
	}
}

// WithViewTopicManagerBuilder replaces the default topic manager.
func WithViewTopicManagerBuilder(tmb kafka.TopicManagerBuilder) ViewOption {
	return func(o *voptions) {
		o.builders.topicmgr = tmb
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

// WithViewRestartable defines the view can be restarted, even when Run()
// returns errors. If the view is restartable, the client must call Terminate()
// to release all resources, ie, close the local storage.
func WithViewRestartable() ViewOption {
	return func(o *voptions) {
		o.restartable = true
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
		opt.builders.consumer = kafka.DefaultConsumerBuilder
	}
	if opt.builders.topicmgr == nil {
		opt.builders.topicmgr = kafka.DefaultTopicManagerBuilder
	}

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

	hasher func() hash.Hash32

	builders struct {
		topicmgr kafka.TopicManagerBuilder
		producer kafka.ProducerBuilder
	}
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

// WithEmitterTopicManagerBuilder replaces the default topic manager builder.
func WithEmitterTopicManagerBuilder(tmb kafka.TopicManagerBuilder) EmitterOption {
	return func(o *eoptions) {
		o.builders.topicmgr = tmb
	}
}

// WithEmitterProducerBuilder replaces the default producer builder.
func WithEmitterProducerBuilder(pb kafka.ProducerBuilder) EmitterOption {
	return func(o *eoptions) {
		o.builders.producer = pb
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
		opt.builders.producer = kafka.DefaultProducerBuilder
	}
	if opt.builders.topicmgr == nil {
		opt.builders.topicmgr = kafka.DefaultTopicManagerBuilder
	}

	return nil
}
