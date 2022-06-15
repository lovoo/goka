package goka

import (
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"path/filepath"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/storage"
)

// UpdateContext defines the interface for UpdateCallback arguments.
type UpdateContext interface {
	// Topic returns the topic of input message.
	Topic() Stream

	// Partition returns the partition of the input message.
	Partition() int32

	// Offset returns the offset of the input message.
	Offset() int64

	// Headers returns the headers of the input message.
	//
	// It is recommended to lazily evaluate the headers to reduce overhead per message
	// when headers are not used.
	Headers() Headers
}

// UpdateCallback is invoked upon arrival of a message for a table partition.
type UpdateCallback func(ctx UpdateContext, s storage.Storage, key string, value []byte) error

// RebalanceCallback is invoked when the processor receives a new partition assignment.
type RebalanceCallback func(a Assignment)

///////////////////////////////////////////////////////////////////////////////
// default values
///////////////////////////////////////////////////////////////////////////////

const (
	defaultBaseStoragePath = "/tmp/goka"
	defaultClientID        = "goka"

	// duration, after which we'll reset the backoff
	defaultBackoffResetTime = 60 * time.Second
	// duration to increase each time retrying
	defaultBackoffStep = 10 * time.Second
	// maximum duration to wait for the backoff
	defaultBackoffMax = 120 * time.Second
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
func DefaultUpdate(ctx UpdateContext, s storage.Storage, key string, value []byte) error {
	if value == nil {
		return s.Delete(key)
	}

	return s.Set(key, value)
}

// DefaultRebalance is the default callback when a new partition assignment is received.
// DefaultRebalance can be used in the function passed to WithRebalanceCallback.
func DefaultRebalance(a Assignment) {}

// DefaultHasher returns an FNV hasher builder to assign keys to partitions.
func DefaultHasher() func() hash.Hash32 {
	return func() hash.Hash32 {
		return fnv.New32a()
	}
}

// DefaultUpdateContext implements the UpdateContext interface.
type DefaultUpdateContext struct {
	topic     Stream
	partition int32
	offset    int64
	headers   []*sarama.RecordHeader
}

// Topic returns the topic of input message.
func (ctx DefaultUpdateContext) Topic() Stream {
	return ctx.topic
}

// Partition returns the partition of the input message.
func (ctx DefaultUpdateContext) Partition() int32 {
	return ctx.partition
}

// Offset returns the offset of the input message.
func (ctx DefaultUpdateContext) Offset() int64 {
	return ctx.offset
}

// Headers returns the headers of the input message.
func (ctx DefaultUpdateContext) Headers() Headers {
	return HeadersFromSarama(ctx.headers)
}

///////////////////////////////////////////////////////////////////////////////
// processor options
///////////////////////////////////////////////////////////////////////////////

// ProcessorOption defines a configuration option to be used when creating a processor.
type ProcessorOption func(*poptions, *GroupGraph)

// processor options
type poptions struct {
	log      logger
	clientID string

	updateCallback         UpdateCallback
	rebalanceCallback      RebalanceCallback
	partitionChannelSize   int
	hasher                 func() hash.Hash32
	nilHandling            NilHandling
	backoffResetTime       time.Duration
	hotStandby             bool
	recoverAhead           bool
	producerDefaultHeaders Headers

	builders struct {
		storage        storage.Builder
		consumerSarama SaramaConsumerBuilder
		consumerGroup  ConsumerGroupBuilder
		producer       ProducerBuilder
		topicmgr       TopicManagerBuilder
		backoff        BackoffBuilder
	}
}

// WithUpdateCallback defines the callback called upon recovering a message
// from the log.
func WithUpdateCallback(cb UpdateCallback) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.updateCallback = cb
	}
}

// WithClientID defines the client ID used to identify with Kafka.
func WithClientID(clientID string) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.clientID = clientID
	}
}

// WithStorageBuilder defines a builder for the storage of each partition.
func WithStorageBuilder(sb storage.Builder) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.builders.storage = sb
	}
}

// WithTopicManagerBuilder replaces the default topic manager builder.
func WithTopicManagerBuilder(tmb TopicManagerBuilder) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.builders.topicmgr = tmb
	}
}

// WithConsumerGroupBuilder replaces the default consumer group builder
func WithConsumerGroupBuilder(cgb ConsumerGroupBuilder) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.builders.consumerGroup = cgb
	}
}

// WithConsumerSaramaBuilder replaces the default consumer group builder
func WithConsumerSaramaBuilder(cgb SaramaConsumerBuilder) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.builders.consumerSarama = cgb
	}
}

// WithProducerBuilder replaces the default producer builder.
func WithProducerBuilder(pb ProducerBuilder) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.builders.producer = pb
	}
}

// WithBackoffBuilder replaced the default backoff.
func WithBackoffBuilder(bb BackoffBuilder) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.builders.backoff = bb
	}
}

// WithPartitionChannelSize replaces the default partition channel size.
// This is mostly used for testing by setting it to 0 to have synchronous behavior
// of goka.
func WithPartitionChannelSize(size int) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.partitionChannelSize = size
	}
}

// WithLogger sets the logger the processor should use. By default, processors
// use the standard library logger.
func WithLogger(l Logger) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		if prefixLogger, ok := l.(logger); ok {
			o.log = prefixLogger
		} else {
			o.log = wrapLogger(l, defaultLogger.debug)
		}
	}
}

// WithHasher sets the hash function that assigns keys to partitions.
func WithHasher(hasher func() hash.Hash32) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.hasher = hasher
	}
}

// WithHotStandby configures the processor to keep partitions up to date which are not part
// of the current generation's assignment. This allows fast processor failover since
// all partitions are hot in other processor instances, but it requires
// more resources (in particular network and disk).
// If this option is used, the option `WithRecoverAhead` should also be added to avoid unnecessary delays.
func WithHotStandby() ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.hotStandby = true
	}
}

// WithRecoverAhead configures the processor to recover joins and the processor table ahead
// of joining the group. This reduces the processing delay that occurs when adding new instances to
// groups with high-volume-joins/tables. If the processor does not use joins or a table, it does not have any
// effect.
func WithRecoverAhead() ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.recoverAhead = true
	}
}

// WithGroupGraphHook allows a function to obtain the group graph when a processor is started.
func WithGroupGraphHook(hook func(gg *GroupGraph)) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		hook(gg)
	}
}

// WithBackoffResetTimeout defines the timeout when the backoff
// will be reset.
func WithBackoffResetTimeout(duration time.Duration) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.backoffResetTime = duration
	}
}

// WithProducerDefaultHeaders configures the producer with default headers
// which are included with every emit.
func WithProducerDefaultHeaders(hdr Headers) ProcessorOption {
	return func(p *poptions, graph *GroupGraph) {
		p.producerDefaultHeaders = hdr
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
	return func(o *poptions, gg *GroupGraph) {
		o.nilHandling = nh
	}
}

// Tester interface to avoid import cycles when a processor needs to register to
// the tester.
type Tester interface {
	StorageBuilder() storage.Builder
	ProducerBuilder() ProducerBuilder
	ConsumerGroupBuilder() ConsumerGroupBuilder
	ConsumerBuilder() SaramaConsumerBuilder
	EmitterProducerBuilder() ProducerBuilder
	TopicManagerBuilder() TopicManagerBuilder
	RegisterGroupGraph(*GroupGraph) string
	RegisterEmitter(Stream, Codec)
	RegisterView(Table, Codec) string
}

// WithTester configures all external connections of a processor, ie, storage,
// consumer and producer
func WithTester(t Tester) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.builders.storage = t.StorageBuilder()
		o.builders.producer = t.ProducerBuilder()
		o.builders.topicmgr = t.TopicManagerBuilder()
		o.builders.consumerGroup = t.ConsumerGroupBuilder()
		o.builders.consumerSarama = t.ConsumerBuilder()
		o.partitionChannelSize = 0
		o.clientID = t.RegisterGroupGraph(gg)
	}
}

func (opt *poptions) applyOptions(gg *GroupGraph, opts ...ProcessorOption) error {
	opt.clientID = defaultClientID
	opt.log = defaultLogger
	opt.hasher = DefaultHasher()
	opt.backoffResetTime = defaultBackoffResetTime

	for _, o := range opts {
		o(opt, gg)
	}

	// StorageBuilder should always be set as a default option in NewProcessor
	if opt.builders.storage == nil {
		return fmt.Errorf("StorageBuilder not set")
	}

	if globalConfig.Producer.RequiredAcks == sarama.NoResponse {
		return fmt.Errorf("Processors do not work with `Config.Producer.RequiredAcks==sarama.NoResponse`, as it uses the response's offset to store the value")
	}

	if opt.builders.producer == nil {
		opt.builders.producer = DefaultProducerBuilder
	}

	if opt.builders.topicmgr == nil {
		opt.builders.topicmgr = DefaultTopicManagerBuilder
	}

	if opt.builders.consumerGroup == nil {
		opt.builders.consumerGroup = DefaultConsumerGroupBuilder
	}

	if opt.builders.consumerSarama == nil {
		opt.builders.consumerSarama = DefaultSaramaConsumerBuilder
	}

	if opt.builders.backoff == nil {
		opt.builders.backoff = DefaultBackoffBuilder
	}

	return nil
}

// WithRebalanceCallback sets the callback for when a new partition assignment
// is received. By default, this is an empty function.
func WithRebalanceCallback(cb RebalanceCallback) ProcessorOption {
	return func(o *poptions, gg *GroupGraph) {
		o.rebalanceCallback = cb
	}
}

///////////////////////////////////////////////////////////////////////////////
// view options
///////////////////////////////////////////////////////////////////////////////

// ViewOption defines a configuration option to be used when creating a view.
type ViewOption func(*voptions, Table, Codec)

type voptions struct {
	log              logger
	clientID         string
	tableCodec       Codec
	updateCallback   UpdateCallback
	hasher           func() hash.Hash32
	autoreconnect    bool
	backoffResetTime time.Duration

	builders struct {
		storage        storage.Builder
		consumerSarama SaramaConsumerBuilder
		topicmgr       TopicManagerBuilder
		backoff        BackoffBuilder
	}
}

// WithViewLogger sets the logger the view should use. By default, views
// use the standard library logger.
func WithViewLogger(l Logger) ViewOption {
	return func(o *voptions, table Table, codec Codec) {
		if prefixLogger, ok := l.(logger); ok {
			o.log = prefixLogger
		} else {
			o.log = wrapLogger(l, defaultLogger.debug)
		}
	}
}

// WithViewCallback defines the callback called upon recovering a message
// from the log.
func WithViewCallback(cb UpdateCallback) ViewOption {
	return func(o *voptions, table Table, codec Codec) {
		o.updateCallback = cb
	}
}

// WithViewStorageBuilder defines a builder for the storage of each partition.
func WithViewStorageBuilder(sb storage.Builder) ViewOption {
	return func(o *voptions, table Table, codec Codec) {
		o.builders.storage = sb
	}
}

// WithViewConsumerSaramaBuilder replaces the default sarama consumer builder
func WithViewConsumerSaramaBuilder(cgb SaramaConsumerBuilder) ViewOption {
	return func(o *voptions, table Table, codec Codec) {
		o.builders.consumerSarama = cgb
	}
}

// WithViewTopicManagerBuilder replaces the default topic manager.
func WithViewTopicManagerBuilder(tmb TopicManagerBuilder) ViewOption {
	return func(o *voptions, table Table, codec Codec) {
		o.builders.topicmgr = tmb
	}
}

// WithViewBackoffBuilder replaced the default backoff.
func WithViewBackoffBuilder(bb BackoffBuilder) ViewOption {
	return func(o *voptions, table Table, codec Codec) {
		o.builders.backoff = bb
	}
}

// WithViewHasher sets the hash function that assigns keys to partitions.
func WithViewHasher(hasher func() hash.Hash32) ViewOption {
	return func(o *voptions, table Table, codec Codec) {
		o.hasher = hasher
	}
}

// WithViewClientID defines the client ID used to identify with Kafka.
func WithViewClientID(clientID string) ViewOption {
	return func(o *voptions, table Table, codec Codec) {
		o.clientID = clientID
	}
}

// WithViewRestartable is kept only for backwards compatibility.
// DEPRECATED: since the behavior has changed, this name is misleading and should be replaced by
// WithViewAutoReconnect().
func WithViewRestartable() ViewOption {
	log.Printf("Warning: this option is deprecated and will be removed. Replace with WithViewAutoReconnect, which is semantically equivalent")
	return WithViewAutoReconnect()
}

// WithViewAutoReconnect defines the view is reconnecting internally, so Run() does not return
// in case of connection errors. The view must be shutdown by cancelling the context passed to Run()
func WithViewAutoReconnect() ViewOption {
	return func(o *voptions, table Table, codec Codec) {
		o.autoreconnect = true
	}
}

// WithViewBackoffResetTimeout defines the timeout when the backoff
// will be reset.
func WithViewBackoffResetTimeout(duration time.Duration) ViewOption {
	return func(o *voptions, table Table, codec Codec) {
		o.backoffResetTime = duration
	}
}

// WithViewTester configures all external connections of a processor, ie, storage,
// consumer and producer
func WithViewTester(t Tester) ViewOption {
	return func(o *voptions, table Table, codec Codec) {
		o.builders.storage = t.StorageBuilder()
		o.builders.topicmgr = t.TopicManagerBuilder()
		o.builders.consumerSarama = t.ConsumerBuilder()
		o.clientID = t.RegisterView(table, codec)
	}
}

func (opt *voptions) applyOptions(topic Table, codec Codec, opts ...ViewOption) error {
	opt.clientID = defaultClientID
	opt.log = defaultLogger
	opt.hasher = DefaultHasher()
	opt.backoffResetTime = defaultBackoffResetTime

	for _, o := range opts {
		o(opt, topic, codec)
	}

	// StorageBuilder should always be set as a default option in NewView
	if opt.builders.storage == nil {
		return fmt.Errorf("StorageBuilder not set")
	}

	if opt.builders.consumerSarama == nil {
		opt.builders.consumerSarama = DefaultSaramaConsumerBuilder
	}

	if opt.builders.topicmgr == nil {
		opt.builders.topicmgr = DefaultTopicManagerBuilder
	}

	if opt.builders.backoff == nil {
		opt.builders.backoff = DefaultBackoffBuilder
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////
// emitter options
///////////////////////////////////////////////////////////////////////////////

// EmitterOption defines a configuration option to be used when creating an
// emitter.
type EmitterOption func(*eoptions, Stream, Codec)

// emitter options
type eoptions struct {
	log      logger
	clientID string

	hasher         func() hash.Hash32
	defaultHeaders Headers

	builders struct {
		topicmgr TopicManagerBuilder
		producer ProducerBuilder
	}
}

// WithEmitterLogger sets the logger the emitter should use. By default,
// emitters use the standard library logger.
func WithEmitterLogger(l Logger) EmitterOption {
	return func(o *eoptions, topic Stream, codec Codec) {
		if prefixLogger, ok := l.(logger); ok {
			o.log = prefixLogger
		} else {
			o.log = wrapLogger(l, defaultLogger.debug)
		}
	}
}

// WithEmitterClientID defines the client ID used to identify with kafka.
func WithEmitterClientID(clientID string) EmitterOption {
	return func(o *eoptions, topic Stream, codec Codec) {
		o.clientID = clientID
	}
}

// WithEmitterTopicManagerBuilder replaces the default topic manager builder.
func WithEmitterTopicManagerBuilder(tmb TopicManagerBuilder) EmitterOption {
	return func(o *eoptions, topic Stream, codec Codec) {
		o.builders.topicmgr = tmb
	}
}

// WithEmitterProducerBuilder replaces the default producer builder.
func WithEmitterProducerBuilder(pb ProducerBuilder) EmitterOption {
	return func(o *eoptions, topic Stream, codec Codec) {
		o.builders.producer = pb
	}
}

// WithEmitterHasher sets the hash function that assigns keys to partitions.
func WithEmitterHasher(hasher func() hash.Hash32) EmitterOption {
	return func(o *eoptions, topic Stream, codec Codec) {
		o.hasher = hasher
	}
}

// WithEmitterTester configures the emitter to use passed tester.
// This is used for component tests
func WithEmitterTester(t Tester) EmitterOption {
	return func(o *eoptions, topic Stream, codec Codec) {
		o.builders.producer = t.EmitterProducerBuilder()
		o.builders.topicmgr = t.TopicManagerBuilder()
		t.RegisterEmitter(topic, codec)
	}
}

// WithEmitterDefaultHeaders configures the emitter with default headers
// which are included with every emit.
func WithEmitterDefaultHeaders(headers Headers) EmitterOption {
	return func(o *eoptions, _ Stream, _ Codec) {
		o.defaultHeaders = headers
	}
}

func (opt *eoptions) applyOptions(topic Stream, codec Codec, opts ...EmitterOption) {
	opt.clientID = defaultClientID
	opt.log = defaultLogger
	opt.hasher = DefaultHasher()

	for _, o := range opts {
		o(opt, topic, codec)
	}

	// config not set, use default one
	if opt.builders.producer == nil {
		opt.builders.producer = DefaultProducerBuilder
	}
	if opt.builders.topicmgr == nil {
		opt.builders.topicmgr = DefaultTopicManagerBuilder
	}
}

type ctxOptions struct {
	emitHeaders Headers
}

// ContextOption defines a configuration option to be used when performing
// operations on a context
type ContextOption func(*ctxOptions)

// WithCtxEmitHeaders sets kafka headers to use when emitting to kafka
func WithCtxEmitHeaders(headers Headers) ContextOption {
	return func(opts *ctxOptions) {
		// Accumulate headers rather than discard previous ones.
		opts.emitHeaders = opts.emitHeaders.Merged(headers)
	}
}

func (opt *ctxOptions) applyOptions(opts ...ContextOption) {
	for _, o := range opts {
		o(opt)
	}
}
