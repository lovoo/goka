package tester

import (
	"fmt"
	"github.com/lovoo/goka/headers"
	"hash"
	"reflect"
	"sync"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/storage"

	"github.com/Shopify/sarama"
)

type emitOption struct {
	headers headers.Headers
}

// EmitOption defines a configuration option for emitting messages
type EmitOption func(*emitOption)

// WithHeaders sets kafka headers to use when emitting to kafka
func WithHeaders(hdr headers.Headers) EmitOption {
	return func(opts *emitOption) {
		if opts.headers == nil {
			opts.headers = make(headers.Headers)
		}

		for k, v := range hdr {
			opts.headers[k] = v
		}
	}
}

func (opt *emitOption) applyOptions(opts ...EmitOption) {
	for _, o := range opts {
		o(opt)
	}
}

type debugLogger interface {
	Printf(s string, args ...interface{})
}

type nilLogger int

func (*nilLogger) Printf(s string, args ...interface{}) {}

var (
	logger debugLogger = new(nilLogger)
)

// T abstracts the interface we assume from the test case.
// Will most likely be T
type T interface {
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Fatal(a ...interface{})
}

// Tester mimicks kafka for complex highlevel testing of single or multiple processors/views/emitters
type Tester struct {
	t        T
	producer *producerMock
	tmgr     goka.TopicManager

	mClients sync.RWMutex
	clients  map[string]*client

	mCodecs sync.RWMutex
	codecs  map[string]goka.Codec

	mQueues     sync.Mutex
	topicQueues map[string]*queue

	mStorages sync.Mutex
	storages  map[string]storage.Storage
}

// New creates a new tester instance
func New(t T) *Tester {

	tt := &Tester{
		t: t,

		clients: make(map[string]*client),

		codecs:      make(map[string]goka.Codec),
		topicQueues: make(map[string]*queue),
		storages:    make(map[string]storage.Storage),
	}
	tt.tmgr = NewMockTopicManager(tt, 1, 1)
	tt.producer = newProducerMock(tt.handleEmit)

	return tt
}

func (tt *Tester) nextClient() *client {
	tt.mClients.Lock()
	defer tt.mClients.Unlock()
	c := &client{
		clientID: fmt.Sprintf("client-%d", len(tt.clients)),
		consumer: newConsumerMock(tt),
	}
	tt.clients[c.clientID] = c
	return c
}

// ConsumerGroupBuilder builds a builder. The builder returns the consumergroup for passed client-ID
// if it was expected by registering the processor to the Tester
func (tt *Tester) ConsumerGroupBuilder() goka.ConsumerGroupBuilder {
	return func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) {
		tt.mClients.RLock()
		defer tt.mClients.RUnlock()
		client, exists := tt.clients[clientID]
		if !exists {
			return nil, fmt.Errorf("cannot create consumergroup because no client registered with ID: %s", clientID)
		}

		if client.consumerGroup == nil {
			return nil, fmt.Errorf("Did not expect a group graph")
		}

		return client.consumerGroup, nil
	}
}

// ConsumerBuilder creates a consumerbuilder that builds consumers for passed clientID
func (tt *Tester) ConsumerBuilder() goka.SaramaConsumerBuilder {
	return func(brokers []string, clientID string) (sarama.Consumer, error) {
		tt.mClients.RLock()
		defer tt.mClients.RUnlock()

		client, exists := tt.clients[clientID]
		if !exists {
			return nil, fmt.Errorf("cannot create sarama consumer because no client registered with ID: %s", clientID)
		}

		return client.consumer, nil
	}
}

// EmitterProducerBuilder creates a producer builder used for Emitters.
// Emitters need to flush when emitting messages.
func (tt *Tester) EmitterProducerBuilder() goka.ProducerBuilder {
	builder := tt.ProducerBuilder()
	return func(b []string, cid string, hasher func() hash.Hash32) (goka.Producer, error) {
		prod, err := builder(b, cid, hasher)
		return &flushingProducer{
			tester:   tt,
			producer: prod,
		}, err
	}
}

// handleEmit handles an Emit-call on the producerMock.
// This takes care of queueing calls
// to handled topics or putting the emitted messages in the emitted-messages-list
func (tt *Tester) handleEmit(topic string, key string, value []byte, options ...EmitOption) *goka.Promise {
	opts := new(emitOption)
	opts.applyOptions(options...)
	_, finisher := goka.NewPromiseWithFinisher()
	offset := tt.pushMessage(topic, key, value, opts.headers)
	return finisher(&sarama.ProducerMessage{Offset: offset}, nil)
}

func (tt *Tester) pushMessage(topic string, key string, data []byte, hdr headers.Headers) int64 {
	return tt.getOrCreateQueue(topic).push(key, data, hdr)
}

func (tt *Tester) ProducerBuilder() goka.ProducerBuilder {
	return func(b []string, cid string, hasher func() hash.Hash32) (goka.Producer, error) {
		return tt.producer, nil
	}
}

func (tt *Tester) TopicManagerBuilder() goka.TopicManagerBuilder {
	return func(brokers []string) (goka.TopicManager, error) {
		return tt.tmgr, nil
	}
}

// RegisterGroupGraph is called by a processor when the tester is passed via
// `WithTester(..)`.
// This will setup the tester with the neccessary consumer structure
func (tt *Tester) RegisterGroupGraph(gg *goka.GroupGraph) string {

	client := tt.nextClient()
	// we need to expect a consumer group so we're creating one in the client
	if gg.GroupTable() != nil || len(gg.InputStreams()) > 0 {
		client.consumerGroup = newConsumerGroup(tt.t, tt)
	}

	// register codecs
	if gg.GroupTable() != nil {
		tt.registerCodec(gg.GroupTable().Topic(), gg.GroupTable().Codec())
	}

	for _, input := range gg.InputStreams() {
		tt.registerCodec(input.Topic(), input.Codec())
	}

	for _, output := range gg.OutputStreams() {
		tt.registerCodec(output.Topic(), output.Codec())
	}

	for _, join := range gg.JointTables() {
		tt.registerCodec(join.Topic(), join.Codec())
	}

	if loop := gg.LoopStream(); loop != nil {
		tt.registerCodec(loop.Topic(), loop.Codec())
	}

	for _, lookup := range gg.LookupTables() {
		tt.registerCodec(lookup.Topic(), lookup.Codec())
	}

	return client.clientID
}

// RegisterView registers a new view to the tester
func (tt *Tester) RegisterView(table goka.Table, c goka.Codec) string {
	tt.registerCodec(string(table), c)
	client := tt.nextClient()
	client.requireConsumer(string(table))
	return client.clientID
}

// RegisterEmitter registers an emitter to be working with the tester.
func (tt *Tester) RegisterEmitter(topic goka.Stream, codec goka.Codec) {
	tt.registerCodec(string(topic), codec)
}

func (tt *Tester) getOrCreateQueue(topic string) *queue {
	tt.mQueues.Lock()
	defer tt.mQueues.Unlock()
	queue, exists := tt.topicQueues[topic]
	if !exists {
		queue = newQueue(topic)
		tt.topicQueues[topic] = queue
	}
	return queue
}

func (tt *Tester) codecForTopic(topic string) goka.Codec {
	// lock the access to codecs-map
	tt.mCodecs.RLock()
	defer tt.mCodecs.RUnlock()

	codec, exists := tt.codecs[topic]
	if !exists {
		panic(fmt.Errorf("no codec for topic %s registered", topic))
	}
	return codec
}

func (tt *Tester) registerCodec(topic string, codec goka.Codec) {
	// lock the access to codecs-map
	tt.mCodecs.Lock()
	defer tt.mCodecs.Unlock()

	// create a queue, we're going to need it anyway
	tt.getOrCreateQueue(topic)

	if existingCodec, exists := tt.codecs[topic]; exists {
		if reflect.TypeOf(codec) != reflect.TypeOf(existingCodec) {
			panic(fmt.Errorf("There are different codecs for the same topic. This is messed up (%#v, %#v)", codec, existingCodec))
		}
	}
	tt.codecs[topic] = codec
}

// TableValue attempts to get a value from any table that is used in the tester
func (tt *Tester) TableValue(table goka.Table, key string) interface{} {
	tt.waitStartup()

	topic := string(table)
	tt.mStorages.Lock()
	st, exists := tt.storages[topic]
	tt.mStorages.Unlock()
	if !exists {
		panic(fmt.Errorf("topic %s does not exist", topic))
	}
	item, err := st.Get(key)
	if err != nil {
		tt.t.Fatalf("Error getting table value from storage (table=%s, key=%s): %v", table, key, err)
	}
	if item == nil {
		return nil
	}
	value, err := tt.codecForTopic(topic).Decode(item)
	if err != nil {
		tt.t.Fatalf("error decoding value from storage (table=%s, key=%s, value=%v): %v", table, key, item, err)
	}
	return value
}

// SetTableValue sets a value in a processor's or view's table direcly via storage
// This method blocks until all expected clients are running, so make sure
// to call it *after* you have started all processors/views, otherwise it'll deadlock.
func (tt *Tester) SetTableValue(table goka.Table, key string, value interface{}) {
	tt.waitStartup()

	topic := string(table)
	st, err := tt.getOrCreateStorage(topic)
	if err != nil {
		panic(fmt.Errorf("error creating storage for topic %s: %v", topic, err))
	}
	data, err := tt.codecForTopic(topic).Encode(value)
	if err != nil {
		tt.t.Fatalf("error decoding value from storage (table=%s, key=%s, value=%v): %v", table, key, value, err)
	}

	err = st.Set(key, data)
	if err != nil {
		panic(fmt.Errorf("Error setting key %s in storage %s: %v", key, table, err))
	}
}
func (tt *Tester) getOrCreateStorage(table string) (storage.Storage, error) {
	tt.mStorages.Lock()
	defer tt.mStorages.Unlock()

	st := tt.storages[table]
	if st == nil {
		st = storage.NewMemory()
		tt.storages[table] = st
	}
	return st, nil
}

// StorageBuilder builds inmemory storages
func (tt *Tester) StorageBuilder() storage.Builder {
	return func(topic string, partition int32) (storage.Storage, error) {
		return tt.getOrCreateStorage(topic)
	}
}

// ClearValues clears all table values in all storages
func (tt *Tester) ClearValues() {
	tt.mStorages.Lock()
	defer tt.mStorages.Unlock()
	for topic, st := range tt.storages {
		logger.Printf("clearing all values from storage for topic %s", topic)
		it, _ := st.Iterator()
		for it.Next() {
			st.Delete(string(it.Key()))
		}
	}

}

// NewQueueTracker creates a new queue tracker
func (tt *Tester) NewQueueTracker(topic string) *QueueTracker {
	return newQueueTracker(tt, tt.t, topic)
}

func (tt *Tester) waitStartup() {
	tt.mClients.RLock()
	defer tt.mClients.RUnlock()

	for _, client := range tt.clients {
		client.waitStartup()
	}
}

func (tt *Tester) waitForClients() {
	logger.Printf("waiting for consumers")

	tt.mClients.RLock()
	defer tt.mClients.RUnlock()
	for {
		var totalCatchup int
		for _, client := range tt.clients {
			totalCatchup += client.catchup()
		}

		if totalCatchup == 0 {
			break
		}
	}

	logger.Printf("waiting for consumers done")
}

// Consume pushes a message for topic/key to be consumed by all processors/views
// whoever is using it being registered to the Tester
func (tt *Tester) Consume(topic string, key string, msg interface{}, options ...EmitOption) {
	tt.waitStartup()

	opts := new(emitOption)
	opts.applyOptions(options...)
	value := reflect.ValueOf(msg)
	if msg == nil || (value.Kind() == reflect.Ptr && value.IsNil()) {
		tt.pushMessage(topic, key, nil, opts.headers)
	} else {
		data, err := tt.codecForTopic(topic).Encode(msg)
		if err != nil {
			panic(fmt.Errorf("Error encoding value %v: %v", msg, err))
		}
		tt.pushMessage(topic, key, data, opts.headers)
	}

	tt.waitForClients()
}
