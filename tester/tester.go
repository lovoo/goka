package tester

import (
	"fmt"
	"hash"
	"log"
	"sync"

	"github.com/facebookgo/ensure"
	"github.com/golang/protobuf/proto"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/storage"
)

// Codec decodes and encodes from and to []byte
type Codec interface {
	Encode(value interface{}) (data []byte, err error)
	Decode(data []byte) (value interface{}, err error)
}

// EmitHandler abstracts a function that allows to overwrite kafkamock's Emit function to
// simulate producer errors
type EmitHandler func(topic string, key string, value []byte) *kafka.Promise

type viewStorage struct {
	storage storage.Storage
	codec   Codec
}

type eventConsumer struct {
	consumers []chan kafka.Event
}

func (ec *eventConsumer) addConsumer(events chan kafka.Event) {
	ec.consumers = append(ec.consumers, events)
}

func (ec *eventConsumer) sendToAll(msg kafka.Event) {
	for _, cons := range ec.consumers {
		cons <- msg
	}
}

// Tester allows interacting with a test processor
type Tester struct {
	t T

	consumerMock     *consumerMock
	producerMock     *producerMock
	topicMgrMock     *topicMgrMock
	viewConsumerMock *viewConsumerMock
	emitHandler      EmitHandler
	storage          storage.Storage
	codec            Codec
	viewStorages     map[string]*viewStorage
	groupGraphs      map[goka.Group]*goka.GroupGraph
	offset           int64
	tableOffset      int64
	incomingEvents   chan kafka.Event
	// Stores a map of all topics that are handled by the processor.
	// Every time an emit is called, those messages for handled topics are relayed
	// after the consume-function has finished.
	// All other messages are stored in the emitted-slice for further inspection
	handledTopics map[string]*eventConsumer
	initOnce      sync.Once
	groupTopic    string
	emitted       []*kafka.Message

	groupTableCreator func() (string, []byte)
	callQueue         []func()
	wg                sync.WaitGroup
}

// T abstracts the interface we assume from the test case.
// Will most likely be *testing.T
type T interface {
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Fatal(a ...interface{})
}

// New returns a new testprocessor mocking every external service
// It should be passed as goka.WithTester to goka.NewProcessor. It essentially
// replaces the storage/consumer/producer/topicmanager with a mock.
// For example, a normal call to NewProcessor like this
//     goka.NewProcessor(brokers, group, subscriptions,
//                       option_a,
//                       option_b,
//                       option_c,
//     )
// would become in the unit test:
// tester := tester.New(t)
// NewProcessor(brokers, group, subscriptions,
//                       option_a,
//                       option_b,
//                       option_c,
//                       WithTester(tester),
//     )
func New(t T) *Tester {
	tester := &Tester{
		storage:        storage.NewMemory(),
		t:              t,
		incomingEvents: make(chan kafka.Event),
		handledTopics:  make(map[string]*eventConsumer),
		codec:          new(codec.Bytes),
		viewStorages:   make(map[string]*viewStorage),
		groupGraphs:    make(map[goka.Group]*goka.GroupGraph),
	}
	tester.consumerMock = newConsumerMock(tester)
	tester.producerMock = newProducerMock(tester.handleEmit)
	tester.topicMgrMock = newTopicMgrMock(tester)
	tester.viewConsumerMock = newViewConsumerMock(tester)

	return tester
}

func (km *Tester) RegisterGroupGraph(gg *goka.GroupGraph) {
	km.groupGraphs[gg.Group()] = gg
}

// SetCodec sets the codec for the group table.
func (km *Tester) SetCodec(codec Codec) *Tester {
	km.codec = codec
	return km
}

// AddMockLookupTable configures mock table to be used as lookup.
// If this is not set, all lookup-calls from the tested processor will be nil
func (km *Tester) AddMockLookupTable(table string, codec Codec) {
	km.viewStorages[table] = &viewStorage{
		codec:   codec,
		storage: storage.NewMemory(),
	}
}

func (km *Tester) isViewTopic(topic string) bool {
	for _, graph := range km.groupGraphs {
		for _, viewtopic := range graph.JointTables() {
			if viewtopic.Topic() == topic {
				return true
			}
		}

		for _, viewtopic := range graph.LookupTables() {
			if viewtopic.Topic() == topic {
				return true
			}
		}
	}
	return false
}
func (km *Tester) AddEventConsumer(topic string, events chan kafka.Event) {
	if _, exists := km.handledTopics[topic]; !exists {
		km.handledTopics[topic] = new(eventConsumer)
	}
	km.handledTopics[topic].addConsumer(events)
	log.Printf("Adding event consumer for %s (is view: %t)", topic, km.isViewTopic(topic))
	if !km.isViewTopic(topic) {
		go func() {
			events <- &kafka.Assignment{
				0: -1,
			}
			events <- &kafka.EOF{Partition: 0}
			events <- &kafka.NOP{Partition: -1}
			log.Printf("Initialized group protocol for topic %s", topic)
		}()
	}
}

func (km *Tester) startEventForwarding() {
	km.initOnce.Do(func() {
		go func() {
			for {
				select {
				case event, ok := <-km.incomingEvents:
					if !ok {
						return
					}
					var topic string
					switch msg := event.(type) {
					case *kafka.Message:
						topic = msg.Topic
					case *kafka.EOF:
						topic = msg.Topic
					case *kafka.BOF:
						topic = msg.Topic
					case *kafka.Error:
						log.Fatalf("Got kafka error. Can this happen?")
					}
					consumers, exists := km.handledTopics[topic]
					if !exists {
						log.Printf("topic not handled")
					} else {
						consumers.sendToAll(event)
					}
				}
			}
		}()
	})
}

// SetLookupValue registers a key-value pair in a confirgured lookup table
func (km *Tester) SetLookupValue(table string, key string, value interface{}) {
	viewStorage, exists := km.viewStorages[table]

	if !exists {
		panic(fmt.Errorf("Lookup table for %s not set. Did you call AddMockLookupTable?", table))
	}
	encoded, err := viewStorage.codec.Encode(value)

	if err != nil {
		panic(fmt.Errorf("Error encoding value for stoage in Lookup table %s: %v", table, err))
	}
	viewStorage.storage.Set(key, encoded)
}

// TopicManagerBuilder returns the topicmanager builder when this tester is used as an option
// to a processor
func (km *Tester) TopicManagerBuilder() kafka.TopicManagerBuilder {
	return func(brokers []string) (kafka.TopicManager, error) {
		return km.topicMgrMock, nil
	}
}

// ConsumerBuilder returns the consumer builder when this tester is used as an option
// to a processor
func (km *Tester) ConsumerBuilder() kafka.ConsumerBuilder {
	return func(b []string, group, clientID string) (kafka.Consumer, error) {
		return newConsumerMock(km), nil
	}
}

// ProducerBuilder returns the producer builder when this tester is used as an option
// to a processor
func (km *Tester) ProducerBuilder() kafka.ProducerBuilder {
	return func(b []string, cid string, hasher func() hash.Hash32) (kafka.Producer, error) {
		return km.producerMock, nil
	}
}

// StorageBuilder returns the storage builder when this tester is used as an option
// to a processor
func (km *Tester) StorageBuilder() storage.Builder {
	return func(topic string, partition int32) (storage.Storage, error) {
		if viewStorage, exists := km.viewStorages[topic]; exists {
			return viewStorage.storage, nil
		}
		return km.storage, nil
	}
}

// initProtocol initiates the protocol with the client basically making the KafkaMock
// usable.
// func (km *Tester) initProtocol() {
// 	defer func() {
// 		if r := recover(); r != nil {
// 			log.Printf("tester: panic initProtocol: %+v", r)
// 		}
// 	}()
// 	km.consumerEvents <- &kafka.Assignment{
// 		0: -1,
// 	}
//
// 	for km.groupTableCreator != nil {
// 		key, value := km.groupTableCreator()
// 		if key == "" || value == nil {
// 			break
// 		}
// 		km.consumerEvents <- &kafka.Message{
// 			Topic:     km.groupTopic,
// 			Partition: 0,
// 			Offset:    km.tableOffset,
// 			Key:       key,
// 			Value:     value,
// 		}
// 	}
//
// 	km.consumerEvents <- &kafka.EOF{Partition: 0}
// 	km.consumerEvents <- &kafka.NOP{Partition: -1}
//
// 	for ev := range km.incomingEvents {
// 		km.consumerEvents <- ev
// 	}
// }

// ConsumeProto simulates a message on kafka in a topic with a key.
func (km *Tester) ConsumeProto(topic string, key string, msg proto.Message) {
	data, err := proto.Marshal(msg)
	if err != nil && km.t != nil {
		km.t.Errorf("Error marshaling message for consume: %v", err)
	}
	km.consumeData(topic, key, data)
	km.makeCalls()
}

// ConsumeString simulates a message with a string payload.
func (km *Tester) ConsumeString(topic string, key string, msg string) {
	km.consumeData(topic, key, []byte(msg))
	km.makeCalls()
}

// Consume simulates a message with a byte slice payload.
func (km *Tester) Consume(topic string, key string, msg []byte) {
	km.consumeData(topic, key, msg)
	km.makeCalls()
}

// ConsumeData simulates a message with a byte slice payload. This is the same
// as Consume.
// ConsumeData is a helper function consuming marshalled data. This function is
// used by ConsumeProto by the test case as well as any emit calls of the
// processor being tested.
func (km *Tester) ConsumeData(topic string, key string, data []byte) {
	km.consumeData(topic, key, data)
	km.makeCalls()
}

func (km *Tester) consumeData(topic string, key string, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("tester: panic ConsumeData: %+v\n", r)
		}
	}()
	km.offset++
	kafkaMsg := &kafka.Message{
		Topic:     topic,
		Partition: 0,
		Offset:    km.offset,

		Key:   key,
		Value: data,
	}
	// send message to processing goroutine
	km.incomingEvents <- kafkaMsg
	// wait until partition processing goroutine processes message by requiring it to read
	// the following NOP.
	km.incomingEvents <- &kafka.NOP{Partition: 0}
	km.incomingEvents <- &kafka.NOP{Partition: 0}

	// wait util processor goroutine is ready
	km.incomingEvents <- &kafka.NOP{Partition: -1}
	km.incomingEvents <- &kafka.NOP{Partition: -1}
}

func (km *Tester) consumeError(err error) {
	km.incomingEvents <- &kafka.Error{Err: err}
	// no need to send NOP (actuallly we can't, otherwise we might panic
	// as the channels are already closed due to the error first).
}

// ValueForKey attempts to get a value from KafkaMock's storage.
func (km *Tester) ValueForKey(key string) interface{} {
	item, err := km.storage.Get(key)
	ensure.Nil(km.t, err)
	if item == nil {
		return nil
	}
	value, err := km.codec.Decode(item)
	ensure.Nil(km.t, err)
	return value
}

// SetValue sets a value in the storage.
func (km *Tester) SetValue(key string, value interface{}) {
	data, err := km.codec.Encode(value)
	ensure.Nil(km.t, err)
	err = km.storage.Set(key, data)
	ensure.Nil(km.t, err)
}

// ReplaceEmitHandler replaces the emitter.
func (km *Tester) ReplaceEmitHandler(emitter EmitHandler) {
	km.producerMock.emitter = emitter
}

// ExpectEmit ensures a message exists in passed topic and key. The message may be
// inspected/unmarshalled by a passed expecter function.
func (km *Tester) ExpectEmit(topic string, key string, expecter func(value []byte)) {
	for i := 0; i < len(km.emitted); i++ {
		msg := km.emitted[i]
		if msg.Topic != topic || msg.Key != key {
			continue
		}
		if expecter != nil {
			expecter(msg.Value)
		}
		// remove element from slice
		// https://github.com/golang/go/wiki/SliceTricks
		km.emitted = append(km.emitted[:i], km.emitted[i+1:]...)
		return
	}

	km.t.Errorf("Expected emit for key %s in topic %s was not present.", key, topic)
}

// ExpectAllEmitted calls passed expected-emit-handler function for all emitted values and clears the
// emitted values
func (km *Tester) ExpectAllEmitted(handler func(topic string, key string, value []byte)) {
	for _, emitted := range km.emitted {
		handler(emitted.Topic, emitted.Key, emitted.Value)
	}
	km.emitted = make([]*kafka.Message, 0)
}

// Finish marks the kafkamock that there is no emit to be expected.
// Set @param fail to true, if kafkamock is supposed to fail the test case in case
// of remaining emits.
// Clears the list of emits either case.
// This should always be called at the end of a test case to make sure
// no emits of prior test cases are stuck in the list and mess with the test results.
func (km *Tester) Finish(fail bool) {
	if len(km.emitted) > 0 {
		if fail {
			km.t.Errorf("The following emits are still in the list, although it's supposed to be empty:")
			for _, emitted := range km.emitted {
				km.t.Errorf("    topic: %s key: %s", emitted.Topic, emitted.Key)
			}
		}
	}
	km.emitted = make([]*kafka.Message, 0)
}

// handleEmit handles an Emit-call on the producerMock.
// This takes care of queueing calls
// to handled topics or putting the emitted messages in the emitted-messages-list
func (km *Tester) handleEmit(topic string, key string, value []byte) *kafka.Promise {
	promise := kafka.NewPromise()
	if topic == km.groupTopic {
		return promise.Finish(nil)
	}
	if _, hasTopic := km.handledTopics[topic]; hasTopic {
		km.newCall(func() {
			km.consumeData(topic, key, value)
		})
	} else {
		km.offset++
		km.emitted = append(km.emitted, &kafka.Message{
			Topic:  topic,
			Key:    key,
			Value:  value,
			Offset: km.offset,
		})
	}
	return promise.Finish(nil)
}

// creates a new call being executed after the consume function has run.
func (km *Tester) newCall(call func()) {
	km.wg.Add(1)
	km.callQueue = append(km.callQueue, call)
}

// executes all calls on the call queue.
// Executing calls may put new calls on the queue (if they emit something),
// so this function executes until no further calls are being made.
func (km *Tester) makeCalls() {
	go func() {
		for len(km.callQueue) > 0 {
			call := km.callQueue[0]
			call()
			km.callQueue = km.callQueue[1:]
			km.wg.Done()
		}
	}()
	km.wg.Wait()
}

// ClearValues resets everything that might be in the storage by deleting everything
// using the iterator.
func (km *Tester) ClearValues() {
	it, _ := km.storage.Iterator()
	for it.Next() {
		km.storage.Delete(string(it.Key()))
	}
}

type viewConsumerMock struct {
	events    chan kafka.Event
	closeOnce sync.Once
}

func newViewConsumerMock(tester *Tester) *viewConsumerMock {
	return &viewConsumerMock{
		events: make(chan kafka.Event, 1),
	}
}

// Events returns the event channel of the consumer mock
func (vcm *viewConsumerMock) Events() <-chan kafka.Event {
	return vcm.events
}

// Subscribe marks the consumer to subscribe to passed topics.
// the viewconsumer does not subscribe to topics
func (vcm *viewConsumerMock) Subscribe(topics map[string]int64) error {
	return nil
}

// AddGroupPartition adds a partition for group consumption.
// No action required in the mock.
func (vcm *viewConsumerMock) AddGroupPartition(partition int32) {
}

// Commit commits an offest.
// No action required in the mock.
func (vcm *viewConsumerMock) Commit(topic string, partition int32, offset int64) error {
	return nil
}

// AddPartition marks the topic as a table topic.
// The mock has to know the group table topic to ignore emit calls (which would never be consumed)
func (vcm *viewConsumerMock) AddPartition(topic string, partition int32, initialOffset int64) error {
	return nil
}

// RemovePartition removes a partition from a topic.
// No action required in the mock.
func (vcm *viewConsumerMock) RemovePartition(topic string, partition int32) error {
	return nil
}

// Close closes the consumer.
// No action required in the mock.
func (vcm *viewConsumerMock) Close() error {
	vcm.closeOnce.Do(func() {
		close(vcm.events)
	})
	return nil
}

type consumerMock struct {
	tester    *Tester
	events    chan kafka.Event
	closeOnce sync.Once
}

func newConsumerMock(tester *Tester) *consumerMock {
	return &consumerMock{
		tester: tester,
		events: make(chan kafka.Event),
	}
}

// Events returns the event channel of the consumer mock
func (km *consumerMock) Events() <-chan kafka.Event {
	return km.events
}

// Subscribe marks the consumer to subscribe to passed topics.
// The consumerMock simply marks the topics as handled to make sure to
// pass emitted messages back to the processor.
func (km *consumerMock) Subscribe(topics map[string]int64) error {
	for topic := range topics {
		km.tester.AddEventConsumer(topic, km.events)
	}
	km.tester.startEventForwarding()
	return nil
}

// AddGroupPartition adds a partition for group consumption.
// No action required in the mock.
func (km *consumerMock) AddGroupPartition(partition int32) {
}

// Commit commits an offest.
// No action required in the mock.
func (km *consumerMock) Commit(topic string, partition int32, offset int64) error {
	return nil
}

// AddPartition marks the topic as a table topic.
// The mock has to know the group table topic to ignore emit calls (which would never be consumed)
func (km *consumerMock) AddPartition(topic string, partition int32, initialOffset int64) error {
	km.tester.AddEventConsumer(topic, km.events)
	km.tester.startEventForwarding()
	return nil
}

// RemovePartition removes a partition from a topic.
// No action required in the mock.
func (km *consumerMock) RemovePartition(topic string, partition int32) error {
	return nil
}

// Close closes the consumer.
// No action required in the mock.
func (km *consumerMock) Close() error {
	km.closeOnce.Do(func() {
		close(km.events)
	})
	return nil
}

type topicMgrMock struct {
	tester *Tester
}

// EnsureTableExists checks that a table (log-compacted topic) exists, or create one if possible
func (tm *topicMgrMock) EnsureTableExists(topic string, npar int) error {
	return nil
}

// EnsureStreamExists checks that a stream topic exists, or create one if possible
func (tm *topicMgrMock) EnsureStreamExists(topic string, npar int) error {
	return nil
}

// Partitions returns the number of partitions of a topic, that are assigned to the running
// instance, i.e. it doesn't represent all partitions of a topic.
func (tm *topicMgrMock) Partitions(topic string) ([]int32, error) {
	return []int32{0}, nil
}

// Close closes the topic manager.
// No action required in the mock.
func (tm *topicMgrMock) Close() error {
	return nil
}

func newTopicMgrMock(tester *Tester) *topicMgrMock {
	return &topicMgrMock{
		tester: tester,
	}
}

type producerMock struct {
	emitter EmitHandler
}

func newProducerMock(emitter EmitHandler) *producerMock {
	return &producerMock{
		emitter: emitter,
	}
}

// Emit emits messages to arbitrary topics.
// The mock simply forwards the emit to the KafkaMock which takes care of queueing calls
// to handled topics or putting the emitted messages in the emitted-messages-list
func (p *producerMock) Emit(topic string, key string, value []byte) *kafka.Promise {
	return p.emitter(topic, key, value)
}

// Close closes the producer mock
// No action required in the mock.
func (p *producerMock) Close() error {
	fmt.Println("Closing producer mock")
	return nil
}
