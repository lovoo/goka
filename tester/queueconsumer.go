package tester

import (
	"log"
	"sync"
	"time"

	"github.com/lovoo/goka/kafka"
)

type testerConsumer struct {
	tester           *Tester
	events           chan kafka.Event
	subscribedTopics map[string]*queueConsumer
	closeOnce        sync.Once
}
type consumerState int

type queueConsumer struct {
	queue           *queue
	offset          int64
	waitEventBuffer sync.WaitGroup
	eventBuffer     chan kafka.Event
	events          chan kafka.Event
	state           consumerState
}

func newQueueConsumer(topic string, tester *Tester, events chan kafka.Event) *queueConsumer {
	qc := &queueConsumer{
		queue:       tester.queueForTopic(topic),
		eventBuffer: make(chan kafka.Event, 1000),
		events:      events,
		offset:      0,
	}
	tester.queueForTopic(topic).addConsumer(qc)
	go qc.consumeBuffer()
	return qc
}

func (qc *queueConsumer) consumeBuffer() {
	for {
		event, ok := <-qc.eventBuffer
		if !ok {
			return
		}
		qc.events <- event
		qc.waitEventBuffer.Done()
	}
}

func (qc *queueConsumer) catchupAndSync() {
	qc.catchupQueue(-1)
	qc.waitEventBuffer.Wait()
}

func (qc *queueConsumer) ready() bool {
	return len(qc.eventBuffer) == 0
}

func (qc *queueConsumer) startGroupConsumer() {
	qc.catchupQueue(-1)
}

func (qc *queueConsumer) addToBuffer(event kafka.Event) {
	qc.waitEventBuffer.Add(1)
	qc.eventBuffer <- event
}

func (qc *queueConsumer) startSimpleConsumer(offset int64) {
	qc.addToBuffer(&kafka.BOF{
		Hwm:       qc.queue.hwm,
		Offset:    0,
		Partition: 0,
		Topic:     qc.queue.topic,
	})
	qc.catchupQueue(offset)
	qc.addToBuffer(&kafka.EOF{
		Hwm:       qc.queue.hwm,
		Partition: 0,
		Topic:     qc.queue.topic,
	})
}

func (qc *queueConsumer) close() {
	close(qc.eventBuffer)
}

func (qc *queueConsumer) catchupQueue(fromOffset int64) {
	if qc.offset > fromOffset {
		fromOffset = qc.offset
	}

	for _, msg := range qc.queue.messagesFromOffset(fromOffset) {
		qc.addToBuffer(&kafka.Message{
			Key:       string(msg.key),
			Offset:    msg.offset,
			Partition: 0,
			Timestamp: time.Unix(msg.offset, 0),
			Topic:     qc.queue.topic,
			Value:     msg.value,
		})
		// mark the offset as consumed
		qc.offset = msg.offset
	}

	// push some more NOPs
	for i := 0; i < 2; i++ {
		qc.addToBuffer(&kafka.NOP{
			Partition: 0,
			Topic:     qc.queue.topic,
		})
	}
}

func (qc *queueConsumer) rebalance() {
	log.Printf("doing rebalance")
	qc.addToBuffer(&kafka.Assignment{
		0: -1,
	})
}

func newTesterConsumer(tester *Tester) *testerConsumer {
	return &testerConsumer{
		tester:           tester,
		events:           make(chan kafka.Event, 0),
		subscribedTopics: make(map[string]*queueConsumer),
	}
}

// Events returns the event channel of the consumer mock
func (tc *testerConsumer) Events() <-chan kafka.Event {
	return tc.events
}

// Subscribe marks the consumer to subscribe to passed topics.
// The consumerMock simply marks the topics as handled to make sure to
// pass emitted messages back to the processor.
func (tc *testerConsumer) Subscribe(topics map[string]int64) error {
	log.Printf("Consumer: Subscribe to topics: %+v", topics)
	for topic := range topics {
		if _, exists := tc.subscribedTopics[topic]; exists {
			log.Printf("consumer for %s already exists. This is strange", topic)
		}
		tc.subscribedTopics[topic] = newQueueConsumer(topic, tc.tester, tc.events)
		tc.subscribedTopics[topic].rebalance()
	}
	return nil
}

// AddGroupPartition adds a partition for group consumption.
// No action required in the mock.
func (tc *testerConsumer) AddGroupPartition(partition int32) {
	log.Printf("%+v Consumer: AddGroupPartition", tc.subscribedTopics)
	for _, consumer := range tc.subscribedTopics {
		consumer.startGroupConsumer()
	}
}

// Commit commits an offest.
// No action required in the mock.
func (tc *testerConsumer) Commit(topic string, partition int32, offset int64) error {
	return nil
}

// AddPartition marks the topic as a table topic.
// The mock has to know the group table topic to ignore emit calls (which would never be consumed)
func (tc *testerConsumer) AddPartition(topic string, partition int32, initialOffset int64) error {
	if _, exists := tc.subscribedTopics[topic]; exists {
		log.Printf("Consumer for topic %s seems to already exist. This is strange", topic)
	}
	tc.subscribedTopics[topic] = newQueueConsumer(topic, tc.tester, tc.events)
	tc.subscribedTopics[topic].startSimpleConsumer(initialOffset)
	return nil
}

// RemovePartition removes a partition from a topic.
// No action required in the mock.
func (tc *testerConsumer) RemovePartition(topic string, partition int32) error {
	if cons, exists := tc.subscribedTopics[topic]; exists {
		cons.close()
		delete(tc.subscribedTopics, topic)
	} else {
		log.Printf("consumer for topic %s did not exist. Cannot Remove partition", topic)
	}
	return nil
}

// Close closes the consumer.
// No action required in the mock.
func (tc *testerConsumer) Close() error {
	tc.closeOnce.Do(func() {
		for _, cons := range tc.subscribedTopics {
			cons.close()
		}
		close(tc.events)
	})
	return nil
}
