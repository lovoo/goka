package tester

import (
	"fmt"
	"sync"
	"time"

	"github.com/lovoo/goka/kafka"
)

type consumer struct {
	tester           *Tester
	events           chan kafka.Event
	subscribedTopics map[string]*queueConsumer
	simpleConsumers  map[string]*queueConsumer
	closeOnce        sync.Once
}

const (
	unbound State = iota
	bound
	running
	stopping
	stopped
	killed
)

type queueConsumer struct {
	queue           *queue
	nextOffset      int64
	waitEventBuffer sync.WaitGroup
	state           *Signal
	eventBuffer     chan kafka.Event
	events          chan kafka.Event
	consumer        *consumer
}

func newQueueConsumer(topic string, queue *queue) *queueConsumer {
	qc := &queueConsumer{
		queue:       queue,
		eventBuffer: make(chan kafka.Event, 100000),
		state:       NewSignal(unbound, bound, stopped, stopping, running, killed).SetState(unbound),
	}
	return qc
}

func (qc *queueConsumer) bindToConsumer(cons *consumer) {
	logger.Printf("binding consumer to topic %s", qc.queue.topic)
	if !qc.state.IsState(unbound) {
		panic(fmt.Errorf("error binding %s to consumer. Already bound", qc.queue.topic))
	}
	qc.state.SetState(bound)
	qc.consumer = cons
	qc.events = cons.events
}

func (qc *queueConsumer) isBound() bool {
	return !qc.state.IsState(unbound)
}

func (qc *queueConsumer) isRunning() bool {
	return qc.state.IsState(running)
}

func (qc *queueConsumer) setRunning() {
	qc.state.SetState(running)
}

func (qc *queueConsumer) stop() {
	logger.Printf("closing the queueConsumer for topic %s", qc.queue.topic)
	if !qc.state.IsState(running) {
		panic(fmt.Sprintf("trying to stop consumer %s which is not running (state=%d)", qc.queue.topic, qc.state.State()))
	}
	qc.state.SetState(stopping)
	logger.Printf("[consumer %s]waiting for stopped", qc.queue.topic)
	<-qc.state.WaitForState(stopped)
	logger.Printf("[consumer %s] stopped", qc.queue.topic)
}

func (qc *queueConsumer) kill() {
	qc.stop()
	qc.state.SetState(killed)
}

func (qc *queueConsumer) startLoop(setRunning bool) {
	logger.Printf("starting queue consumer %s", qc.queue.topic)
	// not bound or already running
	if qc.state.IsState(unbound) || qc.state.IsState(running) || qc.state.IsState(stopping) {
		panic(fmt.Errorf("the queue consumer %s is in state %v. Cannot start", qc.queue.topic, qc.state.State()))
	}
	if setRunning {
		qc.state.SetState(running)
	}
	go qc.consumeBuffer()
}

func (qc *queueConsumer) consumeBuffer() {
	defer func() {
		err := recover()
		if err != nil {
			logger.Printf("Error consuming the buffer: %v", err)
		}
		qc.state.SetState(stopped)
	}()

	for {
		select {
		case event, ok := <-qc.eventBuffer:
			if !ok {
				return
			}
			logger.Printf("[consumer %s]: From Buffer %#v", qc.queue.topic, event)

			select {
			case qc.events <- event:
				qc.waitEventBuffer.Done()

				logger.Printf("[consumer %s]: Buffer->Events %#v", qc.queue.topic, event)
			case <-qc.state.WaitForState(stopping):
				logger.Printf("[consumer %s] received stopping signal", qc.queue.topic)

				logger.Printf("[consumer %s] DROPPING MESSAGE (%#v) because the consumer is closed", qc.queue.topic, event)
				qc.waitEventBuffer.Done()
				return
			}

		case <-qc.state.WaitForState(stopping):
			logger.Printf("[consumer %s] received stopping signal", qc.queue.topic)
			return
		}
	}
}

func (qc *queueConsumer) catchupAndSync() int {
	logger.Printf("[consumer %s] catching up", qc.queue.topic)
	numMessages := qc.catchupQueue(-1)
	logger.Printf("[consumer %s] catching up DONE (%d messages)", qc.queue.topic, numMessages)

	eventsProcessed := make(chan struct{})
	go func() {
		logger.Printf("[consumer %s] wait for all events to be processed", qc.queue.topic)
		qc.waitEventBuffer.Wait()
		logger.Printf("[consumer %s] done processing events", qc.queue.topic)
		close(eventsProcessed)
	}()

	select {
	case <-eventsProcessed:
	case <-qc.state.WaitForState(killed):
		// The consumer was killed, so we assume the test is done already.
		return 0
	case <-qc.state.WaitForState(stopped):
	}
	return numMessages
}

func (qc *queueConsumer) startGroupConsumer() {
	logger.Printf("[consumer %s] starting group consumer", qc.queue.topic)
	qc.catchupQueue(-1)
}

func (qc *queueConsumer) addToBuffer(event kafka.Event) {
	qc.waitEventBuffer.Add(1)

	qc.eventBuffer <- event
}

func (qc *queueConsumer) startSimpleConsumer(offset int64, firstStart bool) {
	logger.Printf("[consumer %s] starting simple consumer (offset=%d)", qc.queue.topic, offset)
	if firstStart {
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
	qc.startLoop(true)
}

func (qc *queueConsumer) catchupQueue(fromOffset int64) int {
	// we'll always get from the beginning when the consumer
	// requests -1 or -2 (for end or beginning resp)
	if fromOffset < 0 {
		fromOffset = qc.nextOffset
	}

	// count how many messages we had to catch up on
	var forwardedMessages int
	for _, msg := range qc.queue.messagesFromOffset(fromOffset) {
		qc.addToBuffer(&kafka.Message{
			Key:       string(msg.key),
			Offset:    msg.offset,
			Partition: 0,
			Timestamp: time.Unix(msg.offset, 0),
			Topic:     qc.queue.topic,
			Value:     msg.value,
		})
		forwardedMessages++
		// mark the next offset to consume in case we stop here
		qc.nextOffset = msg.offset + 1
	}

	qc.addToBuffer(&kafka.EOF{
		Hwm:       qc.queue.hwm,
		Partition: 0,
		Topic:     qc.queue.topic,
	})

	// push some more NOPs
	for i := 0; i < 2; i++ {
		qc.addToBuffer(&kafka.NOP{
			Partition: 0,
			Topic:     qc.queue.topic,
		})
	}
	return forwardedMessages
}

func (qc *queueConsumer) rebalance() {
	qc.addToBuffer(&kafka.Assignment{
		0: -1,
	})
}

func newConsumer(tester *Tester) *consumer {
	return &consumer{
		tester:           tester,
		events:           make(chan kafka.Event, 0),
		simpleConsumers:  make(map[string]*queueConsumer),
		subscribedTopics: make(map[string]*queueConsumer),
	}
}

// Events returns the event channel of the consumer mock
func (tc *consumer) Events() <-chan kafka.Event {
	return tc.events
}

// Subscribe marks the consumer to subscribe to passed topics.
// The consumerMock simply marks the topics as handled to make sure to
// pass emitted messages back to the processor.
func (tc *consumer) Subscribe(topics map[string]int64) error {
	for topic := range topics {
		if _, exists := tc.subscribedTopics[topic]; exists {
			logger.Printf("consumer for %s already exists. This is strange", topic)
		}
		logger.Printf("Subscribe %s", topic)
		tc.subscribedTopics[topic] = tc.tester.getOrCreateQueue(topic).bindConsumer(tc, true)
		tc.subscribedTopics[topic].rebalance()
		tc.subscribedTopics[topic].startLoop(false)
	}
	return nil
}

// AddGroupPartition adds a partition for group consumption.
// No action required in the mock.
func (tc *consumer) AddGroupPartition(partition int32) {
	for _, consumer := range tc.subscribedTopics {
		logger.Printf("AddGroupPartition %s", consumer.queue.topic)
		consumer.startGroupConsumer()
		consumer.setRunning()
	}
}

// Commit commits an offest.
// No action required in the mock.
func (tc *consumer) Commit(topic string, partition int32, offset int64) error {
	return nil
}

// AddPartition marks the topic as a table topic.
// The mock has to know the group table topic to ignore emit calls (which would never be consumed)
func (tc *consumer) AddPartition(topic string, partition int32, initialOffset int64) error {
	logger.Printf("AddPartition %s", topic)
	var firstStart bool
	if _, exists := tc.simpleConsumers[topic]; !exists {
		firstStart = true
		tc.simpleConsumers[topic] = tc.tester.getOrCreateQueue(topic).bindConsumer(tc, false)
	} else {
		logger.Printf("AddPartition %s: consumer already existed. Will reuse the one", topic)
	}
	if tc.simpleConsumers[topic].isRunning() {
		panic(fmt.Errorf("simple consumer for %s already running. RemovePartition not called or race condition", topic))
	}
	tc.simpleConsumers[topic].startSimpleConsumer(initialOffset, firstStart)

	return nil
}

// RemovePartition removes a partition from a topic.
// No action required in the mock.
func (tc *consumer) RemovePartition(topic string, partition int32) error {
	logger.Printf("consumer RemovePartition %s", topic)
	if cons, exists := tc.simpleConsumers[topic]; exists {
		cons.stop()
	} else {
		logger.Printf("consumer for topic %s did not exist. Cannot Remove partition", topic)
	}
	return nil
}

// Close closes the consumer.
func (tc *consumer) Close() error {
	tc.closeOnce.Do(func() {
		logger.Printf("closing tester consumer. Will close all subscribed topics")
		for _, cons := range tc.subscribedTopics {
			if cons.isRunning() {
				logger.Printf("closing queue consumer for %s", cons.queue.topic)
				cons.kill()
			} else {
				logger.Printf("queue consumer for %s is not running", cons.queue.topic)
			}
		}

		for _, cons := range tc.simpleConsumers {
			if cons.isRunning() {
				logger.Printf("closing simple consumer for %s", cons.queue.topic)
				cons.kill()
			} else {
				logger.Printf("queue consumer for %s is not running", cons.queue.topic)
			}
		}

		close(tc.events)
	})
	return nil
}
