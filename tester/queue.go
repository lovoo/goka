package tester

import (
	"fmt"
	"log"
	"sync"
)

type message struct {
	offset int64
	key    string
	value  []byte
}

type queue struct {
	sync.Mutex
	topic            string
	messages         []*message
	hwm              int64
	waitConsumerInit sync.WaitGroup
	simpleConsumers  map[*queueConsumer]int64
	groupConsumers   map[*queueConsumer]int64
}

func newQueue(topic string) *queue {

	return &queue{
		topic:           topic,
		simpleConsumers: make(map[*queueConsumer]int64),
		groupConsumers:  make(map[*queueConsumer]int64),
	}
}

func (q *queue) size() int {
	return len(q.messages)
}

func (q *queue) message(offset int) *message {
	return q.messages[offset]
}

func (q *queue) messagesFrom(from int) []*message {
	return q.messages[from:]
}

func (q *queue) expectGroupConsumer() {
	q.Lock()
	defer q.Unlock()
	q.groupConsumers[newQueueConsumer(q.topic, q)] = 0
}

func (q *queue) expectSimpleConsumer() {
	q.Lock()
	defer q.Unlock()
	q.simpleConsumers[newQueueConsumer(q.topic, q)] = 0
}

func (q *queue) bindConsumer(cons *consumer, groupConsumer bool) *queueConsumer {
	q.Lock()
	defer q.Unlock()

	consumers := q.simpleConsumers
	if groupConsumer {
		consumers = q.groupConsumers
	}
	for qCons := range consumers {
		if !qCons.isBound() {
			qCons.bindToConsumer(cons)
			return qCons
		}
	}
	panic(fmt.Errorf("did not find an unbound consumer for %s. The group graph was not parsed correctly", q.topic))
}

func (q *queue) messagesFromOffset(offset int64) []*message {
	q.Lock()
	defer q.Unlock()
	return q.messages[offset:]
}

// wait until all consumers are ready to consume (only for startup)
func (q *queue) waitConsumersInit() {
	logger.Printf("Consumers in Queue %s", q.topic)
	for cons := range q.groupConsumers {
		logger.Printf("waiting for group consumer %s to be running", cons.queue.topic)
		select {
		case <-cons.state.WaitForState(killed):
			log.Printf("At least one consumer was killed. No point in waiting for it")
			return
		case <-cons.state.WaitForState(running):
			logger.Printf(" --> %s is running", cons.queue.topic)
		}
	}

	for cons := range q.simpleConsumers {
		logger.Printf("waiting for simple consumer %s to be ready", cons.queue.topic)
		select {
		case <-cons.state.WaitForState(running):
		case <-cons.state.WaitForState(stopped):
		case <-cons.state.WaitForState(killed):
		}
		logger.Printf(" --> %s is ready", cons.queue.topic)
	}
}

func (q *queue) waitForConsumers() int {
	// wait until all consumers for the queue have processed all the messages
	var numMessagesConsumed int
	for sub := range q.simpleConsumers {
		logger.Printf("waiting for simple consumer %s to finish up", q.topic)
		numMessagesConsumed += sub.catchupAndSync()
		logger.Printf(">> done waiting for simple consumer %s to finish up", q.topic)
	}
	for sub := range q.groupConsumers {
		logger.Printf("waiting for simple consumer %s to finish up", q.topic)
		numMessagesConsumed += sub.catchupAndSync()
		logger.Printf(">> done waiting for simple consumer %s to finish up", q.topic)
	}
	return numMessagesConsumed
}

func (q *queue) push(key string, value []byte) {
	q.Lock()
	defer q.Unlock()
	q.messages = append(q.messages, &message{
		offset: q.hwm,
		key:    key,
		value:  value,
	})
	q.hwm++
}
