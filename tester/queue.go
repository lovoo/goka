package tester

import (
	"sync"
)

type message struct {
	offset int64
	key    string
	value  []byte
}

type queue struct {
	topic            string
	messages         []*message
	hwm              int64
	waitConsumerInit sync.WaitGroup
	consumerOffsets  map[*queueConsumer]int64
}

func newQueue(topic string) *queue {
	return &queue{
		topic:           topic,
		consumerOffsets: make(map[*queueConsumer]int64),
	}
}

func (q *queue) expectConsumer() {
	q.waitConsumerInit.Add(1)
}
func (q *queue) addConsumer(cons *queueConsumer) {
	q.consumerOffsets[cons] = 0
	q.waitConsumerInit.Done()
}

func (q *queue) messagesFromOffset(offset int64) []*message {
	return q.messages[offset:]
}

func (q *queue) waitForConsumers() {
	// wait until all consumers are ready to consume (only for startup)
	q.waitConsumerInit.Wait()

	// wait until all consumers for the queue have processed all the messages
	for sub := range q.consumerOffsets {
		sub.catchupAndSync()
	}
}

func (q *queue) push(key string, value []byte) {
	q.messages = append(q.messages, &message{
		offset: q.hwm,
		key:    key,
		value:  value,
	})
	q.hwm++
}
