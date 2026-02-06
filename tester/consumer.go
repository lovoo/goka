package tester

import (
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type consumerMock struct {
	sync.RWMutex
	tester         *Tester
	requiredTopics map[string]bool
	partConsumers  map[string]*partConsumerMock
}

func newConsumerMock(tt *Tester) *consumerMock {
	return &consumerMock{
		tester:         tt,
		requiredTopics: make(map[string]bool),
		partConsumers:  make(map[string]*partConsumerMock),
	}
}

func (cm *consumerMock) catchup() int {
	cm.RLock()
	defer cm.RUnlock()
	var catchup int
	for _, pc := range cm.partConsumers {
		catchup += pc.catchup()
	}
	return catchup
}

func (cm *consumerMock) Topics() ([]string, error) {
	cm.tester.mCodecs.RLock()
	defer cm.tester.mCodecs.RUnlock()

	var topics []string

	for topic := range cm.tester.codecs {
		topics = append(topics, topic)
	}
	return topics, nil
}

func (cm *consumerMock) Partitions(topic string) ([]int32, error) {
	return []int32{0}, nil
}

func (cm *consumerMock) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	cm.Lock()
	defer cm.Unlock()
	if _, exists := cm.partConsumers[topic]; exists {
		return nil, fmt.Errorf("Got duplicate consume partition for topic %s", topic)
	}
	cons := &partConsumerMock{
		hwm:      offset,
		queue:    cm.tester.getOrCreateQueue(topic),
		messages: make(chan *sarama.ConsumerMessage),
		errors:   make(chan *sarama.ConsumerError),
		closer: func() error {
			cm.Lock()
			defer cm.Unlock()
			if _, exists := cm.partConsumers[topic]; !exists {
				return fmt.Errorf("partition consumer seems already closed")
			}
			delete(cm.partConsumers, topic)
			return nil
		},
	}

	cm.partConsumers[topic] = cons

	return cons, nil
}
func (cm *consumerMock) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}
func (cm *consumerMock) Close() error {
	return nil
}

func (cm *consumerMock) Pause(topicPartitions map[string][]int32) {}

func (cm *consumerMock) Resume(topicPartitions map[string][]int32) {}

func (cm *consumerMock) PauseAll() {}

func (cm *consumerMock) ResumeAll() {}

func (cm *consumerMock) waitRequiredConsumersStartup() {
	doCheck := func() bool {
		cm.RLock()
		defer cm.RUnlock()

		for topic := range cm.requiredTopics {
			_, ok := cm.partConsumers[topic]
			if !ok {
				return false
			}
		}
		return true
	}
	for !doCheck() {
		time.Sleep(50 * time.Millisecond)
	}
}

func (cm *consumerMock) requirePartConsumer(topic string) {
	cm.requiredTopics[topic] = true
}

type partConsumerMock struct {
	hwm      int64
	closer   func() error
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
	queue    *queue
}

func (pcm *partConsumerMock) catchup() int {
	var numCatchup int
	for _, msg := range pcm.queue.messagesFromOffset(pcm.hwm) {
		pcm.messages <- &sarama.ConsumerMessage{
			Headers:   msg.headers.ToSaramaPtr(),
			Key:       []byte(msg.key),
			Value:     msg.value,
			Topic:     pcm.queue.topic,
			Partition: 0,
			Offset:    msg.offset,
			Timestamp: msg.time,
		}

		// we'll send a nil that is being ignored by the partition_table to make sure the other message
		// really went through the channel
		pcm.messages <- nil
		numCatchup++
		pcm.hwm = msg.offset + 1
	}

	return numCatchup
}

func (pcm *partConsumerMock) Close() error {
	close(pcm.messages)
	close(pcm.errors)
	return pcm.closer()
}

func (pcm *partConsumerMock) AsyncClose() {
	go pcm.Close()
}

func (pcm *partConsumerMock) Messages() <-chan *sarama.ConsumerMessage {
	return pcm.messages
}

func (pcm *partConsumerMock) Errors() <-chan *sarama.ConsumerError {
	return pcm.errors
}

func (pcm *partConsumerMock) HighWaterMarkOffset() int64 {
	return pcm.queue.Hwm()
}

func (pcm *partConsumerMock) Pause() {}

func (pcm *partConsumerMock) Resume() {}

func (pcm *partConsumerMock) IsPaused() bool {
	return false
}
