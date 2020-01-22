package goka

import (
	"testing"
)

type Message struct {
	Key   string
	Value []byte
}

type MockProducer struct {
	messages map[string][]*Message
}

func (p *MockProducer) Emit(topic string, key string, value []byte) *Promise {
	p.messages[topic] = append(p.messages[topic], &Message{
		Key:   key,
		Value: value,
	})
	return NewPromise().Finish(nil)
}

func (p *MockProducer) Clear() {
	p.messages = make(map[string][]*Message)
}

func (p *MockProducer) MessagesForTopic(topic string) []*Message {
	return p.messages[topic]
}

func (p *MockProducer) Close() error {
	return nil
}

func NewMockProducer(t *testing.T) *MockProducer {
	return &MockProducer{
		messages: make(map[string][]*Message),
	}
}
