package mock

import (
	"testing"

	"github.com/lovoo/goka"
)

type Message struct {
	Key   string
	Value []byte
}

// Producer mimicks a real producer
type Producer struct {
	messages map[string][]*Message
}

func (p *Producer) Emit(topic string, key string, value []byte) *goka.Promise {
	p.messages[topic] = append(p.messages[topic], &Message{
		Key:   key,
		Value: value,
	})
	return goka.NewPromise().Finish(nil)
}

func (p *Producer) Clear() {
	p.messages = make(map[string][]*Message)
}

func (p *Producer) MessagesForTopic(topic string) []*Message {
	return p.messages[topic]
}

func (p *Producer) Close() error {
	return nil
}

func NewProducer(t *testing.T) *Producer {
	return &Producer{
		messages: make(map[string][]*Message),
	}
}
