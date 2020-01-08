package mock

import (
	"testing"

	kafka "github.com/lovoo/goka/kafka"
)

// Producer mimicks a real producer
type Producer struct {
}

func (p *Producer) Emit(topic string, key string, value []byte) *kafka.Promise {
	return kafka.NewPromise().Finish(nil)
}
func (p *Producer) Close() error {
	return nil
}

func NewProducer(t *testing.T) *Producer {
	return &Producer{}
}
