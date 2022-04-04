package goka

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// Producer abstracts the kafka producer
type Producer interface {
	// Emit sends a message to topic.
	Emit(topic string, key string, value []byte) *Promise
	EmitWithHeaders(topic string, key string, value []byte, headers Headers) *Promise
	Close() error
}

type producer struct {
	producer sarama.AsyncProducer
	wg       sync.WaitGroup
}

// NewProducer creates new kafka producer for passed brokers.
func NewProducer(brokers []string, config *sarama.Config) (Producer, error) {
	aprod, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("Failed to start Sarama producer: %v", err)
	}

	p := producer{
		producer: aprod,
	}

	p.run()

	return &p, nil
}

// Close stops the producer and waits for the Success/Error channels to drain.
// Emitting to a closing/closed producer results in write-to-closed-channel panic
func (p *producer) Close() error {
	// do an async close to get the rest of the success/error messages to avoid
	// leaving unfinished promises.
	p.producer.AsyncClose()

	// wait for the channels to drain
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.NewTimer(60 * time.Second).C:
	}

	return nil
}

// Emit emits a key-value pair to topic and returns a Promise that
// can be checked for errors asynchronously
func (p *producer) Emit(topic string, key string, value []byte) *Promise {
	promise := NewPromise()

	p.producer.Input() <- &sarama.ProducerMessage{
		Topic:    topic,
		Key:      sarama.StringEncoder(key),
		Value:    sarama.ByteEncoder(value),
		Metadata: promise,
	}
	return promise
}

// EmitWithHeaders emits a key-value pair with headers to topic and returns a Promise that
// can be checked for errors asynchronously
func (p *producer) EmitWithHeaders(topic string, key string, value []byte, headers Headers) *Promise {
	promise := NewPromise()

	p.producer.Input() <- &sarama.ProducerMessage{
		Topic:    topic,
		Key:      sarama.StringEncoder(key),
		Value:    sarama.ByteEncoder(value),
		Metadata: promise,
		Headers:  headers.ToSarama(),
	}
	return promise
}

// resolve or reject a promise in the message's metadata on Success or Error
func (p *producer) run() {
	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		for {
			err, ok := <-p.producer.Errors()

			// channel closed, the producer is stopping
			if !ok {
				return
			}
			err.Msg.Metadata.(*Promise).finish(nil, err)
		}
	}()

	go func() {
		defer p.wg.Done()
		for {
			msg, ok := <-p.producer.Successes()
			// channel closed, the producer is stopping
			if !ok {
				return
			}
			msg.Metadata.(*Promise).finish(msg, nil)
		}
	}()
}
