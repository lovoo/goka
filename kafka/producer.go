package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/logger"
)

// Producer abstracts the kafka producer
type Producer interface {
	// Emit sends a message to topic.
	Emit(topic string, key string, value []byte) *Promise
	Close() error
}

type producer struct {
	log      logger.Logger
	producer sarama.AsyncProducer
	stop     chan bool
	done     chan bool
}

// NewProducer creates new kafka producer for passed brokers.
func NewProducer(brokers []string, config *sarama.Config, log logger.Logger) (Producer, error) {
	aprod, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("Failed to start Sarama producer: %v", err)
	}

	p := producer{
		log:      log,
		producer: aprod,
		stop:     make(chan bool),
		done:     make(chan bool),
	}

	go p.run()

	return &p, nil
}

func (p *producer) Close() error {
	close(p.stop)
	<-p.done
	return p.producer.Close()
}

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

// resolve or reject a promise in the message's metadata on Success or Error
func (p *producer) run() {
	defer close(p.done)
	for {
		select {
		case <-p.stop:
			return

		case err := <-p.producer.Errors():
			promise, is := err.Msg.Metadata.(*Promise)
			if !is {
				p.log.Panicf("invalid metadata type. expected *Promise, got %T", err.Msg.Metadata)
			}
			promise.Finish(err.Err)

		case msg := <-p.producer.Successes():
			promise, is := msg.Metadata.(*Promise)
			if !is {
				p.log.Panicf("invalid metadata type. expected *Promise, got %T", msg.Metadata)
			}
			promise.Finish(nil)
		}
	}
}
