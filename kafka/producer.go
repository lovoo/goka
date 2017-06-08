package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/logger"
	metrics "github.com/rcrowley/go-metrics"
)

// Producer abstracts the kafka producer
type Producer interface {
	// Emit sends a message to topic.
	// TODO (franz): this method should return a promise, instead of getting one.
	// Otherwise a callback is sufficient
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
func NewProducer(brokers []string, registry metrics.Registry, log logger.Logger) (Producer, error) {
	config := CreateDefaultKafkaConfig("whatever", sarama.OffsetOldest, registry)
	aprod, err := sarama.NewAsyncProducer(brokers, &config.Config)
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
	err := p.producer.Close()
	<-p.done
	return err
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
