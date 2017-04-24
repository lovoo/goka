package goka

import (
	"fmt"
	"sync"

	"github.com/lovoo/goka/kafka"
)

type Producer struct {
	codec    Codec
	producer kafka.Producer

	topic string

	wg sync.WaitGroup
}

// NewProducer creates a new producer using passed brokers, topic, codec and possibly options
func NewProducer(brokers []string, topic string, codec Codec, options ...ProducerOption) (*Producer, error) {
	options = append(
		// default options comes first
		[]ProducerOption{},

		// user-defined options (may overwrite default ones)
		options...,
	)

	opts := new(proptions)

	err := opts.applyOptions(options...)
	if err != nil {
		return nil, fmt.Errorf(errApplyOptions, err)
	}

	prod, err := opts.builders.producer(brokers, opts.kafkaRegistry)
	if err != nil {
		return nil, fmt.Errorf(errBuildProducer, err)
	}

	return &Producer{
		codec:    codec,
		producer: prod,
		topic:    topic,
	}, nil
}

// Produce sends a message for passed key using the producer's codec.
func (p *Producer) Produce(key string, msg interface{}) (*kafka.Promise, error) {
	var (
		err  error
		data []byte
	)

	if msg != nil {
		data, err = p.codec.Encode(msg)
		if err != nil {
			return nil, fmt.Errorf("Error encoding value for key %s in topic %s: %v", key, p.topic, err)
		}
	}
	p.wg.Add(1)
	return p.producer.Emit(p.topic, key, data).Then(func(err error) {
		p.wg.Done()
	}), nil
}

// ProduceSync sends a message to passed topic and key
func (p *Producer) ProduceSync(key string, msg interface{}) error {
	promise, err := p.Produce(key, msg)

	if err != nil {
		return err
	}

	done := make(chan struct{})
	promise.Then(func(err error) {
		close(done)
	})
	<-done
	return nil
}

// Finish waits until the producer is finished producing all pending messages
func (p *Producer) Finish() {
	p.wg.Wait()
}
