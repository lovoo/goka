package goka

import (
	"fmt"
	"sync"

	"github.com/lovoo/goka/kafka"
)

// Emitter emits messages into a specific Kafka topic, first encoding the message with the given codec.
type Emitter struct {
	codec    Codec
	producer kafka.Producer

	topic string

	wg sync.WaitGroup
}

// NewEmitter creates a new emitter using passed brokers, topic, codec and possibly options.
func NewEmitter(brokers []string, topic Stream, codec Codec, options ...EmitterOption) (*Emitter, error) {
	options = append(
		// default options comes first
		[]EmitterOption{},

		// user-defined options (may overwrite default ones)
		options...,
	)

	opts := new(eoptions)

	err := opts.applyOptions(options...)
	if err != nil {
		return nil, fmt.Errorf(errApplyOptions, err)
	}

	prod, err := opts.builders.producer(brokers, opts.clientID, opts.hasher)
	if err != nil {
		return nil, fmt.Errorf(errBuildProducer, err)
	}

	return &Emitter{
		codec:    codec,
		producer: prod,
		topic:    string(topic),
	}, nil
}

// Emit sends a message for passed key using the emitter's codec.
func (e *Emitter) Emit(key string, msg interface{}) (*kafka.Promise, error) {
	var (
		err  error
		data []byte
	)

	if msg != nil {
		data, err = e.codec.Encode(msg)
		if err != nil {
			return nil, fmt.Errorf("Error encoding value for key %s in topic %s: %v", key, e.topic, err)
		}
	}
	e.wg.Add(1)
	return e.producer.Emit(e.topic, key, data).Then(func(err error) {
		e.wg.Done()
	}), nil
}

// EmitSync sends a message to passed topic and key.
func (e *Emitter) EmitSync(key string, msg interface{}) error {
	var (
		err     error
		promise *kafka.Promise
	)
	promise, err = e.Emit(key, msg)

	if err != nil {
		return err
	}

	done := make(chan struct{})
	promise.Then(func(asyncErr error) {
		err = asyncErr
		close(done)
	})
	<-done
	return err
}

// Finish waits until the emitter is finished producing all pending messages.
func (e *Emitter) Finish() error {
	e.wg.Wait()
	return e.producer.Close()
}
