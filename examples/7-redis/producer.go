package main

import "github.com/lovoo/goka"

// Producer defines an interface whose events are produced on kafka.
type Producer interface {
	Emit(key string, event *Event) error
	Close() error
}

type kafkaProducer struct {
	emitter *goka.Emitter
}

// NewProducer returns a new kafka producer.
func NewProducer(brokers []string, stream string) (Producer, error) {
	codec := new(Codec)
	emitter, err := goka.NewEmitter(brokers, goka.Stream(stream), codec)
	if err != nil {
		return nil, err
	}
	return &kafkaProducer{emitter}, nil
}

func (p *kafkaProducer) Emit(key string, event *Event) error {
	return p.emitter.EmitSync(key, event)
}

func (p *kafkaProducer) Close() error {
	p.emitter.Finish()
	return nil
}
