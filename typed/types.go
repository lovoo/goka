package typed

import (
	"fmt"

	"github.com/lovoo/goka"
)

type gdef[K, V any] interface {
	TopicName() string
	KeyCodec() GCodec[K]
	ValueCodec() GCodec[V]
}

type (
	GInput[K, V any] interface {
		gdef[K, V]
		is_input()
	}
	GOutput[K, V any] interface {
		gdef[K, V]
		is_output()
	}
	GLoop[K, V any]   interface{}
	GLookup[K, V any] interface {
		gdef[K, V]
		is_lookup()
	}
	GMappedLookup[K, V, T any] interface {
		gdef[K, T]
		is_mappedlookup()
	}
	GJoin[K, V any] interface {
		gdef[K, V]
		is_join()
	}
	GState[K, V any] interface {
		gdef[K, V]
		is_state()
	}
)

type (
	topicOptions struct {
		autoCreate int
	}
	GOption func(*topicOptions)
)

func AutoCreate(partitions int) GOption {
	return func(opts *topicOptions) {
		opts.autoCreate = partitions
	}
}

type gstream[K, V any] struct {
	topic       string
	_keyCodec   GCodec[K]
	_valueCodec GCodec[V]
}

func (g *gstream[K, V]) TopicName() string {
	return g.topic
}

func (g *gstream[K, V]) KeyCodec() GCodec[K] {
	return g._keyCodec
}

func (g *gstream[K, V]) ValueCodec() GCodec[V] {
	return g._valueCodec
}

type ginput[K, V any] struct {
	*gstream[K, V]
}

func (g *ginput[K, V]) is_input() {}

type goutput[K, V any] struct {
	*gstream[K, V]
}

func (g *goutput[K, V]) is_output() {}

func DefineInput[K, V any](topic goka.Stream, keyCodec GCodec[K], valueCodec GCodec[V]) GInput[K, V] {
	return &ginput[K, V]{
		&gstream[K, V]{
			topic:       string(topic),
			_keyCodec:   keyCodec,
			_valueCodec: valueCodec,
		},
	}
}

func DefineOutput[K, V any](topic goka.Stream, keyCodec GCodec[K], valueCodec GCodec[V]) GOutput[K, V] {
	return &goutput[K, V]{
		&gstream[K, V]{
			topic:       string(topic),
			_keyCodec:   keyCodec,
			_valueCodec: valueCodec,
		},
	}
}

type gjoin[K, V any] struct {
	*gstream[K, V]
}

func (g *gjoin[K, V]) is_join() {}

func DefineJoin[K, V any](topic goka.Table, keyCodec GCodec[K], valueCodec GCodec[V]) GJoin[K, V] {
	return &gjoin[K, V]{
		&gstream[K, V]{
			topic:       string(topic),
			_keyCodec:   keyCodec,
			_valueCodec: valueCodec,
		},
	}
}

type glookup[K, V any] struct {
	*gstream[K, V]
}

func (g *glookup[K, V]) is_lookup() {}

func DefineLookup[K, V any](topic goka.Table, keyCodec GCodec[K], valueCodec GCodec[V]) GLookup[K, V] {
	return &glookup[K, V]{
		&gstream[K, V]{
			topic:       string(topic),
			_keyCodec:   keyCodec,
			_valueCodec: valueCodec,
		},
	}
}

type gstate[K, V any] struct {
	*gstream[K, V]
}

func (g *gstate[K, V]) is_state() {}

func DefineState[K, V any](keyCodec GCodec[K], valueCodec GCodec[V]) GState[K, V] {
	return &gstate[K, V]{
		&gstream[K, V]{
			_keyCodec:   keyCodec,
			_valueCodec: valueCodec,
		},
	}
}

type CodecBridge[T any] struct {
	c GCodec[T]
}

type GCodec[V any] interface {
	Encode(value V) (data []byte, err error)
	Decode(data []byte) (value V, err error)
}

func (cb *CodecBridge[T]) Encode(value interface{}) ([]byte, error) {
	tVal, ok := value.(T)
	if !ok {
		return nil, fmt.Errorf("unexpected while encoding")
	}
	return cb.c.Encode(tVal)
}

func (cb *CodecBridge[T]) Decode(data []byte) (interface{}, error) {
	return cb.c.Decode(data)
}

func NewCodecBridge[T any](codec GCodec[T]) *CodecBridge[T] {
	return &CodecBridge[T]{
		c: codec,
	}
}

type StringCodec[T ~string] struct{}

func (s *StringCodec[T]) Encode(value T) (data []byte, err error) {
	return []byte(string(value)), nil
}

func (s *StringCodec[T]) Decode(data []byte) (T, error) {
	return T(string(data)), nil
}
