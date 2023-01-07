package goka

import "fmt"

type gdef[K, V any] interface {
	topicName() string
	keyCodec() GCodec[K]
	valueCodec() GCodec[V]
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

type gstream[K, V any] struct {
	topic       string
	_keyCodec   GCodec[K]
	_valueCodec GCodec[V]
}

func (g *gstream[K, V]) topicName() string {
	return g.topic
}

func (g *gstream[K, V]) keyCodec() GCodec[K] {
	return g._keyCodec
}

func (g *gstream[K, V]) valueCodec() GCodec[V] {
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

func DefineInput[K, V any](topic Stream, keyCodec GCodec[K], valueCodec GCodec[V]) GInput[K, V] {
	return &ginput[K, V]{
		&gstream[K, V]{
			topic:       string(topic),
			_keyCodec:   keyCodec,
			_valueCodec: valueCodec,
		},
	}
}

func DefineOutput[K, V any](topic Stream, keyCodec GCodec[K], valueCodec GCodec[V]) GOutput[K, V] {
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

func DefineJoin[K, V any](topic Table, keyCodec GCodec[K], valueCodec GCodec[V]) GJoin[K, V] {
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

func DefineLookup[K, V any](topic Table, keyCodec GCodec[K], valueCodec GCodec[V]) GLookup[K, V] {
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

// user defined
type (
	PhotoUploaded struct{}
	PhotoHashed   struct{}
	BigProfile    struct{}
	SmallProfile  struct{}
)

type PhotoUploadedCodec struct{}

func (p *PhotoUploadedCodec) Encode(value *PhotoUploaded) (data []byte, err error) {
	return nil, nil
}

func (p *PhotoUploadedCodec) Decode(data []byte) (value *PhotoUploaded, err error) {
	return nil, nil
}

type GValueMapper[S, T any] interface {
	MapValue(val S) T
}

type mapper[S, T any] struct {
	_mapper   func(val S) T
	_inCodec  GCodec[S]
	_outCodec GCodec[T]
}

func (m *mapper[S, T]) MapValue(val S) T {
	return m._mapper(val)
}

func NewGValueMapper[S, T any](inCodec GCodec[S], outCodec GCodec[T], mapFunc func(val S) T) GValueMapper[S, T] {
	return &mapper[S, T]{
		_mapper: mapFunc,
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

// current questions:
// how to initialize the topo fields with all codecs, without specifying everything else.

// step 1: define it
// go:generate goka-topology -type topology
type topology struct {
	PhotoUploaded GInput[string, *PhotoUploaded]
	PhotoHashed2  GInput[string, *PhotoHashed]
	PhotoHashed   GOutput[string, *PhotoHashed]
	Profile       GLookup[string, *BigProfile]
	ProfileJoin   GJoin[string, *BigProfile]
	SmallProfile  GMappedLookup[string, *BigProfile, *SmallProfile]
	State         GState[string, *PhotoHashed]
}

// for the issue:
// Why not the declarative DSL like kafka streams?
// Things like conditional lookups/emits become very hard to write
// one would have to define branch/if/else constructs to do so simple things like
// calling the context.
// Sometimes you want to lookup from different tables depending on each others values.
// Expressing this becomes very complicated and we'd have to create factory functions
// for different number of types like Join2[A, B] and Join3[A, B, C] etc. which feels unintuitive.
