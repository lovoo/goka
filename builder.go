package goka

import (
	"fmt"
	"log"
)

type GCodec[T any] interface {
	Encode(value T) (data []byte, err error)
	Decode(data []byte) (value T, err error)
}

type GFuncCodec[T any] struct {
	decoder func([]byte) (T, error)
	encoder func(T) ([]byte, error)
}

func (gfc *GFuncCodec[T]) Encode(value T) (data []byte, err error) {
	return gfc.encoder(value)
}

func (gfc *GFuncCodec[T]) Decode(data []byte) (value T, err error) {
	return gfc.decoder(data)
}

type CodecBridge[T any] struct {
	c GCodec[T]
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

func NewGFuncCodec[T any](decoder func([]byte) (T, error),
	encoder func(T) ([]byte, error),
) *GFuncCodec[T] {
	return &GFuncCodec[T]{
		decoder: decoder,
		encoder: encoder,
	}
}

var GStringCodec = NewGFuncCodec(
	func(b []byte) (string, error) { return string(b), nil },
	func(s string) ([]byte, error) { return []byte(s), nil },
)

type GIntCodec struct{}

func (s *GIntCodec) Encode(value int64) (data []byte, err error) {
	_ = value
	return nil, nil
}

func (s *GIntCodec) Decode(data []byte) (value int64, err error) {
	return 1234, nil
}

type GStream[K any, V any] struct {
	stream  Stream
	builder *GBuilder

	codec GCodec[V]
}

type GBuilder struct {
	handlers map[string]func(ctx Context, msg interface{})
}

func NewBuilder() *GBuilder {
	return &GBuilder{}
}

func (gb *GBuilder) Build(group Group) *GroupGraph {
	return DefineGroup(group)
}

func NewGInput[K, V any](builder *GBuilder, stream Stream, codec GCodec[V]) *GStream[K, V] {
	return &GStream[K, V]{
		codec:   codec,
		stream:  stream,
		builder: builder,
	}
}

type GTable[K, V any] struct {
	table   Table
	builder *GBuilder

	codec GCodec[V]
}

func NewGtable[K, V any](builder *GBuilder, table Table, codec GCodec[V]) *GTable[K, V] {
	return &GTable[K, V]{
		codec:   codec,
		table:   table,
		builder: builder,
	}
}

func NewTableMap[K any, In any, Out any](table *GTable[K, In]) *GTable[K, Out] {
}

func NewJoin[InK, InV any, J any, OutK, OutV any](builder *GBuilder, input *GStream[InK, InV], join *GTable[InK, J], handler func(in InV, joined J) (OutV, error)) *GStream[OutK, OutV] {
	return &GStream[OutK, OutV]{}
}

func NewMap[InK, InV any, OutK, OutV any](builder *GBuilder, input *GStream[InK, InV], handler func(in InV) (OutV, error)) *GStream[OutK, OutV] {
	return &GStream[OutK, OutV]{
		// joinTable: join,
		// handler:   handler,
	}
}

func (m *GStream[K, V]) Persist(table *GTable[K, V]) {
}

func (m *GStream[K, V]) Write(stream *GStream[K, V]) {
}

func (m *GStream[K, V]) Filter(f func(value V) (bool, error)) *GStream[K, V] {
	return m
}

func (m *GStream[K, V]) Shuffle(f func(key K, value V) (K, error)) *GStream[K, V] {
	return m
}

type GView[K, V any] struct {
	view       *View
	KeyCodec   GCodec[K]
	ValueCodec GCodec[V]
}

func (v *GView[K, V]) Get(key K) (V, error) {
	var def V
	encKey, err := v.KeyCodec.Encode(key)
	if err != nil {
		return def, fmt.Errorf("error encoding key: %w", err)
	}

	val, err := v.view.GetBytes(encKey)
	if err != nil {
		return def, err
	}

	valObj, ok := val.(V)
	if !ok {
		return def, fmt.Errorf("unexpected value type")
	}
	return valObj, nil
}

func NewGView[K, V any](gb *GBuilder, table *GTable[K, V]) *GView[K, V] {
	view, err := NewView(nil, table.table, NewCodecBridge(table.codec))
	_ = err
	return &GView[K, V]{
		view: view,
	}
}

func Foo() {
	graph := DefineGroup("asdf",
		Input("test", nil, func(ctx Context, msg interface{}) {
			val := ctx.Value()

			ctx.SetValue(val)
		}),
		Persist(nil),
	)

	_ = graph

	gb := NewBuilder()

	tab := NewGtable[string, int64](gb, "tab", new(GIntCodec))

	input := NewGInput[string, string](gb, "input-stream", GStringCodec)
	output := NewGInput[string, int64](gb, "output-stream", new(GIntCodec))

	NewJoin[string, string, int64, string](gb, input, tab, func(input string, state int64) (int64, error) {
		return state + 1, nil
	}).Filter(func(value int64) (bool, error) { return value%2 == 0, nil }).Persist(tab)

	NewMap[string, string, string](gb, input, func(input string) (int64, error) {
		return 1234, nil
	}).Write(output)

	v := NewGView(gb, tab)

	val, _ := v.Get("asdf")

	log.Printf("value is %d", val)
}
