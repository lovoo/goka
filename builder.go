package goka

import (
	"fmt"
)

type GCodec[K, V any] interface {
	Encode(value V) (data []byte, err error)
	Decode(data []byte) (value V, err error)
	EncodeKey(key K) (data []byte, err error)
	DecodeKey(data []byte) (key K, err error)
}
type GCodec2[V any] interface {
	Encode(value V) (data []byte, err error)
	Decode(data []byte) (value V, err error)
}

type GFuncCodec[K, V any] struct {
	decoder    func([]byte) (V, error)
	encoder    func(V) ([]byte, error)
	decoderkey func([]byte) (K, error)
	encoderkey func(K) ([]byte, error)
}

func (gfc *GFuncCodec[K, V]) Encode(value V) (data []byte, err error) {
	return gfc.encoder(value)
}

func (gfc *GFuncCodec[K, V]) Decode(data []byte) (value V, err error) {
	return gfc.decoder(data)
}

func (gfc *GFuncCodec[K, V]) EncodeKey(key K) (data []byte, err error) {
	return gfc.encoderkey(key)
}

func (gfc *GFuncCodec[K, V]) DecodeKey(data []byte) (key K, err error) {
	return gfc.decoderkey(data)
}

type CodecBridge[T any] struct {
	c GCodec2[T]
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

func NewCodecBridge[T any](codec GCodec2[T]) *CodecBridge[T] {
	return &CodecBridge[T]{
		c: codec,
	}
}

func NewGFuncCodec[T any](decoder func([]byte) (T, error),
	encoder func(T) ([]byte, error),
) GCodec[string, T] {
	return &GFuncCodec[string, T]{
		decoder:    decoder,
		encoder:    encoder,
		decoderkey: func(b []byte) (string, error) { return string(b), nil },
		encoderkey: func(s string) ([]byte, error) { return []byte(s), nil },
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

	codec GCodec[K, V]
}

type GBuilder struct {
	handlers map[string]func(ctx Context, msg interface{})
	group    string
}

func NewBuilder(group string) *GBuilder {
	return &GBuilder{
		group: group,
	}
}

func (gb *GBuilder) Build(group Group) *GroupGraph {
	return DefineGroup(group)
}

func NewGInput[K, V any](builder *GBuilder, stream Stream, codec GCodec[K, V]) *GStream[K, V] {
	return &GStream[K, V]{
		codec:   codec,
		stream:  stream,
		builder: builder,
	}
}

type GTable[K, V any] struct {
	table   Table
	builder *GBuilder

	codec GCodec[K, V]
}

func NewGtable[K, V any](builder *GBuilder, table Table, codec GCodec[K, V]) *GTable[K, V] {
	return &GTable[K, V]{
		codec:   codec,
		table:   table,
		builder: builder,
	}
}

func NewTableMap[K any, In any, Out any](table *GTable[K, In]) *GTable[K, Out] {
	return nil
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
	view  *View
	Codec GCodec[K, V]
}

func (v *GView[K, V]) Get(key K) (V, error) {
	var def V
	encKey, err := v.Codec.EncodeKey(key)
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

type GHandler struct{}

func NewGHandler(builder *GBuilder) *GHandler {
	return &GHandler{}
}

// func Foo() {
// 	graph := DefineGroup("asdf",
// 		Input("test", nil, func(ctx Context, msg interface{}) {
// 			val := ctx.Value()

// 			ctx.SetValue(val)
// 		}),
// 		Persist(nil),
// 	)

// 	_ = graph

// 	gb := NewBuilder("test")

// 	tab := NewGtable[string, int64](gb, "tab", new(GIntCodec))

// 	input := NewGInput[string, string](gb, "input-stream", GStringCodec)
// 	output := NewGInput[string, int64](gb, "output-stream", new(GIntCodec))

// 	NewJoin[string, string, int64, string](gb, input, tab, func(input string, state int64) (int64, error) {
// 		return state + 1, nil
// 	}).Filter(func(value int64) (bool, error) { return value%2 == 0, nil }).Persist(tab)

// 	NewMap[string, string, string](gb, input, func(input string) (int64, error) {
// 		return 1234, nil
// 	}).Write(output)

// 	v := NewGView(gb, tab)

// 	val, _ := v.Get("asdf")

// 	log.Printf("value is %d", val)
// }
