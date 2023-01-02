package goka

import (
	"github.com/lovoo/goka/codec"
)

type gdef[K, V any] interface {
	topicName() string
	keyCodec() GCodec2[K]
	valueCodec() GCodec2[V]
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
	GLoop[K, V any]            interface{}
	GLookup[K, V any]          interface{}
	GMappedLookup[K, V, T any] interface{}
	GJoin[K, V any]            interface{}
	GState[K, V any]           interface {
		keyCodec() GCodec2[K]
		valueCodec() GCodec2[V]
	}
)

type gstream[K, V any] struct {
	topic       string
	_keyCodec   GCodec2[K]
	_valueCodec GCodec2[V]
	autocreate  bool // set via options
}

func (g *gstream[K, V]) topicName() string {
	return g.topic
}

func (g *gstream[K, V]) keyCodec() GCodec2[K] {
	return g._keyCodec
}

func (g *gstream[K, V]) valueCodec() GCodec2[V] {
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

func DefineInput[K, V any](topic Stream, keyCodec GCodec2[K], valueCodec GCodec2[V]) GInput[K, V] {
	return &ginput[K, V]{
		&gstream[K, V]{
			topic:       string(topic),
			_keyCodec:   keyCodec,
			_valueCodec: valueCodec,
		},
	}
}

func DefineOutput[K, V any](topic Stream, keyCodec GCodec2[K], valueCodec GCodec2[V]) GOutput[K, V] {
	return &goutput[K, V]{
		&gstream[K, V]{
			topic:       string(topic),
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
	_inCodec  GCodec2[S]
	_outCodec GCodec2[T]
}

func (m *mapper[S, T]) MapValue(val S) T {
	return m._mapper(val)
}

func NewGValueMapper[S, T any](inCodec GCodec2[S], outCodec GCodec2[T], mapFunc func(val S) T) GValueMapper[S, T] {
	return &mapper[S, T]{
		_mapper: mapFunc,
	}
}

// we don't do this! We'll go the road of "topology"
type topology2 struct {
	PhotoUploaded GStream[string, *PhotoUploaded]                          `goka:"type=input,autocreate=20,name=photo_uploaded"`
	OutStream     GStream[string, *PhotoUploaded]                          `goka:"type=output,autocreate=20,name=output_stream"`
	PhotoHash     GTable[string, *PhotoHashed]                             `goka:"type=lookup,name=photo_hash_table,source_group="photo_hash"`
	PhotoHash2    GTable[string, *PhotoHashed]                             `goka:"type=join,name=photo_hash_table,source_group="photo_hash"`
	PhotoHash3    GTable[string, *PhotoHashed]                             `goka:"type=state,name=photo_hash_table,source_group="photo_hash"`
	PhotoHash4    GTable[string, *PhotoHashed]                             `goka:"type=temp"`
	PhotoHash5    GTable[string, GValueMapper[*BigProfile, *SmallProfile]] `goka:"type=lookup"`
}

// current questions:
// how to initialize the topo fields with all codecs, without specifying everything else.
// how to allow to both

// step 1: define it
// go:generate goka-topology -type topology
type topology struct {
	PhotoUploaded GInput[string, *PhotoUploaded]
	PhotoHashed   GOutput[string, *PhotoHashed]
	Profile       GLookup[string, *BigProfile]
	SmallProfile  GMappedLookup[string, *BigProfile, *SmallProfile]
	State         GState[string, *PhotoHashed]
}

// step 2: this generates:
type topologyGroupBuilder struct {
	unexported_PhotoUploaded GInput[string, *PhotoUploaded]
	unexported_PhotoHashed   GOutput[string, *PhotoHashed]
}

func NewTopologyGroupBuilder(
	photoUploaded GInput[string, *PhotoUploaded],
	photoHashed GOutput[string, *PhotoHashed],
) *topologyGroupBuilder {
	return &topologyGroupBuilder{
		unexported_PhotoUploaded: photoUploaded,
		unexported_PhotoHashed:   photoHashed,
	}
}

// for the issue:
// Why not the declarative DSL like kafka streams?
// Things like conditional lookups/emits become very hard to write
// one would have to define branch/if/else constructs to do so simple things like
// calling the context.
// Sometimes you want to lookup from different tables depending on each others values.
// Expressing this becomes very complicated and we'd have to create factory functions
// for different number of types like Join2[A, B] and Join3[A, B, C] etc. which feels unintuitive.

func (gb *topologyGroupBuilder) ProcessPhotoUploaded(handler func(ctx TopologyCtx, message string)) Edge {
	return Input("input", new(codec.String), func(ctx Context, msg interface{}) {
		handler(&topologyCtx{
			ctx: ctx,
		}, msg.(string))
	})
}

type TopologyCtx interface {
	EmitPhotoHashed(key string, value *PhotoHashed)
}

type topologyCtx struct {
	ctx Context
}

func (c *topologyCtx) EmitPhotoHashed(key string, value *PhotoHashed) {}

func newTopologyGroupBuilder(group string,
	input GCodec[string],
	output GCodec[string],
) *topologyGroupBuilder {
	return nil
}

// that's how you use it
func foo() {
	builder := newTopologyGroupBuilder("test", nil, nil) // pass string-codecs somehow

	builder.HandleInput(nil)

	builder.build()
}
