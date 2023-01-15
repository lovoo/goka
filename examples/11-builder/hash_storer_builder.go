package main

// AUTO GENERATED DO NOT MODIFY!

import (
	"fmt"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/typed"
)

type hash_storerContext struct {
	goka.Context // goka context
}

func (b *hash_storerProcessorBuilder) newContext(ctx goka.Context) *hash_storerContext {
	return &hash_storerContext{
		Context: ctx,
	}
}

func (c *hash_storerContext) SetValue(value *PhotoHashed) {
	c.Context.SetValue(value)
}

func (c *hash_storerContext) Value() *PhotoHashed {
	if val := c.Context.Value(); val != nil {
		return val.(*PhotoHashed)
	}
	return nil
}

type hash_storerProcessorBuilder struct {
	group        goka.Group
	edges        []goka.Edge
	_PhotoHashed typed.GInput[PhotoID, *PhotoHashed]
	_State       typed.GState[PhotoID, *PhotoHashed]
}

func NewHash_storerProcessorBuilder(group goka.Group) *hash_storerProcessorBuilder {
	return &hash_storerProcessorBuilder{group: group}
}

func (p *hash_storerProcessorBuilder) HandlePhotoHashed(topic goka.Stream, keyCodec typed.GCodec[PhotoID], valueCodec typed.GCodec[*PhotoHashed], handler func(ctx *hash_storerContext, key PhotoID, msg *PhotoHashed), opts ...typed.GOption) *hash_storerProcessorBuilder {
	codecBridge := typed.NewCodecBridge(valueCodec)
	p._PhotoHashed = typed.DefineInput(topic, keyCodec, valueCodec)
	edge := goka.Input(topic, codecBridge, func(ctx goka.Context, _msg interface{}) {

		decKey, err := keyCodec.Decode([]byte(ctx.Key()))
		if err != nil {
			ctx.Fail(err)
		}
		var msg *PhotoHashed
		if _msg != nil {
			msg = _msg.(*PhotoHashed)
		}

		handler(p.newContext(ctx), decKey, msg)
	})
	p.edges = append(p.edges, edge)
	return p
}

func (p *hash_storerProcessorBuilder) WithPersistState(keyCodec typed.GCodec[PhotoID], valueCodec typed.GCodec[*PhotoHashed], opts ...typed.GOption) *hash_storerProcessorBuilder {
	p._State = typed.DefineState(keyCodec, valueCodec)
	return p
}
func (p *hash_storerProcessorBuilder) Build() (*goka.GroupGraph, error) {
	if p._PhotoHashed == nil {
		return nil, fmt.Errorf("Uninitialized graph edge 'PhotoHashed'. Did you call 'HandlePhotoHashed(...)'?")
	}
	if p._State == nil {
		return nil, fmt.Errorf("Uninitialized graph edge 'State'. Did you call 'WithPersistState(...)'?")
	}
	var edges []goka.Edge
	edges = append(edges, goka.Persist(typed.NewCodecBridge(p._State.ValueCodec())))

	return goka.DefineGroup(p.group, append(edges, p.edges...)...), nil
}
