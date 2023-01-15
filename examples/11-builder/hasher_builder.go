package main

// AUTO GENERATED DO NOT MODIFY!

import (
	"fmt"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/typed"
)

type hasherContext struct {
	goka.Context              // goka context
	_PhotoHashedTopic         goka.Stream
	_PhotoHashedTopicKeyCodec *typed.CodecBridge[PhotoID]
}

func (b *hasherProcessorBuilder) newContext(ctx goka.Context) *hasherContext {
	return &hasherContext{
		Context:                   ctx,
		_PhotoHashedTopic:         goka.Stream(b._PhotoHashed.TopicName()),
		_PhotoHashedTopicKeyCodec: typed.NewCodecBridge(b._PhotoHashed.KeyCodec()),
	}
}

func (c *hasherContext) EmitPhotoHashed(key PhotoID, msg *PhotoHashed) {
	encodedKey, err := c._PhotoHashedTopicKeyCodec.Encode(key)
	if err != nil {
		c.Fail(err)
	}
	c.Emit(c._PhotoHashedTopic, string(encodedKey), msg)
}

type hasherProcessorBuilder struct {
	group          goka.Group
	edges          []goka.Edge
	_PhotoUploaded typed.GInput[UserID, *PhotoUploaded]
	_PhotoHashed   typed.GOutput[PhotoID, *PhotoHashed]
}

func NewHasherProcessorBuilder(group goka.Group) *hasherProcessorBuilder {
	return &hasherProcessorBuilder{group: group}
}

func (p *hasherProcessorBuilder) HandlePhotoUploaded(topic goka.Stream, keyCodec typed.GCodec[UserID], valueCodec typed.GCodec[*PhotoUploaded], handler func(ctx *hasherContext, key UserID, msg *PhotoUploaded), opts ...typed.GOption) *hasherProcessorBuilder {
	codecBridge := typed.NewCodecBridge(valueCodec)
	p._PhotoUploaded = typed.DefineInput(topic, keyCodec, valueCodec)
	edge := goka.Input(topic, codecBridge, func(ctx goka.Context, _msg interface{}) {

		decKey, err := keyCodec.Decode([]byte(ctx.Key()))
		if err != nil {
			ctx.Fail(err)
		}
		var msg *PhotoUploaded
		if _msg != nil {
			msg = _msg.(*PhotoUploaded)
		}

		handler(p.newContext(ctx), decKey, msg)
	})
	p.edges = append(p.edges, edge)
	return p
}

func (p *hasherProcessorBuilder) WithOutputPhotoHashed(topic goka.Stream, keyCodec typed.GCodec[PhotoID], valueCodec typed.GCodec[*PhotoHashed], opts ...typed.GOption) *hasherProcessorBuilder {
	p._PhotoHashed = typed.DefineOutput(topic, keyCodec, valueCodec)
	return p
}
func (p *hasherProcessorBuilder) Build() (*goka.GroupGraph, error) {
	if p._PhotoUploaded == nil {
		return nil, fmt.Errorf("Uninitialized graph edge 'PhotoUploaded'. Did you call 'HandlePhotoUploaded(...)'?")
	}
	if p._PhotoHashed == nil {
		return nil, fmt.Errorf("Uninitialized graph edge 'PhotoHashed'. Did you call 'WithOutputPhotoHashed(...)'?")
	}
	var edges []goka.Edge
	edges = append(edges, goka.Output(goka.Stream(p._PhotoHashed.TopicName()), typed.NewCodecBridge(p._PhotoHashed.ValueCodec())))

	return goka.DefineGroup(p.group, append(edges, p.edges...)...), nil
}
