package main

// AUTO GENERATED DO NOT MODIFY!

import (
	"fmt"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/typed"
)

type user_collectorContext struct {
	goka.Context            // goka context
	_PhotoHashTopic         goka.Table
	_PhotoHashTopicKeyCodec *typed.CodecBridge[PhotoID]
}

func (b *user_collectorProcessorBuilder) newContext(ctx goka.Context) *user_collectorContext {
	return &user_collectorContext{
		Context:                 ctx,
		_PhotoHashTopic:         goka.Table(b._PhotoHash.TopicName()),
		_PhotoHashTopicKeyCodec: typed.NewCodecBridge(b._PhotoHash.KeyCodec()),
	}
}

func (c *user_collectorContext) LookupPhotoHash(key PhotoID) *PhotoHashed {
	encodedKey, err := c._PhotoHashTopicKeyCodec.Encode(key)
	if err != nil {
		c.Fail(err)
	}
	if val := c.Lookup(c._PhotoHashTopic, string(encodedKey)); val != nil {
		return val.(*PhotoHashed)
	}
	return nil
}

func (c *user_collectorContext) SetValue(value *Profile) {
	c.Context.SetValue(value)
}

func (c *user_collectorContext) Value() *Profile {
	if val := c.Context.Value(); val != nil {
		return val.(*Profile)
	}
	return nil
}

type user_collectorProcessorBuilder struct {
	group       goka.Group
	edges       []goka.Edge
	_PhotoCheck typed.GInput[UserID, *PhotoCheck]
	_PhotoHash  typed.GLookup[PhotoID, *PhotoHashed]
	_Profile    typed.GState[UserID, *Profile]
}

func NewUser_collectorProcessorBuilder(group goka.Group) *user_collectorProcessorBuilder {
	return &user_collectorProcessorBuilder{group: group}
}

func (p *user_collectorProcessorBuilder) HandlePhotoCheck(topic goka.Stream, keyCodec typed.GCodec[UserID], valueCodec typed.GCodec[*PhotoCheck], handler func(ctx *user_collectorContext, key UserID, msg *PhotoCheck), opts ...typed.GOption) *user_collectorProcessorBuilder {
	codecBridge := typed.NewCodecBridge(valueCodec)
	p._PhotoCheck = typed.DefineInput(topic, keyCodec, valueCodec)
	edge := goka.Input(topic, codecBridge, func(ctx goka.Context, _msg interface{}) {

		decKey, err := keyCodec.Decode([]byte(ctx.Key()))
		if err != nil {
			ctx.Fail(err)
		}
		var msg *PhotoCheck
		if _msg != nil {
			msg = _msg.(*PhotoCheck)
		}

		handler(p.newContext(ctx), decKey, msg)
	})
	p.edges = append(p.edges, edge)
	return p
}

func (p *user_collectorProcessorBuilder) WithLookupPhotoHash(table goka.Table, keyCodec typed.GCodec[PhotoID], valueCodec typed.GCodec[*PhotoHashed], opts ...typed.GOption) *user_collectorProcessorBuilder {

	p._PhotoHash = typed.DefineLookup(table, keyCodec, valueCodec)
	return p
}

func (p *user_collectorProcessorBuilder) WithPersistProfile(keyCodec typed.GCodec[UserID], valueCodec typed.GCodec[*Profile], opts ...typed.GOption) *user_collectorProcessorBuilder {
	p._Profile = typed.DefineState(keyCodec, valueCodec)
	return p
}
func (p *user_collectorProcessorBuilder) Build() (*goka.GroupGraph, error) {
	if p._PhotoCheck == nil {
		return nil, fmt.Errorf("Uninitialized graph edge 'PhotoCheck'. Did you call 'HandlePhotoCheck(...)'?")
	}
	if p._PhotoHash == nil {
		return nil, fmt.Errorf("Uninitialized graph edge 'PhotoHash'. Did you call 'WithLookupPhotoHash(...)'?")
	}
	if p._Profile == nil {
		return nil, fmt.Errorf("Uninitialized graph edge 'Profile'. Did you call 'WithPersistProfile(...)'?")
	}
	var edges []goka.Edge
	edges = append(edges, goka.Lookup(goka.Table(p._PhotoHash.TopicName()), typed.NewCodecBridge(p._PhotoHash.ValueCodec())))
	edges = append(edges, goka.Persist(typed.NewCodecBridge(p._Profile.ValueCodec())))

	return goka.DefineGroup(p.group, append(edges, p.edges...)...), nil
}
