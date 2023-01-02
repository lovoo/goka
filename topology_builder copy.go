package goka

// AUTO GENERATED DO NOT MODIFY!
type (
	topologyProcessorBuilder2 struct {
		group Group
		edges []Edge

		_PhotoUploaded GInput[string, *PhotoUploaded]
		_state         GState[string, *PhotoHashed]
	}
	topologyContext2 interface {
		EmitPhotoHashed(key string, msg *PhotoHashed)
		LookupProfile(key string) *BigProfile
		LookupSmallProfile(key string) *SmallProfile
		NativeCtx() Context
		SetValue(value *PhotoHashed)
		Value() *PhotoHashed
	}

	_topologyContext struct {
		ctx Context
	}
)

func (c *_topologyContext) Value() *PhotoHashed {
	if val := c.ctx.Value(); val != nil {
		return val.(*PhotoHashed)
	}
	return nil
}

func (c *_topologyContext) EmitPhotoHashed(key string, msg *PhotoHashed) {
}

func (c *_topologyContext) LookupProfile(key string) *BigProfile {
	return nil
}

func (c *_topologyContext) LookupSmallProfile(key string) *SmallProfile {
	return nil
}

func (c *_topologyContext) NativeCtx() Context {
	return c.ctx
}

func (c *_topologyContext) SetValue(value *PhotoHashed) {
}

func NewTopologyProcessorBuilder2(
	group Group,
) *topologyProcessorBuilder2 {
	return &topologyProcessorBuilder2{
		group: group,
	}
}

func (t *topologyProcessorBuilder2) Build(brokers []string, opts ...ProcessorOption) (*Processor, error) {
	// if has state
	edges := []Edge{
		Persist(NewCodecBridge[*PhotoHashed](t._state.valueCodec())),
	}

	gg := DefineGroup(t.group, append(edges, t.edges...)...)

	return NewProcessor(brokers, gg)
}

func (p *topologyProcessorBuilder2) HandlePhotoUploaded2(handler func(ctx topologyContext, key string, msg *PhotoUploaded)) {
	var (
		valCodec = p._PhotoUploaded.valueCodec()
		keyCodec = p._PhotoUploaded.keyCodec()
	)

	codecBridge := NewCodecBridge[*PhotoUploaded](valCodec)
	edge := Input(Stream(p._PhotoUploaded.topicName()), codecBridge, func(ctx Context, _msg interface{}) {
		tCtx := &_topologyContext{
			ctx: ctx,
		}
		decKey, err := keyCodec.Decode([]byte(ctx.Key()))
		if err != nil {
			ctx.Fail(err)
		}
		var msg *PhotoUploaded
		if _msg != nil {
			msg = _msg.(*PhotoUploaded)
		}
		handler(tCtx, decKey, msg)
	})
	p.edges = append(p.edges, edge)
}
