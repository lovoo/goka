package goka

// AUTO GENERATED DO NOT MODIFY!
type topologyContext struct {
	ctx                Context // goka context
	_PhotoHashedTopic  Stream
	_ProfileTopic      Table
	_ProfileJoinTopic  Table
	_SmallProfileTopic Table
}

func (b *topologyProcessorBuilder) newContext(ctx Context) *topologyContext {
	return &topologyContext{
		ctx:                ctx,
		_PhotoHashedTopic:  Stream(b._PhotoHashed.topicName()),
		_ProfileTopic:      Table(b._Profile.topicName()),
		_ProfileJoinTopic:  Table(b._ProfileJoin.topicName()),
		_SmallProfileTopic: Table(b._SmallProfile.topicName()),
	}
}
func (c *topologyContext) EmitPhotoHashed(key string, msg *PhotoHashed) {
	c.ctx.Emit(c._PhotoHashedTopic, key, msg)
}
func (c *topologyContext) LookupProfile(key string) *BigProfile {
	if val := c.ctx.Lookup(c._ProfileTopic, key); val != nil {
		return val.(*BigProfile)
	}
	return nil
}
func (c *topologyContext) JoinProfileJoin() *BigProfile {
	if val := c.ctx.Join(c._ProfileJoinTopic); val != nil {
		return val.(*BigProfile)
	}
	return nil
}
func (c *topologyContext) LookupSmallProfile(key string) *SmallProfile {
	if val := c.ctx.Lookup(c._SmallProfileTopic, key); val != nil {
		return val.(*SmallProfile)
	}
	return nil
}
func (c *topologyContext) SetValue(value *PhotoHashed) {
	c.ctx.SetValue(value)
}
func (c *topologyContext) Value() *PhotoHashed {
	if val := c.ctx.Value(); val != nil {
		return val.(*PhotoHashed)
	}
	return nil
}

type topologyProcessorBuilder struct {
	edges          []Edge
	_PhotoUploaded GInput[string, *PhotoUploaded]
	_PhotoHashed2  GInput[string, *PhotoHashed]
	_PhotoHashed   GOutput[string, *PhotoHashed]
	_Profile       GLookup[string, *BigProfile]
	_ProfileJoin   GJoin[string, *BigProfile]
	_SmallProfile  GMappedLookup[string, *BigProfile, *SmallProfile]
	_State         GState[string, *PhotoHashed]
}

func NewTopologyProcessorBuilder(_PhotoUploaded GInput[string, *PhotoUploaded],
	_PhotoHashed2 GInput[string, *PhotoHashed],
	_PhotoHashed GOutput[string, *PhotoHashed],
	_Profile GLookup[string, *BigProfile],
	_ProfileJoin GJoin[string, *BigProfile],
	_SmallProfile GMappedLookup[string, *BigProfile, *SmallProfile],
	_State GState[string, *PhotoHashed],
) *topologyProcessorBuilder {
	return &topologyProcessorBuilder{_PhotoUploaded: _PhotoUploaded,
		_PhotoHashed2: _PhotoHashed2,
		_PhotoHashed:  _PhotoHashed,
		_Profile:      _Profile,
		_ProfileJoin:  _ProfileJoin,
		_SmallProfile: _SmallProfile,
		_State:        _State,
	}
}

func (p *topologyProcessorBuilder) OnPhotoUploaded(handler func(ctx *topologyContext, key string, msg *PhotoUploaded)) {
	var (
		valCodec = p._PhotoUploaded.valueCodec()
		keyCodec = p._PhotoUploaded.keyCodec()
	)

	codecBridge := NewCodecBridge(valCodec)
	edge := Input(Stream(p._PhotoUploaded.topicName()), codecBridge, func(ctx Context, _msg interface{}) {

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
}

func (p *topologyProcessorBuilder) OnPhotoHashed2(handler func(ctx *topologyContext, key string, msg *PhotoHashed)) {
	var (
		valCodec = p._PhotoHashed2.valueCodec()
		keyCodec = p._PhotoHashed2.keyCodec()
	)

	codecBridge := NewCodecBridge(valCodec)
	edge := Input(Stream(p._PhotoHashed2.topicName()), codecBridge, func(ctx Context, _msg interface{}) {

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
}
func (p *topologyProcessorBuilder) Build(group Group) (*GroupGraph, error) {
	var edges []Edge
	edges = append(edges, Output(Stream(p._PhotoHashed.topicName()), NewCodecBridge(p._PhotoHashed.valueCodec())))
	edges = append(edges, Lookup(Table(p._Profile.topicName()), NewCodecBridge(p._Profile.valueCodec())))
	edges = append(edges, Join(Table(p._ProfileJoin.topicName()), NewCodecBridge(p._ProfileJoin.valueCodec())))
	edges = append(edges, Lookup(Table(p._SmallProfile.topicName()), NewCodecBridge(p._SmallProfile.valueCodec())))
	edges = append(edges, Persist(NewCodecBridge(p._State.valueCodec())))

	return DefineGroup(group, append(edges, p.edges...)...), nil
}
