package goka

// AUTO GENERATED DO NOT MODIFY!
type topologyProcessorBuilder struct {
}
type topologyContext interface {
	EmitPhotoHashed(key string, msg *PhotoHashed)
	LookupProfile(key string) *BigProfile
	LookupSmallProfile(key string) *SmallProfile
	SetValue(value *PhotoHashed)
	Value() *PhotoHashed
}

func (p *topologyProcessorBuilder) HandlePhotoUploaded(handler func(ctx topologyContext, key string, msg *PhotoUploaded)) {
}
