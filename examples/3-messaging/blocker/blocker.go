package blocker

import (
	"context"
	"encoding/json"
	"io"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/examples/3-messaging/topicinit"
)

var (
	group  goka.Group  = "blocker"
	Table  goka.Table  = goka.GroupTable(group)
	Stream goka.Stream = "block_user"
)

type BlockEvent struct {
	Unblock bool
}

type BlockEventCodec struct{}

func (c *BlockEventCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *BlockEventCodec) Decode(data []byte) (interface{}, error) {
	var m BlockEvent
	return &m, json.Unmarshal(data, &m)
}

func (c *BlockEventCodec) DecodeP(data []byte) (interface{}, io.Closer, error) {
	dec, err := c.Decode(data)
	return dec, codec.NoopCloser, err
}

type BlockValue struct {
	Blocked bool
}
type BlockValueCodec struct{}

func (c *BlockValueCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *BlockValueCodec) Decode(data []byte) (interface{}, error) {
	var m BlockValue
	return &m, json.Unmarshal(data, &m)
}

func (c *BlockValueCodec) DecodeP(data []byte) (interface{}, io.Closer, error) {
	dec, err := c.Decode(data)
	return dec, codec.NoopCloser, err
}

func block(ctx goka.Context, msg interface{}) {
	var s *BlockValue
	if v := ctx.Value(); v == nil {
		s = new(BlockValue)
	} else {
		s = v.(*BlockValue)
	}

	if msg.(*BlockEvent).Unblock {
		s.Blocked = false
	} else {
		s.Blocked = true
	}
	ctx.SetValue(s)
}

func PrepareTopics(brokers []string) {
	topicinit.EnsureStreamExists(string(Stream), brokers)
}

func Run(ctx context.Context, brokers []string) func() error {
	return func() error {
		g := goka.DefineGroup(group,
			goka.Input(Stream, new(BlockEventCodec), block),
			goka.Persist(new(BlockValueCodec)),
		)
		p, err := goka.NewProcessor(brokers, g)
		if err != nil {
			return err
		}

		return p.Run(ctx)
	}
}
