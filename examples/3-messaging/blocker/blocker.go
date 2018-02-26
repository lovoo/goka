package blocker

import (
	"encoding/json"

	"github.com/lovoo/goka"
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

func Run(brokers []string) {
	g := goka.DefineGroup(group,
		goka.Input(Stream, new(BlockEventCodec), block),
		goka.Persist(new(BlockValueCodec)),
	)
	if p, err := goka.NewProcessor(brokers, g); err != nil {
		panic(err)
	} else if err = p.Start(); err != nil {
		panic(err)
	}
}
