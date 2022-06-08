package blocker

import (
	"context"
	"encoding/json"
	"github.com/lovoo/goka/examples/3-messaging/topic"
	"sync"

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

func Run(ctx context.Context, brokers []string, initialized *sync.WaitGroup) func() error {
	return func() error {
		topic.EnsureStreamExists(string(Stream), brokers)

		g := goka.DefineGroup(group,
			goka.Input(Stream, new(BlockEventCodec), block),
			goka.Persist(new(BlockValueCodec)),
		)
		p, err := goka.NewProcessor(brokers, g)
		if err != nil {
			// we have to signal done here so other Goroutines of the errgroup
			// can continue execution
			initialized.Done()
			return err
		}

		initialized.Done()
		initialized.Wait()

		return p.Run(ctx)
	}
}
