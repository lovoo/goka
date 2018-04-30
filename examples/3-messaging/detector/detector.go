package detector

import (
	"context"
	"encoding/json"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/examples/3-messaging"
	"github.com/lovoo/goka/examples/3-messaging/blocker"
)

const (
	minMessages = 200
	maxRate     = 0.5
)

var (
	group goka.Group = "detector"
)

type Counters struct {
	Sent     int
	Received int
}

type CountersCodec struct{}

func (c *CountersCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *CountersCodec) Decode(data []byte) (interface{}, error) {
	var m Counters
	return &m, json.Unmarshal(data, &m)
}

func getValue(ctx goka.Context) *Counters {
	if v := ctx.Value(); v != nil {
		return v.(*Counters)
	}
	return &Counters{}
}

func detectSpammer(ctx goka.Context, c *Counters) bool {
	var (
		total = float64(c.Sent + c.Received)
		rate  = float64(c.Sent) / total
	)
	return total >= minMessages && rate >= maxRate
}

func Run(ctx context.Context, brokers []string) func() error {
	return func() error {
		g := goka.DefineGroup(group,
			goka.Input(messaging.SentStream, new(messaging.MessageCodec), func(ctx goka.Context, msg interface{}) {
				c := getValue(ctx)
				c.Sent++
				ctx.SetValue(c)

				// check if sender is a spammer
				if detectSpammer(ctx, c) {
					ctx.Emit(blocker.Stream, ctx.Key(), new(blocker.BlockEvent))
				}

				// Loop to receiver
				m := msg.(*messaging.Message)
				ctx.Loopback(m.To, m)
			}),
			goka.Loop(new(messaging.MessageCodec), func(ctx goka.Context, msg interface{}) {
				c := getValue(ctx)
				c.Received++
				ctx.SetValue(c)

				// check if receiver is a spammer
				if detectSpammer(ctx, c) {
					ctx.Emit(blocker.Stream, ctx.Key(), new(blocker.BlockEvent))
				}
			}),
			goka.Output(blocker.Stream, new(blocker.BlockEventCodec)),
			goka.Persist(new(CountersCodec)),
		)
		p, err := goka.NewProcessor(brokers, g)
		if err != nil {
			return err
		}

		return p.Run(ctx)
	}
}
