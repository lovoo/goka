package detector

import (
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

func detectSpammer(ctx goka.Context, c *Counters) {
	var (
		total = float64(c.Sent + c.Received)
		rate  = float64(c.Sent) / total
	)
	if total >= minMessages && rate >= maxRate {
		ctx.Emit(blocker.Stream, ctx.Key(), new(blocker.BlockEvent))
	}
}

func Run(brokers []string) {
	g := goka.DefineGroup(group,
		goka.Input(messaging.SentStream, new(messaging.MessageCodec), func(ctx goka.Context, msg interface{}) {
			c := getValue(ctx)
			c.Sent++
			ctx.SetValue(c)
			m := msg.(*messaging.Message)
			// Loop to receiver
			ctx.Loopback(m.To, m)
			// Check if sender is a spammer
			detectSpammer(ctx, c)
		}),
		goka.Loop(new(messaging.MessageCodec), func(ctx goka.Context, msg interface{}) {
			c := getValue(ctx)
			c.Received++
			ctx.SetValue(c)
			// Check if receiver is a spammer
			detectSpammer(ctx, c)
		}),
		goka.Output(blocker.Stream, new(blocker.BlockEventCodec)),
		goka.Persist(new(CountersCodec)),
	)
	if p, err := goka.NewProcessor(brokers, g); err != nil {
		panic(err)
	} else if err = p.Start(); err != nil {
		panic(err)
	}
}
