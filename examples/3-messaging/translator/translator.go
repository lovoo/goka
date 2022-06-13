package translator

import (
	"context"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/examples/3-messaging/topicinit"
)

var (
	group  goka.Group  = "translator"
	Table  goka.Table  = goka.GroupTable(group)
	Stream goka.Stream = "translate-word"
)

type ValueCodec struct {
	codec.String
}

func translate(ctx goka.Context, msg interface{}) {
	ctx.SetValue(msg.(string))
}

func PrepareTopics(brokers []string) {
	topicinit.EnsureStreamExists(string(Stream), brokers)
}

func Run(ctx context.Context, brokers []string) func() error {
	return func() error {
		g := goka.DefineGroup(group,
			goka.Input(Stream, new(ValueCodec), translate),
			goka.Persist(new(ValueCodec)),
		)
		p, err := goka.NewProcessor(brokers, g)
		if err != nil {
			return err
		}

		return p.Run(ctx)
	}
}
