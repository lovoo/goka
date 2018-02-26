package translator

import (
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
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

func Run(brokers []string) {
	g := goka.DefineGroup(group,
		goka.Input(Stream, new(ValueCodec), translate),
		goka.Persist(new(ValueCodec)),
	)
	if p, err := goka.NewProcessor(brokers, g); err != nil {
		panic(err)
	} else if err = p.Start(); err != nil {
		panic(err)
	}
}
