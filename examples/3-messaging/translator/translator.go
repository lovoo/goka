package translator

import (
	"context"
	"github.com/lovoo/goka/examples/3-messaging/topicinit"
	"sync"

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

func Run(ctx context.Context, brokers []string, initialized *sync.WaitGroup) func() error {
	return func() error {
		topicinit.EnsureStreamExists(string(group), brokers)
		topicinit.EnsureStreamExists(string(Stream), brokers)

		g := goka.DefineGroup(group,
			goka.Input(Stream, new(ValueCodec), translate),
			goka.Persist(new(ValueCodec)),
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
