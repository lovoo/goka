package tester

import (
	"context"
	"log"
	"testing"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

func Test_Blubb(t *testing.T) {

	kafkaMock := New(t).SetCodec(new(codec.String))

	proc, err := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("group-testloop", new(codec.String), func(ctx goka.Context, msg interface{}) {
			log.Printf("%v", msg)
		}),
		goka.Input("topic", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.Emit("group-testloop", "key", msg)
		}),
		goka.Output("group-testloop", new(codec.String)),
		goka.Persist(new(codec.String)),
	),
		goka.WithTester(kafkaMock),
	)
	if err != nil {
		log.Fatalf("%v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		proc.Run(ctx)
		close(done)
	}()
	kafkaMock.ConsumeString("topic", "sender", "message")
	cancel()
	<-done
}
