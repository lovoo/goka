package tester

import (
	"context"
	"fmt"
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

func Test_Lookup(t *testing.T) {

	kafkaMock := New(t).SetCodec(new(codec.String))

	// add a lookup table
	kafkaMock.AddMockLookupTable("lookup-table", new(codec.String))
	kafkaMock.SetLookupValue("lookup-table", "somekey", "42")
	proc, err := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			val := ctx.Lookup("lookup-table", "somekey").(string)
			if val != "42" {
				ctx.Fail(fmt.Errorf("lookup value was unexpected"))
			}
		}),
		goka.Lookup("lookup-table", new(codec.String)),
	),
		goka.WithTester(kafkaMock),
	)

	if err != nil {
		log.Fatalf("Error creating processor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		err := proc.Run(ctx)
		if err != nil {
			panic(fmt.Errorf("Error running processor: %v", err))
		}

		close(done)
	}()
	kafkaMock.ConsumeString("input", "sender", "message")
	cancel()
	<-done
}
