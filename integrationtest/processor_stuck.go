package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/kafka"
)

func main() {

	proc, err := goka.NewProcessor([]string{"localhost:9092"},
		goka.DefineGroup("processor-stuck-test",
			goka.Input("input", new(codec.Int64), func(ctx goka.Context, msg interface{}) {
				ctx.SetValue(msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
				ctx.Emit("output", ctx.Key(), msg)
			}),
			goka.Output("output", new(codec.Int64)),
			goka.Persist(new(codec.Int64)),
		))

	if err != nil {
		log.Fatalf("Cannot start processor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)

		log.Printf("Running processor")
		procRunErr := proc.Run(ctx)
		log.Printf("Processor finished with %v", procRunErr)
	}()

	log.Printf("wait 5 seconds before starting to emit")
	time.Sleep(5 * time.Second)

	for i := 0; i < 50; i++ {
		go func() {

			cfg := kafka.NewConfig()
			cfg.Producer.Retry.Max = 0
			cfg.Producer.Retry.Backoff = 1 * time.Millisecond
			emitter, err := goka.NewEmitter([]string{"localhost:9092"}, "input", new(codec.Int64),
				goka.WithEmitterProducerBuilder(
					kafka.ProducerBuilderWithConfig(cfg),
				),
			)
			if err != nil {
				log.Fatalf("Error creating emitter: %v", err)
			}

			time.Sleep(2 * time.Second)
			defer func() {
				log.Printf("finishing")
				emitter.Finish()
				log.Printf("done")
			}()

			defer recover()
			var done int64
			var emitted int64
			for i := 0; ; i++ {
				if atomic.LoadInt64(&done) > 0 {
					break
				}

				// when the context is done, stop emitting
				go func() {
					<-ctx.Done()
					atomic.AddInt64(&done, 1)
				}()
				emitted++
				if emitted%1000 == 0 {
					log.Printf("emitted %d", emitted)
				}
				prom, err := emitter.Emit(fmt.Sprintf("%d", i), int64(i))
				if err != nil {
					break
				}
				prom.Then(func(err error) {
					if err != nil {
						atomic.AddInt64(&done, 1)
					}
				})
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	log.Printf("waiting for the processor to shutdown")
	<-done
	log.Printf("processor is dead. Nice!")

	cancel()
}
