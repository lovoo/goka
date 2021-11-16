package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/multierr"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "example-visit-clicks-input"
	group   goka.Group  = "example-visit-group"

	tmc *goka.TopicManagerConfig
)

func init() {
	// This sets the default replication to 1. If you have more then one broker
	// the default configuration can be used.
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
}

// emit messages until stopped
func runEmitter(ctx context.Context) {
	emitter, err := goka.NewEmitter(brokers, topic, new(codec.Int64))
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer emitter.Finish()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for i := 0; ; i++ {
		select {
		case <-ticker.C:

			err = emitter.EmitSync(fmt.Sprintf("key-%d", i%10), int64(1))
			if err != nil {
				log.Fatalf("error emitting message: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func main() {

	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	err = tm.EnsureStreamExists(string(topic), 8)
	if err != nil {
		log.Fatalf("Error creating kafka topic %s: %v", topic, err)
	}

	sigs := make(chan os.Signal)
	go func() {
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errg, ctx := multierr.NewErrGroup(ctx)

	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.Int64), func(ctx goka.Context, msg interface{}) {
			var counter int64
			if val := ctx.Value(); val != nil {
				counter = val.(int64)
			}
			counter += msg.(int64)
			log.Printf("%s: %d", ctx.Key(), counter)
			ctx.SetValue(counter)

		}),
		goka.Visitor("reset", func(ctx goka.Context, meta interface{}) {
			log.Printf("resetting %s: %d", ctx.Key(), meta.(int64))
			ctx.SetValue(meta)
		}),
		goka.Persist(new(codec.Int64)),
	)

	proc, err := goka.NewProcessor(brokers,
		g,
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
	)
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}

	// start the emitter
	errg.Go(func() error {
		runEmitter(ctx)
		return nil
	})

	// start the processor
	errg.Go(func() error {
		return proc.Run(ctx)
	})

	errg.Go(func() error {
		select {
		case <-sigs:
		case <-ctx.Done():
		}
		cancel()
		return nil
	})

	time.Sleep(5 * time.Second)

	visited, err := proc.VisitAllWithStats(ctx, "reset", int64(0))
	if err != nil {
		log.Printf("error visiting: %v", err)
	}

	log.Printf("visited %d values", visited)

	time.Sleep(5 * time.Second)
	log.Printf("stopping...")
	cancel()
	if err := errg.Wait().ErrorOrNil(); err != nil {
		log.Fatalf("error running: %v", err)
	}
	log.Printf("done.")

}
