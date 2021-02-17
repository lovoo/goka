package main

import (
	"context"
	"flag"
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
	brokers                  = []string{"localhost:9092"}
	inputTopic   goka.Stream = "input-topic"
	forwardTopic goka.Stream = "forward-topic"

	nocommit = flag.Bool("no-commit", false, "set this to true for testing what happens if we don't commit")

	tmc *goka.TopicManagerConfig
)

func init() {
	// This sets the default replication to 1. If you have more then one broker
	// the default configuration can be used.
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
}

func main() {
	flag.Parse()

	createTopics()

	// some channel to stop on signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal)
	go func() {
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
		<-sigs
		cancel()
	}()

	errg, ctx := multierr.NewErrGroup(ctx)

	// (1) start the input processor
	errg.Go(func() error {

		inputEmitter, err := goka.NewEmitter(brokers, "input-topic", new(codec.String))
		if err != nil {
			log.Fatalf("error external emitter: %v", err)
		}

		// error dropped here for simplicity
		defer inputEmitter.Finish()

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				// error dropped here for simplicity
				inputEmitter.Emit("time", fmt.Sprintf("%d", time.Now().Unix()))
			}
		}
	})

	// (2) start the forwarding processor
	errg.Go(func() error {

		// This emitter represents the component that depends on "external" ressources, like a different kafka-cluster,
		// or an emitter to a different message queue or database with async writes etc...
		forwardEmitter, err := goka.NewEmitter(brokers, forwardTopic, new(codec.String))
		if err != nil {
			log.Fatalf("error external emitter: %v", err)
		}

		p, err := goka.NewProcessor(brokers,
			goka.DefineGroup("forwarder",
				goka.Input(inputTopic, new(codec.String), func(ctx goka.Context, msg interface{}) {

					// forward the incoming message to the "external" emitter
					prom, err := forwardEmitter.Emit("time", msg)
					if err != nil {
						ctx.Fail(fmt.Errorf("error emitting: %v", err))
					}

					commit := ctx.DeferCommit()

					if !*nocommit {
						// wire the commit to external emitter's promise.
						prom.Then(commit)
					}
				}),
			),
			goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		)
		if err != nil {
			log.Fatalf("error creating processor: %v", err)
		}

		return p.Run(ctx)
	})

	// (3) start the consuming processor
	errg.Go(func() error {

		// processor that simply prints the incoming message.
		p, err := goka.NewProcessor(brokers,
			goka.DefineGroup("consumer",
				goka.Input(forwardTopic, new(codec.String), func(ctx goka.Context, msg interface{}) {
					log.Printf("received message %s: %s", ctx.Key(), msg.(string))
				}),
			),
			goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		)
		if err != nil {
			log.Fatalf("error creating processor: %v", err)
		}

		return p.Run(ctx)
	})

	if err := errg.Wait().NilOrError(); err != nil {
		log.Fatalf("Error executing: %v", err)
	}
}

func createTopics() {
	// create a new topic manager so we can create the streams we need
	tmg, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}

	// error ignored for simplicity
	defer tmg.Close()
	errs := new(multierr.Errors)
	errs.Collect(tmg.EnsureStreamExists(string(inputTopic), 6))
	errs.Collect(tmg.EnsureStreamExists(string(forwardTopic), 6))
	if errs.HasErrors() {
		log.Fatalf("cannot create topics: %v", errs.NilOrError())
	}
}
