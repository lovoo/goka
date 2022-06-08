package main

import (
	"context"
	"log"

	"github.com/lovoo/goka"
	storage "github.com/lovoo/goka/storage/redis"

	redis "gopkg.in/redis.v5"
)

// Publisher defines an interface to Publish the event somewhere.
type Publisher interface {
	Publish(ctx context.Context, key string, event *Event) error
	Close() error
}

// Consume starts goka events consumer.
func Consume(pub Publisher, brokers []string, group string, stream string, store string, namespace string) error {
	codec := new(Codec)

	tmc := goka.NewTopicManagerConfig()
	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	err = tm.EnsureStreamExists(stream, 8)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", stream, err)
	}

	input := goka.Input(goka.Stream(stream), codec, func(ctx goka.Context, msg interface{}) {
		event, ok := msg.(*Event)
		if ok {
			pub.Publish(context.Background(), ctx.Key(), event)
		}
	})
	graph := goka.DefineGroup(goka.Group(group), input, goka.Persist(codec))

	opts := []goka.ProcessorOption{}
	switch {
	case store != "":
		client := redis.NewClient(&redis.Options{
			Addr: store,
		})
		opts = append(opts, goka.WithStorageBuilder(storage.RedisBuilder(client, namespace)))
		defer client.Close()
	}
	processor, err := goka.NewProcessor(brokers, graph, opts...)
	if err != nil {
		return err
	}

	return processor.Run(context.Background())
}
