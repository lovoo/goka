package goka

import (
	"context"
	"hash"
	"log"
	"testing"

	"github.com/Shopify/sarama"
	smock "github.com/Shopify/sarama/mocks"
	"github.com/facebookgo/ensure"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/mock"
	"github.com/lovoo/goka/storage"
)

func createTestConsumerGroupBuilder(t *testing.T) (ConsumerGroupBuilder, *mock.ConsumerGroup) {
	mock := mock.NewConsumerGroup(t)
	return func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) {
		return mock, nil
	}, mock
}

func createTestConsumerBuilder(t *testing.T) (ConsumerBuilder, *smock.Consumer) {
	cons := smock.NewConsumer(t, sarama.NewConfig())

	return func(brokers []string, clientID string) (sarama.Consumer, error) {
		return cons, nil
	}, cons
}

func createMockTopicManagerBuilder(t *testing.T) (kafka.TopicManagerBuilder, *mock.TopicManager) {
	tm := mock.NewTopicManager(20, 2)

	return func(broker []string) (kafka.TopicManager, error) {
		return tm, nil
	}, tm
}

func createMockProducer(t *testing.T) (kafka.ProducerBuilder, *mock.Producer) {
	pb := mock.NewProducer(t)

	return func(brokers []string, clientID string, hasher func() hash.Hash32) (kafka.Producer, error) {
		return pb, nil
	}, pb
}

func TestProcessor2_Run(t *testing.T) {

	var consumedMessage string
	graph := DefineGroup("test",
		Input("input", new(codec.String), func(ctx Context, msg interface{}) {
			consumedMessage = msg.(string)
		}),
		Persist(new(codec.Int64)),
	)

	groupBuilder, cg := createTestConsumerGroupBuilder(t)
	consBuilder, cons := createTestConsumerBuilder(t)
	tmBuilder, tm := createMockTopicManagerBuilder(t)
	prodBuilder, prod := createMockProducer(t)
	_ = cg
	_ = cons
	_ = tm
	_ = prod
	ctx, cancel := context.WithCancel(context.Background())

	newProc, err := NewProcessor2([]string{"localhost:9092"}, graph,
		WithConsumerGroupBuilder(groupBuilder),
		WithConsumerSaramaBuilder(consBuilder),
		WithProducerBuilder(prodBuilder),
		WithStorageBuilder(storage.MemoryBuilder()),
		WithTopicManagerBuilder(tmBuilder),
	)
	ensure.Nil(t, err)
	var (
		procErr error
		done    = make(chan struct{})
	)

	go func() {
		defer close(done)
		procErr = newProc.Run(ctx)
	}()

	newProc.WaitForReady()

	cg.SendMessageWait(&sarama.ConsumerMessage{Topic: "input",
		Value: []byte("testmessage"),
		Key:   []byte("testkey"),
	})

	if consumedMessage != "testmessage" {
		log.Printf("did not receive message")
		t.Errorf("did not receive message")
	}

	val, err := newProc.Get("testkey")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, val.(int64), 1)

	// shutdown
	cancel()
	<-done
	ensure.Nil(t, procErr)

}
