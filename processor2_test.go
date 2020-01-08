package goka

import (
	"context"
	"hash"
	"log"
	"testing"

	"github.com/Shopify/sarama"
	smock "github.com/Shopify/sarama/mocks"
	"github.com/facebookgo/ensure"
	"github.com/golang/mock/gomock"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/mock"
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

func TestProcessor2_Run(t *testing.T) {

	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	producerMock := mock.NewMockProducer(ctrl)

	var consumedMessage string
	graph := DefineGroup("test",
		Input("input", new(codec.String), func(ctx Context, msg interface{}) {
			consumedMessage = msg.(string)
		}),
	)

	groupBuilder, cg := createTestConsumerGroupBuilder(t)
	consBuilder, cons := createTestConsumerBuilder(t)
	tmBuilder, tm := createMockTopicManagerBuilder(t)
	_ = cg
	_ = cons
	_ = tm
	ctx, cancel := context.WithCancel(context.Background())

	newProc, err := NewProcessor2([]string{"localhost:9092"}, graph,
		WithConsumerGroupBuilder(groupBuilder),
		WithConsumerSaramaBuilder(consBuilder),
		WithProducerBuilder(func(brokers []string, clientID string, hasher func() hash.Hash32) (kafka.Producer, error) {
			return producerMock, nil
		}),
		WithTopicManagerBuilder(tmBuilder),
	)
	ensure.Nil(t, err)
	var (
		procErr error
		done    = make(chan struct{})
	)
	producerMock.EXPECT().Close().Return(nil)

	go func() {
		defer close(done)
		procErr = newProc.Run(ctx)
	}()

	newProc.WaitForReady()

	<-cg.SendMessage(&sarama.ConsumerMessage{Topic: "input",
		Value: []byte("testmessage"),
		Key:   []byte("testkey"),
	})

	if consumedMessage != "testmessage" {
		log.Printf("did not receive message")
		t.Errorf("did not receive message")
	}

	cancel()

	<-done
	ensure.Nil(t, procErr)

}
