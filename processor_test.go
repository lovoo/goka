package goka

import (
	"context"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/facebookgo/ensure"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/storage"
)

func PanicStringContains(t *testing.T, s string) {
	if r := recover(); r != nil {
		err := r.(error)
		ensure.StringContains(t, err.Error(), s)
	} else {
		// there was no panic
		t.Errorf("panic expected")
		t.FailNow()
	}
}

func createTestConsumerGroupBuilder(t *testing.T) (ConsumerGroupBuilder, *MockConsumerGroup) {
	mock := NewMockConsumerGroup(t)
	return func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) {
		return mock, nil
	}, mock
}

func createTestConsumerBuilder(t *testing.T) (SaramaConsumerBuilder, *mocks.Consumer) {
	cons := mocks.NewConsumer(t, nil)

	return func(brokers []string, clientID string) (sarama.Consumer, error) {
		return cons, nil
	}, cons
}

func createMockTopicManagerBuilder(t *testing.T) (TopicManagerBuilder, *MockTopicManager) {
	tm := NewMockTopicManager(1, 1)

	return func(broker []string) (TopicManager, error) {
		return tm, nil
	}, tm
}

func createMockProducer(t *testing.T) (ProducerBuilder, *MockProducer) {
	pb := NewMockProducer(t)

	return func(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error) {
		return pb, nil
	}, pb
}

// accumulate is a callback that increments the
// table value by the incoming message.
// Persist and incoming codecs must be codec.Int64
func accumulate(ctx Context, msg interface{}) {
	inc := msg.(int64)
	val := ctx.Value()
	if val == nil {
		ctx.SetValue(inc)
	} else {
		ctx.SetValue(val.(int64) + inc)
	}
}

func TestProcessor_Run(t *testing.T) {

	groupBuilder, cg := createTestConsumerGroupBuilder(t)
	consBuilder, cons := createTestConsumerBuilder(t)
	tmBuilder, tm := createMockTopicManagerBuilder(t)
	prodBuilder, prod := createMockProducer(t)
	_ = cg
	_ = cons
	_ = tm
	_ = prod

	t.Run("input-persist", func(t *testing.T) {

		graph := DefineGroup("test",
			Input("input", new(codec.Int64), accumulate),
			Persist(new(codec.Int64)),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
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

		cons.ExpectConsumePartition("test-table", 0, 0)

		go func() {
			defer close(done)
			procErr = newProc.Run(ctx)
		}()

		newProc.WaitForReady()

		// if there was an error during startup, no point in sending messages
		// and waiting for them to be delivered
		ensure.Nil(t, procErr)

		cg.SendMessageWait(&sarama.ConsumerMessage{Topic: "input",
			Value: []byte(strconv.FormatInt(3, 10)),
			Key:   []byte("testkey"),
		})

		val, err := newProc.Get("testkey")
		ensure.Nil(t, err)
		ensure.DeepEqual(t, val.(int64), int64(3))

		// shutdown
		newProc.Stop()
		<-done
		ensure.Nil(t, procErr)
	})

	t.Run("loopback", func(t *testing.T) {
		prod.Clear()

		graph := DefineGroup("test",
			// input passes to loopback
			Input("input", new(codec.Int64), func(ctx Context, msg interface{}) {
				ctx.Loopback(ctx.Key(), msg)
			}),
			// this will not be called in the test but we define it, otherwise the context will raise an error
			Loop(new(codec.Int64), accumulate),
			Persist(new(codec.Int64)),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
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

		cons.ExpectConsumePartition("test-table", 0, 0)

		go func() {
			defer close(done)
			procErr = newProc.Run(ctx)
		}()

		newProc.WaitForReady()

		// if there was an error during startup, no point in sending messages
		// and waiting for them to be delivered
		ensure.Nil(t, procErr)
		cg.SendMessageWait(&sarama.ConsumerMessage{Topic: "input",
			Key:   []byte("testkey"),
			Value: []byte(strconv.FormatInt(23, 10)),
		})

		msgs := prod.MessagesForTopic("test-loop")
		ensure.DeepEqual(t, len(msgs), 1)
		parsedValue, err := strconv.ParseInt(string(msgs[0].Value), 10, 64)
		ensure.Nil(t, err)
		ensure.DeepEqual(t, parsedValue, int64(23))

		// shutdown
		newProc.Stop()
		<-done
		ensure.Nil(t, procErr)
	})
	t.Run("consume-error", func(t *testing.T) {
		prod.Clear()

		graph := DefineGroup("test",
			// not really used, we're failing anyway
			Input("input", new(codec.Int64), accumulate),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
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

		// if there was an error during startup, no point in sending messages
		// and waiting for them to be delivered
		ensure.Nil(t, procErr)
		cg.SendError(fmt.Errorf("test-error"))
		cancel()
		<-done
		// the errors sent back by the consumergroup do not lead to a failure of the processor
		ensure.Nil(t, procErr)

	})

	t.Run("consgroup-error", func(t *testing.T) {
		prod.Clear()

		graph := DefineGroup("test",
			// not really used, we're failing anyway
			Input("input", new(codec.Int64), accumulate),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
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

		cg.FailOnConsume(fmt.Errorf("consume-error"))

		go func() {
			defer close(done)
			procErr = newProc.Run(ctx)
		}()

		newProc.WaitForReady()

		// if there was an error during startup, no point in sending messages
		// and waiting for them to be delivered
		<-done
		// the errors sent back by the consumergroup do not lead to a failure of the processor
		ensure.True(t, strings.Contains(procErr.Error(), "consume-error"))
	})
}
