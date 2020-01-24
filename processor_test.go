package goka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/facebookgo/ensure"
	"github.com/golang/mock/gomock"
	"github.com/lovoo/goka/codec"
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

func createMockBuilder(t *testing.T) *builderMock {
	ctrl := gomock.NewController(t)
	bm := newBuilderMock(ctrl)
	return bm
}

func createTestConsumerGroupBuilder(t *testing.T) (ConsumerGroupBuilder, *MockConsumerGroup) {
	mock := NewMockConsumerGroup(t)
	return func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) {
		return mock, nil
	}, mock
}

func createTestConsumerBuilder(t *testing.T) (SaramaConsumerBuilder, *MockConsumer) {
	cons := NewMockConsumer(t, nil)

	return func(brokers []string, clientID string) (sarama.Consumer, error) {
		return cons, nil
	}, cons
}

func setupMockBuilder(bm *builderMock) {
	bm.tmgr.EXPECT().Partitions(gomock.Any()).Return([]int32{0}, nil)
	bm.tmgr.EXPECT().EnsureTableExists("test-table", gomock.Any()).Return(nil)
	bm.tmgr.EXPECT().Close().Return(nil)
	bm.st.EXPECT().Close().Return(nil)
	bm.producer.EXPECT().Close().Return(nil)
	bm.st.EXPECT().GetOffset(gomock.Any()).Return(int64(161), nil)
	bm.tmgr.EXPECT().GetOffset("test-table", gomock.Any(), sarama.OffsetNewest).Return(int64(161), nil)
	bm.tmgr.EXPECT().GetOffset("test-table", gomock.Any(), sarama.OffsetOldest).Return(int64(161), nil)
	bm.st.EXPECT().MarkRecovered().Return(nil)
	bm.st.EXPECT().Get("testkey").Return([]byte(strconv.FormatInt(3, 10)), nil)
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
	t.Run("input-persist", func(t *testing.T) {
		bm := createMockBuilder(t)
		setupMockBuilder(bm)

		groupBuilder, cg := createTestConsumerGroupBuilder(t)
		consBuilder, cons := createTestConsumerBuilder(t)
		_ = cg
		_ = cons

		graph := DefineGroup("test",
			Input("input", new(codec.Int64), accumulate),
			Persist(new(codec.Int64)),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
			bm.createProcessorOptions(consBuilder, groupBuilder)...,
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
		bm := createMockBuilder(t)

		groupBuilder, cg := createTestConsumerGroupBuilder(t)
		consBuilder, cons := createTestConsumerBuilder(t)
		_ = cg
		_ = cons

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
			bm.createProcessorOptions(consBuilder, groupBuilder)...,
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

		// msgs := prod.MessagesForTopic("test-loop")
		// ensure.DeepEqual(t, len(msgs), 1)
		// parsedValue, err := strconv.ParseInt(string(msgs[0].Value), 10, 64)
		// ensure.Nil(t, err)
		// ensure.DeepEqual(t, parsedValue, int64(23))

		// shutdown
		newProc.Stop()
		<-done
		ensure.Nil(t, procErr)
	})
	t.Run("consume-error", func(t *testing.T) {
		bm := createMockBuilder(t)

		groupBuilder, cg := createTestConsumerGroupBuilder(t)
		consBuilder, cons := createTestConsumerBuilder(t)
		_ = cg
		_ = cons

		graph := DefineGroup("test",
			// not really used, we're failing anyway
			Input("input", new(codec.Int64), accumulate),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
			bm.createProcessorOptions(consBuilder, groupBuilder)...,
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
		bm := createMockBuilder(t)

		groupBuilder, cg := createTestConsumerGroupBuilder(t)
		consBuilder, cons := createTestConsumerBuilder(t)
		_ = cg
		_ = cons

		graph := DefineGroup("test",
			// not really used, we're failing anyway
			Input("input", new(codec.Int64), accumulate),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
			bm.createProcessorOptions(consBuilder, groupBuilder)...,
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
