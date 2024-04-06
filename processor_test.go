package goka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/storage"
)

func createMockBuilder(t *testing.T) (*gomock.Controller, *builderMock) {
	ctrl := NewMockController(t)
	bm := newBuilderMock(ctrl)
	bm.st = storage.NewMemory()
	return ctrl, bm
}

func createTestConsumerGroupBuilder(t *testing.T) (ConsumerGroupBuilder, *MockConsumerGroup) {
	mock := NewMockConsumerGroup(t)
	return func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) {
		return mock, nil
	}, mock
}

func createTestConsumerBuilder(t *testing.T) (SaramaConsumerBuilder, *MockAutoConsumer) {
	cons := NewMockAutoConsumer(t, nil)

	return func(brokers []string, clientID string) (sarama.Consumer, error) {
		return cons, nil
	}, cons
}

func expectCGEmit(bm *builderMock, table string, msgs []*sarama.ConsumerMessage) {
	for _, msg := range msgs {
		bm.producer.EXPECT().EmitWithHeaders(table, string(msg.Key), msg.Value, nil).Return(NewPromise().finish(nil, nil))
	}
}

func expectCGLoop(bm *builderMock, loop string, msgs []*sarama.ConsumerMessage) {
	bm.tmgr.EXPECT().EnsureStreamExists(loop, 1).AnyTimes()
	for _, msg := range msgs {
		bm.producer.EXPECT().EmitWithHeaders(loop, string(msg.Key), gomock.Any(), nil).Return(NewPromise().finish(nil, nil))
	}
}

func expectCGConsume(bm *builderMock, table string, msgs []*sarama.ConsumerMessage) {
	var current int64

	bm.producer.EXPECT().Close().Return(nil).AnyTimes()

	bm.tmgr.EXPECT().Close().Return(nil).AnyTimes()
	bm.tmgr.EXPECT().EnsureTableExists(table, gomock.Any()).Return(nil)
	bm.tmgr.EXPECT().Partitions(gomock.Any()).Return([]int32{0}, nil).AnyTimes()
	bm.tmgr.EXPECT().GetOffset(table, gomock.Any(), sarama.OffsetNewest).Return(func() int64 {
		defer func() {
			current++
		}()
		return current
	}(), nil)
	bm.tmgr.EXPECT().GetOffset(table, gomock.Any(), sarama.OffsetOldest).Return(func() int64 {
		return 0
	}(), nil)
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
		ctrl, bm := createMockBuilder(t)
		defer ctrl.Finish()
		var (
			topic  = "test-table"
			toEmit = []*sarama.ConsumerMessage{
				{
					Topic: "input",
					Value: []byte(strconv.FormatInt(3, 10)),
					Key:   []byte("test-key-1"),
				},
				{
					Topic: "input",
					Value: []byte(strconv.FormatInt(3, 10)),
					Key:   []byte("test-key-2"),
				},
			}
		)

		expectCGConsume(bm, topic, toEmit)
		expectCGEmit(bm, topic, toEmit)

		groupBuilder, cg := createTestConsumerGroupBuilder(t)
		consBuilder, _ := createTestConsumerBuilder(t)

		graph := DefineGroup("test",
			Input("input", new(codec.Int64), accumulate),
			Persist(new(codec.Int64)),
		)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000)
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
			bm.createProcessorOptions(consBuilder, groupBuilder)...,
		)
		require.NoError(t, err)
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
		select {
		case <-done:
			require.NoError(t, procErr)
		default:
		}

		for _, msg := range toEmit {
			cg.SendMessageWait(msg)
		}

		val, err := newProc.Get("test-key-1")
		require.NoError(t, err)
		require.Equal(t, int64(3), val.(int64))

		val, err = newProc.Get("test-key-2")
		require.NoError(t, err)
		require.Equal(t, int64(3), val.(int64))

		// shutdown
		newProc.Stop()
		<-done
		require.NoError(t, procErr)
	})
	t.Run("loopback", func(t *testing.T) {
		ctrl, bm := createMockBuilder(t)
		defer ctrl.Finish()

		var (
			topic  = "test-table"
			loop   = "test-loop"
			toEmit = []*sarama.ConsumerMessage{
				{
					Topic: "input",
					Value: []byte(strconv.FormatInt(23, 10)),
					Key:   []byte("test-key"),
				},
			}
		)

		expectCGConsume(bm, topic, toEmit)
		expectCGLoop(bm, loop, toEmit)

		groupBuilder, cg := createTestConsumerGroupBuilder(t)
		consBuilder, _ := createTestConsumerBuilder(t)

		graph := DefineGroup("test",
			// input passes to loopback
			Input("input", new(codec.Int64), func(ctx Context, msg interface{}) {
				ctx.Loopback(ctx.Key(), msg)
			}),
			// this will not be called in the test but we define it, otherwise the context will raise an error
			Loop(new(codec.Int64), accumulate),
			Persist(new(codec.Int64)),
		)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
			bm.createProcessorOptions(consBuilder, groupBuilder)...,
		)
		require.NoError(t, err)
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
		select {
		case <-done:
			require.NoError(t, procErr)
		default:
		}

		for _, msg := range toEmit {
			cg.SendMessageWait(msg)
		}

		// shutdown
		newProc.Stop()
		<-done
		require.NoError(t, procErr)
	})
	t.Run("consume-error", func(t *testing.T) {
		ctrl, bm := createMockBuilder(t)
		defer ctrl.Finish()

		bm.tmgr.EXPECT().Close().Times(1)
		bm.tmgr.EXPECT().Partitions(gomock.Any()).Return([]int32{0}, nil).Times(1)
		bm.producer.EXPECT().Close().Times(1)

		groupBuilder, cg := createTestConsumerGroupBuilder(t)
		consBuilder, _ := createTestConsumerBuilder(t)

		graph := DefineGroup("test",
			// not really used, we're failing anyway
			Input("input", new(codec.Int64), accumulate),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
			bm.createProcessorOptions(consBuilder, groupBuilder)...,
		)
		require.NoError(t, err)
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
		select {
		case <-done:
			require.NoError(t, procErr)
		default:
		}
		cg.SendError(fmt.Errorf("test-error"))
		cancel()
		<-done
		// the errors sent back by the consumergroup do not lead to a failure of the processor
		require.NoError(t, procErr)
	})
	t.Run("setup-error", func(t *testing.T) {
		ctrl, bm := createMockBuilder(t)
		defer ctrl.Finish()

		bm.tmgr.EXPECT().Close().Times(1)
		bm.tmgr.EXPECT().Partitions(gomock.Any()).Return([]int32{0}, nil).Times(1)
		bm.producer.EXPECT().Close().Times(1)

		groupBuilder, cg := createTestConsumerGroupBuilder(t)
		consBuilder, _ := createTestConsumerBuilder(t)

		graph := DefineGroup("test",
			// not really used, we're failing anyway
			Input("input", new(codec.Int64), accumulate),
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
			bm.createProcessorOptions(consBuilder, groupBuilder)...,
		)
		require.NoError(t, err)
		var (
			procErr error
			done    = make(chan struct{})
		)

		cg.FailOnConsume(newErrSetup(123, fmt.Errorf("setup-error")))

		go func() {
			defer close(done)
			procErr = newProc.Run(ctx)
		}()

		newProc.WaitForReady()

		// if there was an error during startup, no point in sending messages
		// and waiting for them to be delivered
		<-done
		require.True(t, strings.Contains(procErr.Error(), "setup-error"))
	})
}

func TestProcessor_StateReader(t *testing.T) {
	state := NewSignal(ProcStateSetup, ProcStateRunning)
	state.SetState(ProcStateRunning)
	p := Processor{state: state}

	require.Equal(t, ProcStateRunning, p.StateReader().State())
}

func TestProcessor_Stop(t *testing.T) {
	t.Run("expected-state", func(t *testing.T) {
		ctrl, bm := createMockBuilder(t)
		defer ctrl.Finish()

		bm.tmgr.EXPECT().Close().Times(1)
		bm.tmgr.EXPECT().Partitions(gomock.Any()).Return([]int32{0}, nil).Times(1)
		bm.producer.EXPECT().Close().Times(1)

		groupBuilder, _ := createTestConsumerGroupBuilder(t)
		consBuilder, _ := createTestConsumerBuilder(t)

		graph := DefineGroup("test",
			Input("input", new(codec.Int64), func(ctx Context, msg interface{}) {
				// Do nothing
			}),
		)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000)
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
			bm.createProcessorOptions(consBuilder, groupBuilder)...,
		)
		require.NoError(t, err)
		var (
			procErr error
			done    = make(chan struct{})
		)

		go func() {
			defer close(done)
			procErr = newProc.Run(ctx)
		}()

		newProc.WaitForReady()

		// if there was an error during startup, no point in continuing
		// and waiting for the processor to be stopped
		select {
		case <-done:
			require.NoError(t, procErr)
		default:
		}

		require.Equal(t, ProcStateRunning, newProc.StateReader().State())

		// shutdown
		newProc.Stop()
		<-done

		select {
		case <-done:
			require.Equal(t, ProcStateStopped, newProc.StateReader().State())
			require.NoError(t, procErr)
		case <-time.After(10 * time.Second):
			t.Errorf("processor did not shut down as expected")
		}
	})
}

func TestProcessor_Done(t *testing.T) {
	t.Run("done-closes", func(t *testing.T) {
		ctrl, bm := createMockBuilder(t)
		defer ctrl.Finish()

		bm.tmgr.EXPECT().Close().Times(1)
		bm.tmgr.EXPECT().Partitions(gomock.Any()).Return([]int32{0}, nil).Times(1)
		bm.producer.EXPECT().Close().Times(1)

		groupBuilder, _ := createTestConsumerGroupBuilder(t)
		consBuilder, _ := createTestConsumerBuilder(t)

		graph := DefineGroup("test",
			Input("input", new(codec.Int64), func(ctx Context, msg interface{}) {
				// Do nothing
			}),
		)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000)
		defer cancel()

		newProc, err := NewProcessor([]string{"localhost:9092"}, graph,
			bm.createProcessorOptions(consBuilder, groupBuilder)...,
		)
		require.NoError(t, err)
		var (
			procErr error
		)

		go func() {
			procErr = newProc.Run(ctx)
		}()

		newProc.WaitForReady()

		// shutdown
		newProc.Stop()

		select {
		case <-newProc.Done():
			require.NoError(t, procErr)
		case <-time.After(10 * time.Second):
			t.Errorf("processor did not shut down as expected")
		}
	})
}
