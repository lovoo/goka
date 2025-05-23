package goka

import (
	"context"
	"fmt"
	"hash"
	"log"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	viewTestRecoveredMessages int
	viewTestGroup             Group = "group-name"
	viewTestTopic                   = tableName(viewTestGroup)
)

// constHasher implements a hasher that will always return the specified
// partition. Doesn't properly implement the Hash32 interface, use only in
// tests.
type constHasher struct {
	partition uint32
	returnErr bool
}

func (ch *constHasher) Sum(b []byte) []byte {
	return nil
}

func (ch *constHasher) Sum32() uint32 {
	return ch.partition
}

func (ch *constHasher) BlockSize() int {
	return 0
}

func (ch *constHasher) Reset() {}

func (ch *constHasher) Size() int { return 4 }

func (ch *constHasher) Write(p []byte) (int, error) {
	if ch.returnErr {
		return 0, fmt.Errorf("constHasher write error")
	}
	return len(p), nil
}

func (ch *constHasher) ReturnError() {
	ch.returnErr = true
}

// newConstHasher creates a constant hasher that hashes any value to 0.
func newConstHasher(part uint32) *constHasher {
	return &constHasher{partition: part}
}

func createTestView(t *testing.T, consumer sarama.Consumer) (*View, *builderMock, *gomock.Controller) {
	ctrl := gomock.NewController(t)
	bm := newBuilderMock(ctrl)
	viewTestRecoveredMessages = 0
	opts := &voptions{
		log:        defaultLogger,
		tableCodec: new(codec.String),
		updateCallback: func(ctx UpdateContext, s storage.Storage, key string, value []byte) error {
			if err := DefaultUpdate(ctx, s, key, value); err != nil {
				return err
			}
			viewTestRecoveredMessages++
			return nil
		},
		hasher: DefaultHasher(),
	}
	opts.builders.storage = bm.getStorageBuilder()
	opts.builders.topicmgr = bm.getTopicManagerBuilder()
	opts.builders.consumerSarama = func(brokers []string, clientID string) (sarama.Consumer, error) {
		return consumer, nil
	}
	opts.builders.backoff = DefaultBackoffBuilder

	view := &View{
		topic: viewTestTopic,
		opts:  opts,
		log:   opts.log,
		state: newViewSignal(),
	}
	return view, bm, ctrl
}

func TestView_hash(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		view, _, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		view.partitions = []*PartitionTable{{}}

		h, err := view.hash("a")
		require.NoError(t, err)
		require.True(t, h == 0)
	})
	t.Run("fail_hash", func(t *testing.T) {
		view, _, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		view.partitions = []*PartitionTable{{}}
		view.opts.hasher = func() hash.Hash32 {
			hasher := newConstHasher(0)
			hasher.ReturnError()
			return hasher
		}

		_, err := view.hash("a")
		require.Error(t, err)
	})
	t.Run("fail_no_partition", func(t *testing.T) {
		view, _, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		_, err := view.hash("a")
		require.Error(t, err)
	})
}

func TestView_find(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		view, _, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			key   = "some-key"
			proxy = &storageProxy{}
		)
		view.partitions = []*PartitionTable{
			{
				st:    proxy,
				state: newPartitionTableState().SetState(State(PartitionRunning)),
			},
		}
		view.opts.hasher = func() hash.Hash32 {
			hasher := newConstHasher(0)
			return hasher
		}

		st, err := view.find(key)
		require.NoError(t, err)
		require.Equal(t, st, view.partitions[0])
	})
	t.Run("fail", func(t *testing.T) {
		view, _, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		view.partitions = []*PartitionTable{{}}
		view.opts.hasher = func() hash.Hash32 {
			hasher := newConstHasher(0)
			hasher.ReturnError()
			return hasher
		}

		_, err := view.find("a")
		require.Error(t, err)
	})
}

func TestView_Get(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			proxy = &storageProxy{
				Storage:   bm.mst,
				partition: 0,
				update:    updateCallbackNoop,
			}
			key         = "some-key"
			value int64 = 3
		)
		view.partitions = []*PartitionTable{
			{
				st:    proxy,
				state: newPartitionTableState().SetState(State(PartitionRunning)),
			},
		}
		view.opts.tableCodec = &codec.Int64{}
		view.state.SetState(State(ViewStateRunning))

		bm.mst.EXPECT().Get(key).Return([]byte(strconv.FormatInt(value, 10)), nil)

		ret, err := view.Get(key)
		require.NoError(t, err)
		require.True(t, ret == value)
	})
	t.Run("succeed_nil", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			proxy = &storageProxy{
				Storage:   bm.mst,
				partition: 0,
				update:    updateCallbackNoop,
			}
			key = "some-key"
		)
		view.partitions = []*PartitionTable{
			{
				st:    proxy,
				state: newPartitionTableState().SetState(State(PartitionRunning)),
			},
		}
		view.opts.tableCodec = &codec.Int64{}
		view.state.SetState(State(ViewStateRunning))
		bm.mst.EXPECT().Get(key).Return(nil, nil)

		ret, err := view.Get(key)
		require.NoError(t, err)
		require.True(t, ret == nil)
	})
	t.Run("fail_get", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			proxy = &storageProxy{
				Storage:   bm.mst,
				partition: 0,
				update:    updateCallbackNoop,
			}
			key          = "some-key"
			errRet error = fmt.Errorf("get failed")
		)
		view.partitions = []*PartitionTable{
			{
				st:    proxy,
				state: newPartitionTableState().SetState(State(PartitionRunning)),
			},
		}
		view.opts.tableCodec = &codec.Int64{}
		view.state.SetState(State(ViewStateRunning))
		bm.mst.EXPECT().Get(key).Return(nil, errRet)

		_, err := view.Get(key)
		require.Error(t, err)
	})
}

func TestView_Has(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			proxy = &storageProxy{
				Storage: bm.mst,
			}
			key = "some-key"
			has = true
		)
		view.partitions = []*PartitionTable{
			{
				st:    proxy,
				state: newPartitionTableState().SetState(State(PartitionRunning)),
			},
		}
		view.opts.hasher = func() hash.Hash32 {
			hasher := newConstHasher(0)
			return hasher
		}

		bm.mst.EXPECT().Has(key).Return(has, nil)

		ret, err := view.Has(key)
		require.NoError(t, err)
		require.Equal(t, ret, has)
	})
	t.Run("succeed_false", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			proxy = &storageProxy{
				Storage: bm.mst,
			}
			key = "some-key"
			has = false
		)
		view.partitions = []*PartitionTable{
			{
				st:    proxy,
				state: newPartitionTableState().SetState(State(PartitionRunning)),
			},
		}
		view.opts.hasher = func() hash.Hash32 {
			hasher := newConstHasher(0)
			return hasher
		}

		bm.mst.EXPECT().Has(key).Return(has, nil)

		ret, err := view.Has(key)
		require.NoError(t, err)
		require.Equal(t, ret, has)
	})
	t.Run("fail_err", func(t *testing.T) {
		view, _, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			key = "some-key"
			has = false
		)
		view.partitions = []*PartitionTable{{}}
		view.opts.hasher = func() hash.Hash32 {
			hasher := newConstHasher(0)
			hasher.ReturnError()
			return hasher
		}

		ret, err := view.Has(key)
		require.Error(t, err)
		require.True(t, ret == has)
	})
}

func TestView_Evict(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			key   = "some-key"
			proxy = &storageProxy{
				Storage: bm.mst,
			}
		)
		view.partitions = []*PartitionTable{
			{st: proxy},
		}
		view.opts.hasher = func() hash.Hash32 {
			hasher := newConstHasher(0)
			return hasher
		}
		bm.mst.EXPECT().Delete(key).Return(nil)

		err := view.Evict(key)
		require.NoError(t, err)
	})
}

func TestView_Recovered(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		view, _, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		hasRecovered := true
		view.partitions = []*PartitionTable{
			{state: NewSignal(State(PartitionRunning)).SetState(State(PartitionRunning))},
		}
		ret := view.Recovered()
		require.True(t, ret == hasRecovered)
	})
	t.Run("true", func(t *testing.T) {
		view, _, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		hasRecovered := false
		view.partitions = []*PartitionTable{
			{state: NewSignal(State(PartitionRunning), State(PartitionRecovering)).SetState(State(PartitionRecovering))},
			{state: NewSignal(State(PartitionRunning)).SetState(State(PartitionRunning))},
		}
		ret := view.Recovered()
		require.True(t, ret == hasRecovered)
	})
}

func TestView_Topic(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		view, _, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		ret := view.Topic()
		require.True(t, ret == viewTestTopic)
	})
}

func TestView_close(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		proxy := &storageProxy{
			Storage: bm.mst,
		}
		view.partitions = []*PartitionTable{
			{st: proxy},
			{st: proxy},
			{st: proxy},
		}
		bm.mst.EXPECT().Close().Return(nil).AnyTimes()

		err := view.close()
		require.NoError(t, err)
		require.True(t, len(view.partitions) == 0)
	})
	t.Run("fail", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			proxy = &storageProxy{
				Storage: bm.mst,
			}
			retErr error = fmt.Errorf("some-error")
		)
		view.partitions = []*PartitionTable{
			{st: proxy},
			{st: proxy},
			{st: proxy},
		}
		bm.mst.EXPECT().Close().Return(retErr).AnyTimes()

		err := view.close()
		require.Error(t, err)
		require.True(t, len(view.partitions) == 0)
	})
}

func TestView_Terminate(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			proxy = &storageProxy{
				Storage: bm.mst,
			}
			isAutoReconnect = true
		)
		view.partitions = []*PartitionTable{
			{st: proxy},
			{st: proxy},
			{st: proxy},
		}
		view.opts.autoreconnect = isAutoReconnect
		bm.mst.EXPECT().Close().Return(nil).AnyTimes()

		ret := view.close()
		require.NoError(t, ret)
		require.True(t, len(view.partitions) == 0)
	})

	t.Run("succeed_twice", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			proxy = &storageProxy{
				Storage: bm.mst,
			}
			isAutoReconnect = true
		)
		view.partitions = []*PartitionTable{
			{st: proxy},
			{st: proxy},
			{st: proxy},
		}
		view.opts.autoreconnect = isAutoReconnect
		bm.mst.EXPECT().Close().Return(nil).AnyTimes()

		ret := view.close()
		require.NoError(t, ret)
		require.True(t, len(view.partitions) == 0)
		ret = view.close()
		require.NoError(t, ret)
		require.True(t, len(view.partitions) == 0)
	})

	t.Run("fail", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			proxy = &storageProxy{
				Storage: bm.mst,
			}
			retErr          error = fmt.Errorf("some-error")
			isAutoReconnect       = true
		)
		view.partitions = []*PartitionTable{
			{st: proxy},
			{st: proxy},
			{st: proxy},
		}
		view.opts.autoreconnect = isAutoReconnect
		bm.mst.EXPECT().Close().Return(retErr).AnyTimes()

		ret := view.close()
		require.Error(t, ret)
		require.True(t, len(view.partitions) == 0)
	})
}

func TestView_Run(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			oldest int64
			newest int64 = 10
			// local     int64          = oldest
			consumer  = defaultSaramaAutoConsumerMock(t)
			partition int32
			count     int64
			updateCB  UpdateCallback = func(ctx UpdateContext, s storage.Storage, key string, value []byte) error {
				atomic.AddInt64(&count, 1)
				return nil
			}
		)
		bm.useMemoryStorage()

		pt := newPartitionTable(
			viewTestTopic,
			partition,
			consumer,
			bm.tmgr,
			updateCB,
			bm.getStorageBuilder(),
			defaultLogger,
			NewSimpleBackoff(defaultBackoffStep, defaultBackoffMax),
			time.Minute,
		)

		pt.consumer = consumer
		view.partitions = []*PartitionTable{pt}
		view.state = newViewSignal()

		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil).AnyTimes()
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil).AnyTimes()
		partConsumer := consumer.ExpectConsumePartition(viewTestTopic, partition, anyOffset)
		for i := 0; i < 10; i++ {
			partConsumer.YieldMessage(&sarama.ConsumerMessage{})
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if atomic.LoadInt64(&count) == 10 {
					time.Sleep(time.Millisecond * 10)
					cancel()
					return
				}
			}
		}()

		ret := view.Run(ctx)
		require.NoError(t, ret)
	})
	t.Run("fail", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var (
			partition int32
			consumer  = defaultSaramaAutoConsumerMock(t)
			updateCB  UpdateCallback
			retErr    = fmt.Errorf("run error")
		)
		bm.useMemoryStorage()

		pt := newPartitionTable(
			viewTestTopic,
			partition,
			consumer,
			bm.tmgr,
			updateCB,
			bm.getStorageBuilder(),
			defaultLogger,
			NewSimpleBackoff(defaultBackoffStep, defaultBackoffMax),
			time.Minute,
		)

		pt.consumer = consumer
		view.partitions = []*PartitionTable{pt}
		view.state = newViewSignal()

		bm.mst.EXPECT().GetOffset(gomock.Any()).Return(int64(0), retErr).AnyTimes()
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(sarama.OffsetNewest, retErr).AnyTimes()
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(sarama.OffsetOldest, retErr).AnyTimes()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		ret := view.Run(ctx)
		require.Error(t, ret)
	})
}

func TestView_createPartitions(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var partition int32
		bm.tmgr.EXPECT().Partitions(viewTestTopic).Return([]int32{partition}, nil)
		bm.tmgr.EXPECT().Close()

		ret := view.createPartitions([]string{""})
		require.NoError(t, ret)
		require.True(t, len(view.partitions) == 1)
	})
	t.Run("fail_tmgr", func(t *testing.T) {
		view, bm, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		var retErr error = fmt.Errorf("tmgr-partition-error")
		bm.tmgr.EXPECT().Partitions(viewTestTopic).Return(nil, retErr)
		bm.tmgr.EXPECT().Close()

		ret := view.createPartitions([]string{""})
		require.Error(t, ret)
		require.True(t, len(view.partitions) == 0)
	})
}

func TestView_WaitRunning(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		view, _, ctrl := createTestView(t, NewMockAutoConsumer(t, DefaultConfig()))
		defer ctrl.Finish()

		view.state = newViewSignal().SetState(State(ViewStateRunning))

		var isRunning bool
		select {
		case <-view.WaitRunning():
			isRunning = true
		case <-time.After(time.Second):
		}

		require.True(t, isRunning == true)
	})
}

func TestView_NewView(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		bm := newBuilderMock(ctrl)

		var partition int32
		bm.tmgr.EXPECT().Partitions(viewTestTopic).Return([]int32{partition}, nil).AnyTimes()
		bm.tmgr.EXPECT().Close().AnyTimes()

		view, err := NewView([]string{""}, Table(viewTestTopic), &codec.Int64{}, []ViewOption{
			WithViewTopicManagerBuilder(bm.getTopicManagerBuilder()),
			WithViewConsumerSaramaBuilder(func(brokers []string, clientID string) (sarama.Consumer, error) {
				return NewMockAutoConsumer(t, DefaultConfig()), nil
			}),
		}...)
		require.NoError(t, err)
		require.NotNil(t, view)
	})
	t.Run("succeed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		bm := newBuilderMock(ctrl)

		var retErr error = fmt.Errorf("tmgr-error")
		bm.tmgr.EXPECT().Partitions(viewTestTopic).Return(nil, retErr).AnyTimes()
		bm.tmgr.EXPECT().Close().AnyTimes()

		view, err := NewView([]string{""}, Table(viewTestTopic), &codec.Int64{}, []ViewOption{
			WithViewTopicManagerBuilder(bm.getTopicManagerBuilder()),
			WithViewConsumerSaramaBuilder(func(brokers []string, clientID string) (sarama.Consumer, error) {
				return NewMockAutoConsumer(t, DefaultConfig()), nil
			}),
		}...)
		require.Error(t, err)
		require.Nil(t, view)
	})
}

// This example shows how views are typically created and used
// in the most basic way.
func ExampleView() {
	// create a new view
	view, err := NewView([]string{"localhost:9092"},
		"input-topic",
		new(codec.String))
	if err != nil {
		log.Fatalf("error creating view: %v", err)
	}

	// provide a cancelable
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start the view
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := view.Run(ctx)
		if err != nil {
			log.Fatalf("Error running view: %v", err)
		}
	}()

	// wait for the view to be recovered

	// Option A: by polling
	for !view.Recovered() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}

	// Option B: by waiting for the signal
	<-view.WaitRunning()

	// retrieve a value from the view
	val, err := view.Get("some-key")
	if err != nil {
		log.Fatalf("Error getting item from view: %v", err)
	}

	if val != nil {
		// cast it to string
		// no need for type assertion, if it was not that type, the codec would've failed
		log.Printf("got value %s", val.(string))
	}

	has, err := view.Has("some-key")
	if err != nil {
		log.Fatalf("Error getting item from view: %v", err)
	}

	_ = has

	// stop the view and wait for it to shut down before returning
	cancel()
	<-done
}

func ExampleView_autoreconnect() {
	// create a new view
	view, err := NewView([]string{"localhost:9092"},
		"input-topic",
		new(codec.String),

		// Automatically reconnect in case of errors. This is useful for services where availability
		// is more important than the data being up to date in case of kafka connection issues.
		WithViewAutoReconnect(),

		// Reconnect uses a default backoff mechanism, that can be modified by providing
		// a custom backoff builder using
		// WithViewBackoffBuilder(customBackoffBuilder),

		// When the view is running successfully for some time, the backoff is reset.
		// This time range can be modified using
		// WithViewBackoffResetTimeout(3*time.Second),
	)
	if err != nil {
		log.Fatalf("error creating view: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// start the view
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := view.Run(ctx)
		if err != nil {
			log.Fatalf("Error running view: %v", err)
		}
	}()

	<-view.WaitRunning()
	// at this point we can safely use the view with Has/Get/Iterate,
	// even if the kafka connection is lost

	// Stop the view and wait for it to shutdown before returning
	cancel()
	<-done
}
