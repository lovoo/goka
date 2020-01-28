package goka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/storage"
)

func defaultPT(
	t *testing.T,
	topic string,
	partition int32,
	consumer sarama.Consumer,
	updateCallback UpdateCallback,
) (*PartitionTable, *builderMock, *gomock.Controller) {

	ctrl := gomock.NewController(t)
	bm := newBuilderMock(ctrl)
	return newPartitionTable(
		topic,
		partition,
		consumer,
		bm.tmgr,
		updateCallback,
		bm.getStorageBuilder(),
		logger.Default(),
	), bm, ctrl
}

func TestPT_createStorage(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		var (
			partition int32          = 101
			callback  UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte) error {
				return nil
			}
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			partition,
			nil,
			callback,
		)
		defer ctrl.Finish()

		equalSP := &storageProxy{
			Storage:   bm.mst,
			partition: partition,
			update:    callback,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sp, err := pt.createStorage(ctx)
		assertNil(t, err)
		assertEqual(t, sp.Storage, equalSP.Storage)
		assertEqual(t, sp.partition, equalSP.partition)
		assertFuncEqual(t, sp.Update, equalSP.Update)
	})
	t.Run("fail_ctx_cancel", func(t *testing.T) {
		pt, _, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		pt.builder = errStorageBuilder()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		cancel()
		sp, err := pt.createStorage(ctx)
		assertNotNil(t, err)
		assertNil(t, sp)
	})
	t.Run("fail_storage", func(t *testing.T) {
		pt, _, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		pt.builder = errStorageBuilder()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()
		sp, err := pt.createStorage(ctx)
		assertNotNil(t, err)
		assertNil(t, sp)
	})
}

func TestPT_setup(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		pt, _, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		pt, _, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		pt.builder = errStorageBuilder()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()
		err := pt.setup(ctx)
		assertNotNil(t, err)
	})
}

func TestPT_close(t *testing.T) {
	t.Run("on_storage", func(t *testing.T) {
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.mst.EXPECT().Close().AnyTimes()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
		err = pt.Close()
		assertNil(t, err)
	})
	t.Run("on_nil_storage", func(t *testing.T) {
		pt, _, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()

		err := pt.Close()
		assertNil(t, err)
	})
}

func TestPT_findOffsetToLoad(t *testing.T) {
	t.Run("old_local", func(t *testing.T) {
		var (
			oldest int64 = 161
			newest int64 = 1312
			local  int64 = 15
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)

		actualOldest, actualNewest, err := pt.findOffsetToLoad(local)
		assertNil(t, err)
		assertEqual(t, actualOldest, oldest)
		assertEqual(t, actualNewest, newest)
	})
	t.Run("new_local", func(t *testing.T) {
		var (
			oldest int64 = 161
			newest int64 = 1312
			local  int64 = 175
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)

		actualOldest, actualNewest, err := pt.findOffsetToLoad(local)
		assertNil(t, err)
		assertEqual(t, actualOldest, local)
		assertEqual(t, actualNewest, newest)
	})
	t.Run("too_new_local", func(t *testing.T) {
		var (
			oldest int64 = 161
			newest int64 = 1312
			local  int64 = 161111
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)

		actualOldest, actualNewest, err := pt.findOffsetToLoad(local)
		assertNil(t, err)
		assertEqual(t, actualOldest, newest)
		assertEqual(t, actualNewest, newest)
	})
	t.Run("sarama_oldest", func(t *testing.T) {
		var (
			oldest int64 = 161
			newest int64 = 1312
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)

		actualOldest, actualNewest, err := pt.findOffsetToLoad(sarama.OffsetOldest)
		assertNil(t, err)
		assertEqual(t, actualOldest, oldest)
		assertEqual(t, actualNewest, newest)
	})
	t.Run("sarama_newest", func(t *testing.T) {
		var (
			oldest int64 = 161
			newest int64 = 1312
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)

		actualOldest, actualNewest, err := pt.findOffsetToLoad(sarama.OffsetNewest)
		assertNil(t, err)
		assertEqual(t, actualOldest, newest)
		assertEqual(t, actualNewest, newest)
	})
	t.Run("fail_getoffset", func(t *testing.T) {
		var (
			expectedErr error = fmt.Errorf("some error")
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(int64(0), expectedErr)

		_, _, err := pt.findOffsetToLoad(sarama.OffsetOldest)
		assertNotNil(t, err)
	})
	t.Run("fail_getoffset2", func(t *testing.T) {
		var (
			oldest      int64 = 161
			expectedErr error = fmt.Errorf("some error")
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(int64(0), expectedErr)

		_, _, err := pt.findOffsetToLoad(sarama.OffsetOldest)
		assertNotNil(t, err)
	})
}

func TestPT_load(t *testing.T) {
	t.Run("succeed_no_load_stopAfterCatchup", func(t *testing.T) {
		var (
			oldest           int64 = 161
			newest           int64 = 1312
			local            int64 = 1311
			stopAfterCatchup bool  = true
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.mst.EXPECT().GetOffset(sarama.OffsetOldest).Return(local, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)
		bm.mst.EXPECT().MarkRecovered().Return(nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
		err = pt.load(ctx, stopAfterCatchup)
		assertNil(t, err)
		assertTrue(t, pt.state.IsState(State(PartitionRunning)))
	})
	t.Run("fail_local_offset_too_high_stopAfterCatchup", func(t *testing.T) {
		var (
			oldest           int64 = 161
			newest           int64 = 1312
			local            int64 = 1314
			stopAfterCatchup bool  = true
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.mst.EXPECT().GetOffset(sarama.OffsetOldest).Return(local, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
		err = pt.load(ctx, stopAfterCatchup)
		assertNotNil(t, err)
	})
	t.Run("consume", func(t *testing.T) {
		var (
			oldest           int64          = 161
			newest           int64          = 1312
			local            int64          = sarama.OffsetOldest
			stopAfterCatchup bool           = false
			consumer         *MockConsumer  = defaultSaramaConsumerMock(t)
			topic            string         = "some-topic"
			partition        int32          = 0
			count            int32          = 0
			updateCB         UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte) error {
				count++
				return nil
			}
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			updateCB,
		)
		defer ctrl.Finish()
		pt.consumer = consumer
		bm.mst.EXPECT().GetOffset(gomock.Any()).Return(local, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)
		partConsumer := consumer.ExpectConsumePartition(topic, partition, AnyOffset)
		partConsumer.ExpectMessagesDrainedOnClose()
		for i := 0; i < 10; i++ {
			partConsumer.YieldMessage(&sarama.ConsumerMessage{})
			bm.mst.EXPECT().SetOffset(gomock.Any()).Return(nil)
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
				if count == 10 {
					cancel()
					return
				}
			}
		}()

		err := pt.setup(ctx)
		assertNil(t, err)
		err = pt.load(ctx, stopAfterCatchup)
		assertNil(t, err)
		assertTrue(t, count == 10)
	})
}

func TestPT_loadMessages(t *testing.T) {
	t.Run("consume_till_hwm", func(t *testing.T) {
		var (
			localOffset      int64         = sarama.OffsetOldest
			partitionHwm     int64         = 1
			stopAfterCatchup bool          = true
			topic            string        = "some-topic"
			partition        int32         = 0
			consumer         *MockConsumer = defaultSaramaConsumerMock(t)
			recKey           string
			recVal           []byte
			updateCB         UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte) error {
				recKey = key
				recVal = value
				return nil
			}
			key   string = "some-key"
			value []byte = []byte("some-vale")
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			updateCB,
		)
		defer ctrl.Finish()
		partConsumer := consumer.ExpectConsumePartition(topic, partition, localOffset)
		partConsumer.YieldMessage(&sarama.ConsumerMessage{
			Key:       []byte(key),
			Value:     value,
			Topic:     topic,
			Partition: partition,
			Offset:    partitionHwm,
		})
		partConsumer.ExpectMessagesDrainedOnClose()
		bm.mst.EXPECT().SetOffset(int64(0)).Return(nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
		err = pt.loadMessages(ctx, partConsumer, partitionHwm, stopAfterCatchup)
		assertNil(t, err)
		assertEqual(t, recKey, key)
		assertEqual(t, recVal, value)
	})
	t.Run("consume_till_hwm_more_msgs", func(t *testing.T) {
		var (
			localOffset      int64          = 0
			partitionHwm     int64          = 2
			stopAfterCatchup bool           = true
			topic            string         = "some-topic"
			partition        int32          = 0
			consumer         *MockConsumer  = defaultSaramaConsumerMock(t)
			updateCB         UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte) error {
				return nil
			}
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			updateCB,
		)
		defer ctrl.Finish()
		partConsumer := consumer.ExpectConsumePartition(topic, partition, localOffset)
		partConsumer.YieldMessage(&sarama.ConsumerMessage{
			Topic:     topic,
			Partition: partition,
			Offset:    1,
		})
		partConsumer.YieldMessage(&sarama.ConsumerMessage{
			Topic:     topic,
			Partition: partition,
			Offset:    1,
		})
		partConsumer.ExpectMessagesDrainedOnClose()
		bm.mst.EXPECT().SetOffset(int64(0)).Return(nil)
		bm.mst.EXPECT().SetOffset(int64(1)).Return(nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
		err = pt.loadMessages(ctx, partConsumer, partitionHwm, stopAfterCatchup)
		assertNil(t, err)
	})
	t.Run("consume_till_cancel", func(t *testing.T) {
		var (
			localOffset      int64          = 0
			partitionHwm     int64          = 2
			stopAfterCatchup bool           = false
			topic            string         = "some-topic"
			partition        int32          = 0
			consumer         *MockConsumer  = defaultSaramaConsumerMock(t)
			count            int32          = 0
			updateCB         UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte) error {
				count++
				return nil
			}
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			updateCB,
		)
		defer ctrl.Finish()
		partConsumer := consumer.ExpectConsumePartition(topic, partition, localOffset)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
		go func(ctx context.Context) {
			for i := 0; i < 100; i++ {
				bm.mst.EXPECT().SetOffset(gomock.Any()).Return(nil)
				partConsumer.YieldMessage(&sarama.ConsumerMessage{})
			}
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if count == 100 {
					break
				}
			}
			cancel()
		}(ctx)
		err = pt.loadMessages(ctx, partConsumer, partitionHwm, stopAfterCatchup)
		assertNil(t, err)
		assertTrue(t, count == 100)
	})
	t.Run("close_msg_chan", func(t *testing.T) {
		var (
			localOffset      int64          = 0
			partitionHwm     int64          = 2
			stopAfterCatchup bool           = false
			topic            string         = "some-topic"
			partition        int32          = 0
			consumer         *MockConsumer  = defaultSaramaConsumerMock(t)
			updateCB         UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte) error {
				return nil
			}
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			updateCB,
		)
		defer ctrl.Finish()
		partConsumer := consumer.ExpectConsumePartition(topic, partition, localOffset)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := pt.setup(ctx)
		assertNil(t, err)
		go func() {
			defer cancel()
			err = pt.loadMessages(ctx, partConsumer, partitionHwm, stopAfterCatchup)
			assertNil(t, err)
		}()
		go func(ctx context.Context) {
			lock := sync.Mutex{}
			open := true
			go func() {
				lock.Lock()
				defer lock.Unlock()
				partConsumer.AsyncClose()
				open = false
			}()
			for i := 0; i < 100; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				lock.Lock()
				if open {
					bm.mst.EXPECT().SetOffset(gomock.Any()).Return(nil)
					partConsumer.YieldMessage(&sarama.ConsumerMessage{})
				}
				lock.Unlock()
			}
		}(ctx)
		<-ctx.Done()
	})
	t.Run("stalled", func(t *testing.T) {
		var (
			localOffset      int64         = 0
			partitionHwm     int64         = 2
			stopAfterCatchup bool          = false
			topic            string        = "some-topic"
			partition        int32         = 0
			consumer         *MockConsumer = defaultSaramaConsumerMock(t)
		)
		pt, _, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			nil,
		)
		defer ctrl.Finish()
		pt.stalledTimeout = time.Duration(0)
		pt.stallPeriod = time.Nanosecond

		partConsumer := consumer.ExpectConsumePartition(topic, partition, localOffset)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := pt.setup(ctx)
		assertNil(t, err)
		go func() {
			defer cancel()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if pt.stats.Stalled {
					return
				}
			}
		}()
		err = pt.loadMessages(ctx, partConsumer, partitionHwm, stopAfterCatchup)
		assertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		var (
			localOffset      int64          = 0
			partitionHwm     int64          = 2
			stopAfterCatchup bool           = true
			topic            string         = "some-topic"
			partition        int32          = 0
			consumer         *MockConsumer  = defaultSaramaConsumerMock(t)
			retErr           error          = fmt.Errorf("update error")
			updateCB         UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte) error {
				return retErr
			}
		)
		pt, _, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			updateCB,
		)
		defer ctrl.Finish()
		partConsumer := consumer.ExpectConsumePartition(topic, partition, localOffset)
		partConsumer.YieldMessage(&sarama.ConsumerMessage{
			Topic:     topic,
			Partition: partition,
			Offset:    1,
		})
		partConsumer.ExpectMessagesDrainedOnClose()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
		err = pt.loadMessages(ctx, partConsumer, partitionHwm, stopAfterCatchup)
		assertNotNil(t, err)
	})
}

func TestPT_storeEvent(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		var (
			localOffset int64  = 0
			partition   int32  = 0
			topic       string = "some-topic"
			key         string = "some-key"
			value       []byte = []byte("some-vale")
			actualKey   string
			actualValue []byte
			updateCB    UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte) error {
				actualKey = key
				actualValue = value
				return nil
			}
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			updateCB,
		)
		defer ctrl.Finish()
		bm.mst.EXPECT().SetOffset(localOffset).Return(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
		err = pt.storeEvent(key, value, localOffset)
		assertEqual(t, actualKey, key)
		assertEqual(t, actualValue, value)
		assertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		var (
			localOffset int64          = 0
			partition   int32          = 0
			topic       string         = "some-topic"
			key         string         = "some-key"
			value       []byte         = []byte("some-vale")
			updateCB    UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte) error {
				return nil
			}
			retErr error = fmt.Errorf("storage err")
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			updateCB,
		)
		defer ctrl.Finish()
		bm.mst.EXPECT().SetOffset(localOffset).Return(retErr)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
		err = pt.storeEvent(key, value, localOffset)
		assertNotNil(t, err)
	})
}

func TestPT_Close(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		var (
			partition int32  = 0
			topic     string = "some-topic"
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.mst.EXPECT().Close().Return(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
		err = pt.Close()
		assertNil(t, err)
	})
	t.Run("succeed2", func(t *testing.T) {
		var (
			partition int32  = 0
			topic     string = "some-topic"
		)
		pt, _, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			nil,
		)
		defer ctrl.Finish()
		err := pt.Close()
		assertNil(t, err)
	})
}

func TestPT_markRecovered(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		var (
			partition int32  = 0
			topic     string = "some-topic"
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.mst.EXPECT().MarkRecovered().Return(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
		assertTrue(t, !pt.state.IsState(State(PartitionRunning)))
		err = pt.markRecovered(ctx)
		assertNil(t, err)
		assertTrue(t, pt.state.IsState(State(PartitionRunning)))
	})
	t.Run("fail", func(t *testing.T) {
		var (
			partition int32  = 0
			topic     string = "some-topic"
			retErr    error  = fmt.Errorf("store error")
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.mst.EXPECT().MarkRecovered().Return(retErr)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		assertNil(t, err)
		err = pt.markRecovered(ctx)
		assertNotNil(t, err)
		assertTrue(t, pt.state.IsState(State(PartitionPreparing)))
	})
}

func TestPT_SetupAndCatchupToHwm(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		var (
			oldest    int64          = 161
			newest    int64          = 1312
			local     int64          = oldest
			consumer  *MockConsumer  = defaultSaramaConsumerMock(t)
			topic     string         = "some-topic"
			partition int32          = 0
			count     int32          = 0
			updateCB  UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte) error {
				count++
				return nil
			}
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			updateCB,
		)
		defer ctrl.Finish()
		pt.consumer = consumer
		bm.mst.EXPECT().GetOffset(gomock.Any()).Return(local, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)
		bm.mst.EXPECT().MarkRecovered().Return(nil)
		partConsumer := consumer.ExpectConsumePartition(topic, partition, oldest)
		partConsumer.ExpectMessagesDrainedOnClose()
		for i := 0; i < 10; i++ {
			partConsumer.YieldMessage(&sarama.ConsumerMessage{})
			bm.mst.EXPECT().SetOffset(gomock.Any()).Return(nil)
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
				if count == 10 {
					cancel()
					return
				}
			}
		}()

		err := pt.SetupAndCatchup(ctx)
		assertNil(t, err)
		assertTrue(t, count == 10)
	})
	t.Run("fail", func(t *testing.T) {
		var (
			consumer  *MockConsumer = defaultSaramaConsumerMock(t)
			topic     string        = "some-topic"
			partition int32         = 0
			retErr    error         = fmt.Errorf("offset-error")
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			nil,
		)
		defer ctrl.Finish()
		pt.consumer = consumer
		bm.mst.EXPECT().GetOffset(gomock.Any()).Return(int64(0), retErr)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := pt.SetupAndCatchup(ctx)
		assertNotNil(t, err)
	})
}

func TestPT_SetupAndCatchupForever(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		var (
			oldest int64 = 0
			newest int64 = 10
			// local     int64          = oldest
			consumer  *MockConsumer  = defaultSaramaConsumerMock(t)
			topic     string         = "some-topic"
			partition int32          = 0
			count     int64          = 0
			updateCB  UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte) error {
				count++
				return nil
			}
			restartOnError bool = false
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			updateCB,
		)
		defer ctrl.Finish()
		bm.useMemoryStorage()
		pt.consumer = consumer
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil).AnyTimes()
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil).AnyTimes()
		partConsumer := consumer.ExpectConsumePartition(topic, partition, AnyOffset)
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
				if count == 10 {
					time.Sleep(time.Millisecond * 10)
					cancel()
					return
				}
			}
		}()

		recovered, errChan := pt.SetupAndCatchupForever(ctx, restartOnError)
		var isRecovered bool
		select {
		case <-recovered:
			isRecovered = true
			cancel()
		case <-ctx.Done():
		}
		assertNil(t, <-errChan)
		assertTrue(t, isRecovered)
	})
	t.Run("fail", func(t *testing.T) {
		var (
			consumer       *MockConsumer = defaultSaramaConsumerMock(t)
			topic          string        = "some-topic"
			partition      int32         = 0
			retErr         error         = fmt.Errorf("offset-error")
			restartOnError bool          = false
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			nil,
		)
		defer ctrl.Finish()
		pt.consumer = consumer
		bm.mst.EXPECT().GetOffset(gomock.Any()).Return(int64(0), retErr)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		recoveredCh, errCh := pt.SetupAndCatchupForever(ctx, restartOnError)
		select {
		case <-recoveredCh:
		case <-ctx.Done():
		}
		assertNotNil(t, <-errCh)
	})
}
