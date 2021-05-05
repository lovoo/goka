package goka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/lovoo/goka/internal/test"
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
		NewSimpleBackoff(time.Second*10),
		time.Minute,
	), bm, ctrl
}

func TestPT_createStorage(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		var (
			partition int32          = 101
			callback  UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte, headers ...*sarama.RecordHeader) error {
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

		bm.mst.EXPECT().Open().Return(nil)
		sp, err := pt.createStorage(ctx)
		test.AssertNil(t, err)
		test.AssertEqual(t, sp.Storage, equalSP.Storage)
		test.AssertEqual(t, sp.partition, equalSP.partition)
		test.AssertFuncEqual(t, sp.Update, equalSP.Update)
	})
	t.Run("fail_ctx_cancel", func(t *testing.T) {
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cancel()

		bm.mst.EXPECT().Open().Return(nil)
		bm.mst.EXPECT().Close().Return(nil)

		sp, err := pt.createStorage(ctx)
		test.AssertNil(t, err)
		test.AssertNil(t, sp)
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
		test.AssertNotNil(t, err)
		test.AssertNil(t, sp)
	})
}

func TestPT_setup(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		bm.mst.EXPECT().Open().Return(nil)
		err := pt.setup(ctx)
		test.AssertNil(t, err)
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
		test.AssertNotNil(t, err)
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
		bm.mst.EXPECT().Open().Return(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		test.AssertNil(t, err)
		err = pt.Close()
		test.AssertNil(t, err)
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
		test.AssertNil(t, err)
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
		test.AssertNil(t, err)
		test.AssertEqual(t, actualOldest, oldest)
		test.AssertEqual(t, actualNewest, newest)
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

		offsetToLoad, actualNewest, err := pt.findOffsetToLoad(local)
		test.AssertNil(t, err)
		test.AssertEqual(t, offsetToLoad, local+1)
		test.AssertEqual(t, actualNewest, newest)
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

		offsetToLoad, actualNewest, err := pt.findOffsetToLoad(local)
		test.AssertNil(t, err)
		test.AssertEqual(t, offsetToLoad, local+1)
		test.AssertEqual(t, actualNewest, newest)
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
		test.AssertNil(t, err)
		test.AssertEqual(t, actualOldest, oldest)
		test.AssertEqual(t, actualNewest, newest)
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
		test.AssertNotNil(t, err)
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
		test.AssertNotNil(t, err)
	})
}

func TestPT_load(t *testing.T) {
	t.Run("succeed_no_load_stopAfterCatchup", func(t *testing.T) {
		var (
			oldest           int64 = 161
			newest           int64 = 1312
			local            int64 = 1311
			stopAfterCatchup       = true
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.mst.EXPECT().Open().Return(nil)
		bm.mst.EXPECT().GetOffset(offsetNotStored).Return(local, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)
		bm.mst.EXPECT().MarkRecovered().Return(nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		test.AssertNil(t, err)
		err = pt.load(ctx, stopAfterCatchup)
		test.AssertNil(t, err)
		test.AssertTrue(t, pt.state.IsState(State(PartitionRunning)))
	})
	t.Run("local_offset_too_high_stopAfterCatchup_no_error", func(t *testing.T) {
		var (
			oldest           int64 = 161
			newest           int64 = 1312
			local            int64 = 1314
			stopAfterCatchup       = true
		)
		pt, bm, ctrl := defaultPT(
			t,
			"some-topic",
			0,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.mst.EXPECT().Open().Return(nil)
		bm.mst.EXPECT().GetOffset(offsetNotStored).Return(local, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)
		bm.mst.EXPECT().MarkRecovered().Return(nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		test.AssertNil(t, err)
		err = pt.load(ctx, stopAfterCatchup)
		test.AssertNil(t, err)
	})
	t.Run("consume", func(t *testing.T) {
		var (
			oldest           int64 = 161
			newest           int64 = 1312
			local            int64 = sarama.OffsetOldest
			stopAfterCatchup       = false
			consumer               = defaultSaramaAutoConsumerMock(t)
			topic                  = "some-topic"
			partition        int32
			count            int64
			updateCB         UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte, headers ...*sarama.RecordHeader) error {
				atomic.AddInt64(&count, 1)
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
		bm.mst.EXPECT().Open().Return(nil)
		bm.mst.EXPECT().GetOffset(gomock.Any()).Return(local, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)
		partConsumer := consumer.ExpectConsumePartition(topic, partition, anyOffset)
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
				if atomic.LoadInt64(&count) == 10 {
					cancel()
					return
				}
			}
		}()

		err := pt.setup(ctx)
		test.AssertNil(t, err)
		err = pt.load(ctx, stopAfterCatchup)
		test.AssertNil(t, err)
		test.AssertTrue(t, atomic.LoadInt64(&count) == 10)
	})
}

func TestPT_loadMessages(t *testing.T) {
	t.Run("consume_till_hwm", func(t *testing.T) {
		var (
			localOffset      int64 = sarama.OffsetOldest
			partitionHwm     int64 = 1
			stopAfterCatchup       = true
			topic                  = "some-topic"
			partition        int32
			consumer         = defaultSaramaAutoConsumerMock(t)
			recKey           string
			recVal           []byte
			updateCB         UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte, headers ...*sarama.RecordHeader) error {
				recKey = key
				recVal = value
				return nil
			}
			key   = "some-key"
			value = []byte("some-vale")
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
		bm.mst.EXPECT().Open().Return(nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		test.AssertNil(t, err)
		err = pt.loadMessages(ctx, partConsumer, partitionHwm, stopAfterCatchup)
		test.AssertNil(t, err)
		test.AssertEqual(t, recKey, key)
		test.AssertEqual(t, recVal, value)
	})
	t.Run("consume_till_hwm_more_msgs", func(t *testing.T) {
		var (
			localOffset      int64
			partitionHwm     int64 = 2
			stopAfterCatchup       = true
			topic                  = "some-topic"
			partition        int32
			consumer                        = defaultSaramaAutoConsumerMock(t)
			updateCB         UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte, headers ...*sarama.RecordHeader) error {
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
		bm.mst.EXPECT().Open().Return(nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		test.AssertNil(t, err)
		err = pt.loadMessages(ctx, partConsumer, partitionHwm, stopAfterCatchup)
		test.AssertNil(t, err)
	})
	t.Run("consume_till_cancel", func(t *testing.T) {
		var (
			localOffset      int64
			partitionHwm     int64 = 2
			stopAfterCatchup       = false
			topic                  = "some-topic"
			partition        int32
			consumer         = defaultSaramaAutoConsumerMock(t)
			count            int64
			updateCB         UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte, headers ...*sarama.RecordHeader) error {
				atomic.AddInt64(&count, 1)
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
		bm.mst.EXPECT().Open().Return(nil)
		err := pt.setup(ctx)
		test.AssertNil(t, err)
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
				if atomic.LoadInt64(&count) == 100 {
					break
				}
			}
			cancel()
		}(ctx)
		err = pt.loadMessages(ctx, partConsumer, partitionHwm, stopAfterCatchup)
		test.AssertNil(t, err)
		test.AssertTrue(t, atomic.LoadInt64(&count) == 100)
	})
	t.Run("close_msg_chan", func(t *testing.T) {
		var (
			localOffset      int64
			partitionHwm     int64 = 2
			stopAfterCatchup       = false
			topic                  = "some-topic"
			partition        int32
			consumer                        = defaultSaramaAutoConsumerMock(t)
			updateCB         UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte, headers ...*sarama.RecordHeader) error {
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
		bm.mst.EXPECT().Open().Return(nil)
		err := pt.setup(ctx)
		test.AssertNil(t, err)
		go func() {
			defer cancel()
			err = pt.loadMessages(ctx, partConsumer, partitionHwm, stopAfterCatchup)
			test.AssertNil(t, err)
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
			localOffset      int64
			partitionHwm     int64 = 2
			stopAfterCatchup       = false
			topic                  = "some-topic"
			partition        int32
			consumer         = defaultSaramaAutoConsumerMock(t)
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			nil,
		)
		defer ctrl.Finish()
		pt.stalledTimeout = time.Duration(0)
		pt.stallPeriod = time.Nanosecond

		bm.mst.EXPECT().Open().Return(nil)
		partConsumer := consumer.ExpectConsumePartition(topic, partition, localOffset)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := pt.setup(ctx)
		test.AssertNil(t, err)
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
		test.AssertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		var (
			localOffset      int64
			partitionHwm     int64 = 2
			stopAfterCatchup       = true
			topic                  = "some-topic"
			partition        int32
			consumer                        = defaultSaramaAutoConsumerMock(t)
			retErr           error          = fmt.Errorf("update error")
			updateCB         UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte, headers ...*sarama.RecordHeader) error {
				return retErr
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
		bm.mst.EXPECT().Open().Return(nil)
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
		test.AssertNil(t, err)
		err = pt.loadMessages(ctx, partConsumer, partitionHwm, stopAfterCatchup)
		test.AssertNotNil(t, err)
	})
}

func TestPT_storeEvent(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		var (
			localOffset int64
			partition   int32
			topic       = "some-topic"
			key         = "some-key"
			value       = []byte("some-vale")
			actualKey   string
			actualValue []byte
			updateCB    UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte, headers ...*sarama.RecordHeader) error {
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
		bm.mst.EXPECT().Open().Return(nil)
		bm.mst.EXPECT().SetOffset(localOffset).Return(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		test.AssertNil(t, err)
		err = pt.storeEvent(key, value, localOffset)
		test.AssertEqual(t, actualKey, key)
		test.AssertEqual(t, actualValue, value)
		test.AssertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		var (
			localOffset int64
			partition   int32
			topic                      = "some-topic"
			key                        = "some-key"
			value                      = []byte("some-vale")
			updateCB    UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte, headers ...*sarama.RecordHeader) error {
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
		bm.mst.EXPECT().Open().Return(nil)
		bm.mst.EXPECT().SetOffset(localOffset).Return(retErr)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		test.AssertNil(t, err)
		err = pt.storeEvent(key, value, localOffset)
		test.AssertNotNil(t, err)
	})
}

func TestPT_Close(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		var (
			partition int32
			topic     = "some-topic"
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
		bm.mst.EXPECT().Open().Return(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		test.AssertNil(t, err)
		err = pt.Close()
		test.AssertNil(t, err)
	})
	t.Run("succeed2", func(t *testing.T) {
		var (
			partition int32
			topic     = "some-topic"
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
		test.AssertNil(t, err)
	})
}

func TestPT_markRecovered(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		var (
			partition int32
			topic     = "some-topic"
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.mst.EXPECT().Open().Return(nil)
		bm.mst.EXPECT().MarkRecovered().Return(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		test.AssertNil(t, err)
		test.AssertTrue(t, !pt.state.IsState(State(PartitionRunning)))
		err = pt.markRecovered(ctx)
		test.AssertNil(t, err)
		test.AssertTrue(t, pt.state.IsState(State(PartitionRunning)))
	})
	t.Run("fail", func(t *testing.T) {
		var (
			partition int32
			topic           = "some-topic"
			retErr    error = fmt.Errorf("store error")
		)
		pt, bm, ctrl := defaultPT(
			t,
			topic,
			partition,
			nil,
			nil,
		)
		defer ctrl.Finish()
		bm.mst.EXPECT().Open().Return(nil)
		bm.mst.EXPECT().MarkRecovered().Return(retErr)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := pt.setup(ctx)
		test.AssertNil(t, err)
		err = pt.markRecovered(ctx)
		test.AssertNotNil(t, err)
		test.AssertTrue(t, pt.state.IsState(State(PartitionPreparing)))
	})
}

func TestPT_SetupAndCatchupToHwm(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		var (
			oldest    int64
			newest    int64 = 5
			local           = oldest
			consumer        = defaultSaramaAutoConsumerMock(t)
			topic           = "some-topic"
			partition int32
			count     int64
			updateCB  UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte, headers ...*sarama.RecordHeader) error {
				atomic.AddInt64(&count, 1)
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
		logger.Debug(true, false)
		defer ctrl.Finish()
		pt.consumer = consumer
		bm.mst.EXPECT().Open().Return(nil)
		bm.mst.EXPECT().GetOffset(gomock.Any()).Return(local, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil)
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil)
		bm.mst.EXPECT().MarkRecovered().Return(nil)
		partConsumer := consumer.ExpectConsumePartition(topic, partition, local+1)
		partConsumer.ExpectMessagesDrainedOnClose()

		msgsToRecover := newest - local
		for i := int64(0); i < msgsToRecover; i++ {
			partConsumer.YieldMessage(&sarama.ConsumerMessage{})
			bm.mst.EXPECT().SetOffset(gomock.Any()).Return(nil)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		err := pt.SetupAndRecover(ctx, false)
		test.AssertNil(t, err)
		test.AssertTrue(t, atomic.LoadInt64(&count) == msgsToRecover)
	})
	t.Run("fail", func(t *testing.T) {
		var (
			consumer  = defaultSaramaAutoConsumerMock(t)
			topic     = "some-topic"
			partition int32
			retErr    = fmt.Errorf("offset-error")
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
		bm.mst.EXPECT().Open().Return(nil)
		bm.mst.EXPECT().GetOffset(gomock.Any()).Return(int64(0), retErr)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := pt.SetupAndRecover(ctx, false)
		test.AssertNotNil(t, err)
	})
}

func TestPT_SetupAndCatchupForever(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		var (
			oldest    int64
			newest    int64 = 10
			consumer        = defaultSaramaAutoConsumerMock(t)
			topic           = "some-topic"
			partition int32
			count     int64
			updateCB  UpdateCallback = func(s storage.Storage, partition int32, key string, value []byte, headers ...*sarama.RecordHeader) error {
				atomic.AddInt64(&count, 1)
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
		bm.useMemoryStorage()
		pt.consumer = consumer
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetOldest).Return(oldest, nil).AnyTimes()
		bm.tmgr.EXPECT().GetOffset(pt.topic, pt.partition, sarama.OffsetNewest).Return(newest, nil).AnyTimes()
		partConsumer := consumer.ExpectConsumePartition(topic, partition, anyOffset)
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

		err := pt.SetupAndRecover(ctx, false)
		test.AssertNil(t, err)
		cancel()
	})
	t.Run("fail", func(t *testing.T) {
		var (
			consumer  = defaultSaramaAutoConsumerMock(t)
			topic     = "some-topic"
			partition int32
			retErr    = fmt.Errorf("offset-error")
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
		bm.mst.EXPECT().Open().Return(nil)
		bm.mst.EXPECT().GetOffset(gomock.Any()).Return(int64(0), retErr)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := pt.SetupAndRecover(ctx, false)
		test.AssertNotNil(t, err)
		cancel()
	})
}
