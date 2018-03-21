package goka

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/mock"
	"github.com/lovoo/goka/storage"

	"github.com/facebookgo/ensure"
	"github.com/golang/mock/gomock"
)

const (
	group = "group"
	topic = "topic"
)

func newStorageProxy(st storage.Storage, id int32, update UpdateCallback) *storageProxy {
	return &storageProxy{
		Storage:   st,
		partition: id,
		update:    update,
	}
}

func newNullStorageProxy(id int32) *storageProxy {
	return &storageProxy{
		Storage:   storage.NewMemory(),
		partition: id,
		stateless: true,
	}
}

func TestNewPartition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	p := newPartition(logger.Default(), topic, nil, nil, nil, defaultPartitionChannelSize)
	ensure.True(t, p != nil)
}

func TestPartition_startStateless(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		proxy       = mock.NewMockkafkaProxy(ctrl)
		wait        = make(chan bool)
		final       = make(chan bool)
		ctx, cancel = context.WithCancel(context.Background())
	)

	p := newPartition(logger.Default(), topic, nil, newNullStorageProxy(0), proxy, defaultPartitionChannelSize)
	proxy.EXPECT().AddGroup().Do(func() { close(wait) })
	proxy.EXPECT().Stop()

	go func() {
		err := p.start(ctx)
		ensure.Nil(t, err)
		close(final)
	}()

	err := doTimed(t, func() {
		<-wait
		cancel()
		<-final
	})
	ensure.Nil(t, err)
}

func TestPartition_startStateful(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		proxy       = mock.NewMockkafkaProxy(ctrl)
		st          = mock.NewMockStorage(ctrl)
		offset      = int64(123)
		wait        = make(chan bool)
		ctx, cancel = context.WithCancel(context.Background())
	)

	p := newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, nil), proxy, defaultPartitionChannelSize)

	gomock.InOrder(
		st.EXPECT().GetOffset(int64(-2)).Return(offset, nil),
		proxy.EXPECT().Add(topic, int64(offset)),
		proxy.EXPECT().Remove(topic),
		proxy.EXPECT().Stop(),
	)
	go func() {
		err := p.start(ctx)
		ensure.Nil(t, err)
		close(wait)
	}()

	err := doTimed(t, func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
		<-wait
	})
	ensure.Nil(t, err)
}

func TestPartition_runStateless(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		proxy             = mock.NewMockkafkaProxy(ctrl)
		key               = "key"
		par         int32 = 1
		offset      int64 = 4
		value             = []byte("value")
		wait              = make(chan bool)
		step              = make(chan bool)
		ctx, cancel       = context.WithCancel(context.Background())
		count       int64
	)

	consume := func(msg *message, st storage.Storage, wg *sync.WaitGroup, pstats *PartitionStats) (int, error) {
		atomic.AddInt64(&count, 1)
		ensure.DeepEqual(t, msg.Key, string(key))
		ensure.DeepEqual(t, msg.Data, value)
		step <- true
		return 0, nil
	}

	p := newPartition(logger.Default(), topic, consume, newNullStorageProxy(0), proxy, defaultPartitionChannelSize)

	proxy.EXPECT().AddGroup()
	proxy.EXPECT().Stop()
	go func() {
		err := p.start(ctx)
		ensure.Nil(t, err)
		close(wait)
	}()

	// message will be processed
	p.ch <- &kafka.Message{
		Key:       key,
		Offset:    offset,
		Partition: par,
		Topic:     "some-other-topic",
		Value:     value,
	}

	// garbage will be dropped
	p.ch <- new(kafka.NOP)

	err := doTimed(t, func() {
		<-step
		cancel()
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(1))
		<-wait
	})
	ensure.Nil(t, err)
}

func TestPartition_runStatelessWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		proxy        = mock.NewMockkafkaProxy(ctrl)
		key          = "key"
		par    int32 = 1
		offset int64 = 4
		value        = []byte("value")
		wait         = make(chan bool)
		count  int64
	)

	consume := func(msg *message, st storage.Storage, wg *sync.WaitGroup, pstats *PartitionStats) (int, error) {
		atomic.AddInt64(&count, 1)
		return 0, nil
	}

	p := newPartition(logger.Default(), topic, consume, newNullStorageProxy(0), proxy, defaultPartitionChannelSize)

	proxy.EXPECT().AddGroup()
	proxy.EXPECT().Stop()
	go func() {
		err := p.start(context.Background())
		ensure.NotNil(t, err)
		close(wait)
	}()

	// message causes error dropped because topic is the same as group table topic
	p.ch <- &kafka.Message{
		Key:       key + "something",
		Offset:    offset - 1,
		Partition: par,
		Topic:     topic,
		Value:     value,
	}

	err := doTimed(t, func() {
		<-wait
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(0))
	})
	ensure.Nil(t, err)

	// test sending error into the channel
	p = newPartition(logger.Default(), topic, consume, newNullStorageProxy(0), proxy, defaultPartitionChannelSize)
	wait = make(chan bool)

	proxy.EXPECT().AddGroup()
	proxy.EXPECT().Stop()
	go func() {
		err := p.start(context.Background())
		ensure.NotNil(t, err)
		close(wait)
	}()

	p.ch <- &kafka.Error{}

	err = doTimed(t, func() {
		<-wait
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(0))
	})
	ensure.Nil(t, err)

}

func TestPartition_runStateful(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		proxy             = mock.NewMockkafkaProxy(ctrl)
		st                = mock.NewMockStorage(ctrl)
		key               = "key"
		par         int32 = 1
		offset      int64 = 4
		value             = []byte("value")
		wait              = make(chan bool)
		step              = make(chan bool)
		ctx, cancel       = context.WithCancel(context.Background())
		count       int64
	)

	consume := func(msg *message, st storage.Storage, wg *sync.WaitGroup, pstats *PartitionStats) (int, error) {
		atomic.AddInt64(&count, 1)
		ensure.DeepEqual(t, msg.Key, string(key))
		ensure.DeepEqual(t, msg.Data, value)
		step <- true
		return 0, nil
	}

	p := newPartition(logger.Default(), topic, consume, newStorageProxy(st, 0, nil), proxy, 0)

	gomock.InOrder(
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		st.EXPECT().MarkRecovered(),
		proxy.EXPECT().Remove(topic),
		proxy.EXPECT().AddGroup(),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start(ctx)
		log.Printf("%v", err)
		ensure.Nil(t, err)
		close(wait)
	}()

	// partition should be marked recovered after the HWM or EOF message
	ensure.False(t, p.recovered())
	p.ch <- &kafka.BOF{
		Partition: par,
		Topic:     topic,
		Offset:    offset,
		Hwm:       offset,
	}

	// message will terminate load
	p.ch <- &kafka.EOF{
		Partition: par,
		Topic:     topic,
		Hwm:       offset,
	}
	p.ch <- new(kafka.NOP)
	ensure.True(t, p.recovered())

	// message will be processed
	p.ch <- &kafka.Message{
		Key:       key,
		Offset:    offset,
		Partition: par,
		Topic:     "some-other-topic",
		Value:     value,
	}

	err := doTimed(t, func() {
		<-step
		cancel()
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(1))
		<-wait
	})
	ensure.Nil(t, err)
}

func TestPartition_runStatefulWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		proxy        = mock.NewMockkafkaProxy(ctrl)
		st           = mock.NewMockStorage(ctrl)
		key          = "key"
		par    int32 = 1
		offset int64 = 4
		value        = []byte("value")
		wait         = make(chan bool)
		step         = make(chan bool)
		count  int64
	)

	consume := func(msg *message, st storage.Storage, wg *sync.WaitGroup, pstats *PartitionStats) (int, error) {
		if msg.Topic == "error" {
			return 0, errors.New("some error")
		}
		atomic.AddInt64(&count, 1)
		ensure.DeepEqual(t, msg.Key, string(key))
		ensure.DeepEqual(t, msg.Data, value)
		step <- true
		return 0, nil
	}

	p := newPartition(logger.Default(), topic, consume, newStorageProxy(st, 0, nil), proxy, defaultPartitionChannelSize)

	gomock.InOrder(
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		st.EXPECT().MarkRecovered(),
		proxy.EXPECT().Remove(topic),
		proxy.EXPECT().AddGroup(),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start(context.Background())
		ensure.NotNil(t, err)
		close(wait)
	}()

	// message will terminate load
	p.ch <- &kafka.EOF{
		Partition: par,
		Topic:     topic,
		Hwm:       offset,
	}

	// message will be processed
	p.ch <- &kafka.Message{
		Key:       key,
		Offset:    offset,
		Partition: par,
		Topic:     "some-other-topic",
		Value:     value,
	}

	// message will generate an error and will return the goroutine
	p.ch <- &kafka.Message{
		Key:       key,
		Offset:    offset,
		Partition: par,
		Topic:     "error",
		Value:     value,
	}

	err := doTimed(t, func() {
		<-step
		<-wait
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(1))
	})
	ensure.Nil(t, err)
}

func TestPartition_loadStateful(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		proxy             = mock.NewMockkafkaProxy(ctrl)
		st                = mock.NewMockStorage(ctrl)
		key               = "key"
		par         int32 = 1
		offset      int64 = 4
		value             = []byte("value")
		wait              = make(chan bool)
		step              = make(chan bool)
		ctx, cancel       = context.WithCancel(context.Background())
		count       int64
	)

	consume := func(msg *message, st storage.Storage, wg *sync.WaitGroup, pstats *PartitionStats) (int, error) {
		atomic.AddInt64(&count, 1)
		ensure.DeepEqual(t, msg.Key, string(key))
		ensure.DeepEqual(t, msg.Topic, "some-other-topic")
		ensure.DeepEqual(t, msg.Data, value)
		step <- true
		return 0, nil
	}

	p := newPartition(logger.Default(), topic, consume, newStorageProxy(st, 0, DefaultUpdate), proxy, defaultPartitionChannelSize)

	gomock.InOrder(
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(int64(offset)).Return(nil),
		st.EXPECT().MarkRecovered(),
		proxy.EXPECT().Remove(topic),
		proxy.EXPECT().AddGroup(),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start(ctx)
		ensure.Nil(t, err)
		close(wait)
	}()

	// message will be loaded (Topic is tableTopic)
	p.ch <- &kafka.Message{
		Key:       key,
		Offset:    offset,
		Partition: par,
		Topic:     topic,
		Value:     value,
	}

	// kafka.NOP will be dropped
	p.ch <- new(kafka.NOP)

	// message will terminate load
	p.ch <- &kafka.EOF{
		Partition: par,
		Topic:     topic,
		Hwm:       offset,
	}

	// message will be processed (Topic is != tableTopic)
	p.ch <- &kafka.Message{
		Key:       key,
		Offset:    offset + 1,
		Partition: par,
		Topic:     "some-other-topic",
		Value:     value,
	}

	// kafka.NOP will be dropped
	p.ch <- new(kafka.NOP)

	err := doTimed(t, func() {
		<-step
		cancel()
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(1))
		<-wait
	})
	ensure.Nil(t, err)
}

func TestPartition_loadStatefulWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		proxy        = mock.NewMockkafkaProxy(ctrl)
		st           = mock.NewMockStorage(ctrl)
		key          = "key"
		par    int32 = 1
		offset int64 = 4
		value        = []byte("value")
		wait         = make(chan bool)
		count  int64
	)

	// error in update
	update := func(st storage.Storage, p int32, k string, v []byte) error {
		atomic.AddInt64(&count, 1)
		return errors.New("some error")
	}

	p := newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, update), proxy, 0)

	gomock.InOrder(
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		proxy.EXPECT().Remove(topic),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start(context.Background())
		ensure.NotNil(t, err)
		close(wait)
	}()

	p.ch <- &kafka.Message{
		Key:       key,
		Offset:    offset,
		Partition: par,
		Topic:     topic,
		Value:     value,
	}

	err := doTimed(t, func() {
		<-wait
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(1))
	})
	ensure.Nil(t, err)

	// error in SetOffset
	wait = make(chan bool)
	p = newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, DefaultUpdate), proxy, 0)

	gomock.InOrder(
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(int64(offset)).Return(errors.New("some error")),
		proxy.EXPECT().Remove(topic),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start(context.Background())
		ensure.NotNil(t, err)
		close(wait)
	}()

	p.ch <- &kafka.Message{
		Key:       key,
		Offset:    offset,
		Partition: par,
		Topic:     topic,
		Value:     value,
	}

	err = doTimed(t, func() {
		<-wait
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(1))
	})
	ensure.Nil(t, err)

	// error in GetOffset
	wait = make(chan bool)
	p = newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, nil), proxy, 0)

	gomock.InOrder(
		st.EXPECT().GetOffset(int64(-2)).Return(int64(0), errors.New("some error")),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start(context.Background())
		ensure.NotNil(t, err)
		close(wait)
	}()

	err = doTimed(t, func() {
		<-wait
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(1))
	})
	ensure.Nil(t, err)
}

func TestPartition_loadStatefulWithErrorAddRemovePartition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		proxy        = mock.NewMockkafkaProxy(ctrl)
		st           = mock.NewMockStorage(ctrl)
		key          = "key"
		par    int32 = 1
		offset int64 = 4
		value        = []byte("value")
		wait   chan bool
		count  int64
	)

	// error in AddPartitionError
	wait = make(chan bool)
	p := newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, DefaultUpdate), proxy, 0)

	gomock.InOrder(
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset).Return(errors.New("some error adding partition")),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start(context.Background())
		ensure.NotNil(t, err)
		ensure.StringContains(t, err.Error(), "some error")
		close(wait)
	}()
	ensure.Nil(t, doTimed(t, func() { <-wait }))

	// error in RemovePartition
	update := func(st storage.Storage, p int32, k string, v []byte) error {
		atomic.AddInt64(&count, 1)
		return errors.New("some error")
	}

	wait = make(chan bool)
	p = newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, update), proxy, 0)

	gomock.InOrder(
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset).Return(nil),
		proxy.EXPECT().Remove(topic).Return(errors.New("error while removing partition")),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start(context.Background())
		ensure.NotNil(t, err)
		ensure.StringContains(t, err.Error(), "some error")
		ensure.StringContains(t, err.Error(), "error while removing partition")
		log.Printf("%v", err)
		close(wait)
	}()

	p.ch <- &kafka.Message{
		Key:       key,
		Offset:    offset,
		Partition: par,
		Topic:     topic,
		Value:     value,
	}

	err := doTimed(t, func() {
		<-wait
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(1))
	})
	ensure.Nil(t, err)
}

func TestPartition_catchupStateful(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		proxy             = mock.NewMockkafkaProxy(ctrl)
		st                = mock.NewMockStorage(ctrl)
		key               = "key"
		par         int32 = 1
		offset      int64 = 4
		value             = []byte("value")
		wait              = make(chan bool)
		step              = make(chan bool)
		sync              = func() error { return doTimed(t, func() { <-step }) }
		ctx, cancel       = context.WithCancel(context.Background())
		count       int64
	)

	update := func(st storage.Storage, p int32, k string, v []byte) error {
		atomic.AddInt64(&count, 1)
		step <- true
		return DefaultUpdate(st, p, k, v)
	}
	p := newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, update), proxy, 0)

	gomock.InOrder(
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(offset).Return(nil),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(offset+1).Return(nil),
		proxy.EXPECT().Remove(topic),
		st.EXPECT().MarkRecovered(),
		proxy.EXPECT().Add(topic, offset+2),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(offset+2).Return(nil),
		proxy.EXPECT().Remove(topic),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.startCatchup(ctx)
		ensure.Nil(t, err)
		close(wait)
	}()

	// beginning of file marks the beginning of topic
	p.ch <- &kafka.BOF{
		Topic:     topic,
		Partition: par,
		Offset:    offset,     // first offset that will arrive
		Hwm:       offset + 2, // highwatermark is one offset after the last one that will arrive
	}

	// message will be loaded (Topic is tableTopic)
	p.ch <- &kafka.Message{
		Topic:     topic,
		Partition: par,
		Offset:    offset,
		Key:       key,
		Value:     value,
	}
	err := sync()
	ensure.Nil(t, err)
	offset++

	// message will be loaded (Topic is tableTopic)
	p.ch <- &kafka.Message{
		Topic:     topic,
		Partition: par,
		Offset:    offset,
		Key:       key,
		Value:     value,
	}
	err = sync()
	ensure.Nil(t, err)
	offset++

	// message will not terminate load (catchup modus) but will mark as
	// recovered
	p.ch <- &kafka.EOF{
		Topic:     topic,
		Partition: par,
		Hwm:       offset,
	}
	p.ch <- new(kafka.NOP)
	ensure.True(t, p.recovered())

	// message will not terminate load (catchup modus)
	p.ch <- &kafka.EOF{
		Topic:     topic,
		Partition: par,
		Hwm:       offset,
	}
	p.ch <- new(kafka.NOP)
	ensure.True(t, p.recovered())

	// message will be loaded (Topic is tableTopic)
	p.ch <- &kafka.Message{
		Topic:     topic,
		Partition: par,
		Offset:    offset,
		Key:       key,
		Value:     value,
	}
	err = sync()
	ensure.Nil(t, err)
	p.ch <- new(kafka.NOP)

	err = doTimed(t, func() {
		cancel()
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(3))
		<-wait
	})
	ensure.Nil(t, err)
}

func TestPartition_catchupStatefulWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		proxy        = mock.NewMockkafkaProxy(ctrl)
		st           = mock.NewMockStorage(ctrl)
		key          = "key"
		par    int32 = 1
		offset int64 = 4
		value        = []byte("value")
		wait         = make(chan bool)
		step         = make(chan bool)
		sync         = func() error { return doTimed(t, func() { <-step }) }
		count  int64
	)

	update := func(st storage.Storage, p int32, k string, v []byte) error {
		atomic.AddInt64(&count, 1)
		step <- true
		return DefaultUpdate(st, p, k, v)
	}
	p := newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, update), proxy, 0)

	gomock.InOrder(
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(offset).Return(nil),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(offset+1).Return(nil),
		proxy.EXPECT().Remove(topic),
		st.EXPECT().MarkRecovered(),
		proxy.EXPECT().Add(topic, offset+2),
		proxy.EXPECT().Remove(topic),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.startCatchup(context.Background())
		ensure.NotNil(t, err)
		close(wait)
	}()

	// beginning of file marks the beginning of topic
	p.ch <- &kafka.BOF{
		Topic:     topic,
		Partition: par,
		Offset:    offset,     // first offset that will arrive
		Hwm:       offset + 2, // highwatermark is one offset after the last one that will arrive
	}

	// message will be loaded (Topic is tableTopic)
	p.ch <- &kafka.Message{
		Topic:     topic,
		Partition: par,
		Offset:    offset,
		Key:       key,
		Value:     value,
	}
	err := sync()
	ensure.Nil(t, err)
	offset++

	// message will be loaded (Topic is tableTopic)
	p.ch <- &kafka.Message{
		Topic:     topic,
		Partition: par,
		Offset:    offset,
		Key:       key,
		Value:     value,
	}
	err = sync()
	ensure.Nil(t, err)
	offset++

	// message will not terminate load (catchup modus) but will mark as
	// recovered
	p.ch <- &kafka.EOF{
		Topic:     topic,
		Partition: par,
		Hwm:       offset,
	}
	p.ch <- new(kafka.NOP)
	ensure.True(t, p.recovered())

	// message will not terminate load (catchup modus)
	p.ch <- &kafka.EOF{
		Topic:     topic,
		Partition: par,
		Hwm:       offset,
	}
	p.ch <- new(kafka.NOP)
	ensure.True(t, p.recovered())

	// message will cause error (wrong topic)
	p.ch <- &kafka.Message{
		Topic:     "some-other-topic",
		Partition: par,
		Offset:    offset + 1,
		Key:       key,
		Value:     value,
	}

	err = doTimed(t, func() {
		<-wait
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(2))
	})
	ensure.Nil(t, err)
}

func BenchmarkPartition_load(b *testing.B) {
	var (
		key          = "key"
		par    int32 = 1
		offset int64 = 4
		value        = []byte("value")
		wait         = make(chan bool)
		st           = storage.NewNull()
	)

	update := func(st storage.Storage, p int32, k string, v []byte) error {
		return nil
	}
	p := newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, update), new(nullProxy), 0)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := p.start(ctx)
		if err != nil {
			panic(err)
		}
		close(wait)
	}()

	// beginning of file marks the beginning of topic
	p.ch <- &kafka.BOF{
		Topic:     topic,
		Partition: par,
		Offset:    offset,
		Hwm:       int64(b.N + 1),
	}
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		p.ch <- &kafka.Message{
			Topic:     topic,
			Partition: par,
			Offset:    offset,
			Key:       key,
			Value:     value,
		}
		offset++
	}
	cancel()
	<-wait
}
