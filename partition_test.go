package goka

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/mock"
	"github.com/lovoo/goka/storage"

	"github.com/facebookgo/ensure"
	"github.com/golang/mock/gomock"
	metrics "github.com/rcrowley/go-metrics"
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
	p := newPartition(logger.Default(), topic, nil, nil, nil, metrics.DefaultRegistry, defaultPartitionChannelSize)
	ensure.True(t, p != nil)
}

func TestPartition_startStateless(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		proxy = mock.NewMockkafkaProxy(ctrl)
		wait  = make(chan bool)
		final = make(chan bool)
	)

	p := newPartition(logger.Default(), topic, nil, newNullStorageProxy(0), proxy,
		metrics.DefaultRegistry, defaultPartitionChannelSize)

	proxy.EXPECT().AddGroup().Do(func() { close(wait) })
	proxy.EXPECT().Stop()
	go func() {
		err := p.start()
		ensure.Nil(t, err)
		close(final)
	}()

	err := doTimed(t, func() {
		<-wait
		p.stop()
		<-final
	})
	ensure.Nil(t, err)
}

func TestPartition_startStateful(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		proxy  = mock.NewMockkafkaProxy(ctrl)
		st     = mock.NewMockStorage(ctrl)
		offset = int64(123)
		wait   = make(chan bool)
	)

	p := newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, nil), proxy,
		metrics.DefaultRegistry, defaultPartitionChannelSize)
	gomock.InOrder(
		st.EXPECT().Open().Return(nil),
		st.EXPECT().GetOffset(int64(-2)).Return(offset, nil),
		proxy.EXPECT().Add(topic, int64(offset)),
		proxy.EXPECT().Remove(topic),
		st.EXPECT().Close().Return(nil),
		proxy.EXPECT().Stop(),
	)
	go func() {
		err := p.start()
		ensure.Nil(t, err)
		close(wait)
	}()

	err := doTimed(t, func() {
		p.stop()
		<-wait
	})
	ensure.Nil(t, err)
}

func TestPartition_runStateless(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		proxy        = mock.NewMockkafkaProxy(ctrl)
		key          = "key"
		par    int32 = 1
		offset int64 = 4
		value        = []byte("value")
		wait         = make(chan bool)
		step         = make(chan bool)
		count  int64
	)

	consume := func(msg *message, st storage.Storage, wg *sync.WaitGroup) error {
		atomic.AddInt64(&count, 1)
		ensure.DeepEqual(t, msg.Key, string(key))
		ensure.DeepEqual(t, msg.Data, value)
		step <- true
		return nil
	}

	p := newPartition(logger.Default(), topic, consume, newNullStorageProxy(0), proxy, metrics.DefaultRegistry, defaultPartitionChannelSize)

	proxy.EXPECT().AddGroup()
	proxy.EXPECT().Stop()
	go func() {
		err := p.start()
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
		p.stop()
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

	consume := func(msg *message, st storage.Storage, wg *sync.WaitGroup) error {
		atomic.AddInt64(&count, 1)
		return nil
	}

	p := newPartition(logger.Default(), topic, consume, newNullStorageProxy(0),
		proxy, metrics.DefaultRegistry, defaultPartitionChannelSize)

	proxy.EXPECT().AddGroup()
	proxy.EXPECT().Stop()
	go func() {
		err := p.start()
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
		p.stop()
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(0))
	})
	ensure.Nil(t, err)

	// test sending error into the channel
	p = newPartition(logger.Default(), topic, consume, newNullStorageProxy(0), proxy,
		metrics.DefaultRegistry, defaultPartitionChannelSize)
	wait = make(chan bool)

	proxy.EXPECT().AddGroup()
	proxy.EXPECT().Stop()
	go func() {
		err := p.start()
		ensure.NotNil(t, err)
		close(wait)
	}()

	p.ch <- &kafka.Error{}

	err = doTimed(t, func() {
		<-wait
		p.stop()
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(0))
	})
	ensure.Nil(t, err)

}

func TestPartition_runStateful(t *testing.T) {
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

	consume := func(msg *message, st storage.Storage, wg *sync.WaitGroup) error {
		atomic.AddInt64(&count, 1)
		ensure.DeepEqual(t, msg.Key, string(key))
		ensure.DeepEqual(t, msg.Data, value)
		step <- true
		return nil
	}

	p := newPartition(logger.Default(), topic, consume, newStorageProxy(st, 0, nil), proxy, metrics.DefaultRegistry, 0)

	gomock.InOrder(
		st.EXPECT().Open().Return(nil),
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		proxy.EXPECT().Remove(topic),
		proxy.EXPECT().AddGroup(),
		st.EXPECT().Close().Return(nil),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start()
		ensure.Nil(t, err)
		close(wait)
	}()

	// partition should be marked ready after the HWM or EOF message
	ensure.False(t, p.ready())
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
	ensure.True(t, p.ready())

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
		p.stop()
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

	consume := func(msg *message, st storage.Storage, wg *sync.WaitGroup) error {
		if msg.Topic == "error" {
			return errors.New("some error")
		}
		atomic.AddInt64(&count, 1)
		ensure.DeepEqual(t, msg.Key, string(key))
		ensure.DeepEqual(t, msg.Data, value)
		step <- true
		return nil
	}

	p := newPartition(logger.Default(), topic, consume, newStorageProxy(st, 0, nil), proxy, metrics.DefaultRegistry, defaultPartitionChannelSize)

	gomock.InOrder(
		st.EXPECT().Open().Return(nil),
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		st.EXPECT().MarkRecovered(),
		proxy.EXPECT().Remove(topic),
		proxy.EXPECT().AddGroup(),
		st.EXPECT().Close().Return(nil),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start()
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
		p.stop()
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(1))
	})
	ensure.Nil(t, err)
}

func TestPartition_loadStateful(t *testing.T) {
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

	consume := func(msg *message, st storage.Storage, wg *sync.WaitGroup) error {
		atomic.AddInt64(&count, 1)
		ensure.DeepEqual(t, msg.Key, string(key))
		ensure.DeepEqual(t, msg.Topic, "some-other-topic")
		ensure.DeepEqual(t, msg.Data, value)
		step <- true
		return nil
	}

	p := newPartition(logger.Default(), topic, consume, newStorageProxy(st, 0, DefaultUpdate), proxy,
		metrics.DefaultRegistry, defaultPartitionChannelSize)

	gomock.InOrder(
		st.EXPECT().Open().Return(nil),
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(int64(offset)).Return(nil),
		st.EXPECT().MarkRecovered(),
		proxy.EXPECT().Remove(topic),
		proxy.EXPECT().AddGroup(),
		st.EXPECT().Close().Return(nil),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start()
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
		p.stop()
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

	p := newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, update), proxy, metrics.DefaultRegistry, 0)

	gomock.InOrder(
		st.EXPECT().Open().Return(nil),
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		proxy.EXPECT().Remove(topic),
		st.EXPECT().Close().Return(nil),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start()
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
		p.stop()
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(1))
		<-wait
	})
	ensure.Nil(t, err)

	// error in SetOffset
	wait = make(chan bool)
	p = newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, DefaultUpdate), proxy, metrics.DefaultRegistry, 0)

	gomock.InOrder(
		st.EXPECT().Open().Return(nil),
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(int64(offset)).Return(errors.New("some error")),
		proxy.EXPECT().Remove(topic),
		st.EXPECT().Close().Return(nil),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start()
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
		p.stop()
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(1))
		<-wait
	})
	ensure.Nil(t, err)

	// error in GetOffset
	wait = make(chan bool)
	p = newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, nil), proxy, metrics.DefaultRegistry, 0)

	gomock.InOrder(
		st.EXPECT().Open().Return(nil),
		st.EXPECT().GetOffset(int64(-2)).Return(int64(0), errors.New("some error")),
		st.EXPECT().Close().Return(nil),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.start()
		ensure.NotNil(t, err)
		close(wait)
	}()

	err = doTimed(t, func() {
		p.stop()
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(1))
		<-wait
	})
	ensure.Nil(t, err)
}

func TestPartition_catchupStateful(t *testing.T) {
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
	p := newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, update), proxy, metrics.DefaultRegistry, 0)

	gomock.InOrder(
		st.EXPECT().Open().Return(nil),
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(offset).Return(nil),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(offset+1).Return(nil),
		st.EXPECT().MarkRecovered(),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(offset+2).Return(nil),
		proxy.EXPECT().Remove(topic),
		st.EXPECT().Close().Return(nil),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.startCatchup()
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
	ensure.True(t, p.ready())

	// message will not terminate load (catchup modus)
	p.ch <- &kafka.EOF{
		Topic:     topic,
		Partition: par,
		Hwm:       offset,
	}
	p.ch <- new(kafka.NOP)
	ensure.True(t, p.ready())

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
		p.stop()
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
	p := newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, update), proxy, metrics.DefaultRegistry, 0)

	gomock.InOrder(
		st.EXPECT().Open().Return(nil),
		st.EXPECT().GetOffset(int64(-2)).Return(int64(offset), nil),
		proxy.EXPECT().Add(topic, offset),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(offset).Return(nil),
		st.EXPECT().Set(key, value),
		st.EXPECT().SetOffset(offset+1).Return(nil),
		st.EXPECT().MarkRecovered(),
		proxy.EXPECT().Remove(topic),
		st.EXPECT().Close().Return(nil),
		proxy.EXPECT().Stop(),
	)

	go func() {
		err := p.startCatchup()
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
	ensure.True(t, p.ready())

	// message will not terminate load (catchup modus)
	p.ch <- &kafka.EOF{
		Topic:     topic,
		Partition: par,
		Hwm:       offset,
	}
	p.ch <- new(kafka.NOP)
	ensure.True(t, p.ready())

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
		p.stop()
		ensure.DeepEqual(t, atomic.LoadInt64(&count), int64(2))
	})
	ensure.Nil(t, err)
}

func BenchmarkFib10(b *testing.B) {
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
	p := newPartition(logger.Default(), topic, nil, newStorageProxy(st, 0, update), new(nullProxy), metrics.DefaultRegistry, 0)

	go func() {
		err := p.start()
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

	p.stop()
	<-wait
}
