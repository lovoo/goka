package goka

import (
	"context"
	"errors"
	"hash"
	"testing"
	"time"

	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/mock"
	"github.com/lovoo/goka/storage"

	"github.com/facebookgo/ensure"
	"github.com/golang/mock/gomock"
)

var (
	recoveredMessages int
)

// constHasher implements a hasher that will always return the specified
// partition. Doesn't properly implement the Hash32 interface, use only in
// tests.
type constHasher struct {
	partition uint32
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

func (ch *constHasher) Write(p []byte) (n int, err error) {
	return len(p), nil
}

// NewConstHasher creates a constant hasher that hashes any value to 0.
func NewConstHasher(part uint32) hash.Hash32 {
	return &constHasher{partition: part}
}

func createTestView(t *testing.T, consumer kafka.Consumer, sb storage.Builder, tm kafka.TopicManager) *View {
	recoveredMessages = 0
	opts := &voptions{
		log:        logger.Default(),
		tableCodec: new(codec.String),
		updateCallback: func(s storage.Storage, partition int32, key string, value []byte) error {
			if err := DefaultUpdate(s, partition, key, value); err != nil {
				return err
			}
			recoveredMessages++
			return nil
		},
		hasher: DefaultHasher(),
	}
	opts.builders.storage = sb
	opts.builders.topicmgr = func(brokers []string) (kafka.TopicManager, error) {
		return tm, nil
	}
	opts.builders.consumer = func(brokers []string, topic, id string) (kafka.Consumer, error) {
		return consumer, nil
	}

	reader := &View{topic: tableName(group), opts: opts}
	return reader
}

func TestView_createPartitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		consumer = mock.NewMockConsumer(ctrl)
		st       = mock.NewMockStorage(ctrl)
		sb       = func(topic string, partition int32) (storage.Storage, error) {
			return st, nil
		}
		tm = mock.NewMockTopicManager(ctrl)
	)

	tm.EXPECT().Partitions(tableName(group)).Return([]int32{0, 1}, nil)
	tm.EXPECT().Close()
	v := createTestView(t, consumer, sb, tm)

	err := v.createPartitions(nil)
	ensure.Nil(t, err)

	tm.EXPECT().Partitions(tableName(group)).Return(nil, errors.New("some error"))
	tm.EXPECT().Close()
	v = createTestView(t, consumer, sb, tm)
	err = v.createPartitions(nil)
	ensure.NotNil(t, err)

	tm.EXPECT().Partitions(tableName(group)).Return([]int32{0, 4}, nil)
	tm.EXPECT().Close()
	v = createTestView(t, consumer, sb, tm)
	err = v.createPartitions(nil)
	ensure.NotNil(t, err)

	sb = func(topic string, partition int32) (storage.Storage, error) {
		return nil, errors.New("some error")
	}
	tm.EXPECT().Partitions(tableName(group)).Return([]int32{0, 1}, nil)
	tm.EXPECT().Close()
	v = createTestView(t, consumer, sb, tm)
	err = v.createPartitions(nil)
	ensure.NotNil(t, err)

}

func TestView_HasGet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		st = mock.NewMockStorage(ctrl)
		sb = func(topic string, partition int32) (storage.Storage, error) {
			return st, nil
		}
		consumer = mock.NewMockConsumer(ctrl)
		tm       = mock.NewMockTopicManager(ctrl)
		v        = createTestView(t, consumer, sb, tm)
	)

	gomock.InOrder(
		tm.EXPECT().Partitions(tableName(group)).Return([]int32{0, 1, 2}, nil),
		tm.EXPECT().Close(),
		st.EXPECT().Has("item1").Return(false, nil),
		st.EXPECT().Get("item1").Return([]byte("item1-value"), nil),
	)

	err := v.createPartitions(nil)
	ensure.Nil(t, err)

	hasItem, err := v.Has("item1")
	ensure.Nil(t, err)
	ensure.False(t, hasItem)

	value, err := v.Get("item1")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, value.(string), "item1-value")
}

func TestView_StartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		st = mock.NewMockStorage(ctrl)
		sb = func(topic string, partition int32) (storage.Storage, error) {
			return st, nil
		}
		consumer     = mock.NewMockConsumer(ctrl)
		tm           = mock.NewMockTopicManager(ctrl)
		v            = createTestView(t, consumer, sb, tm)
		initial      = make(chan bool)
		final        = make(chan bool)
		ch           = make(chan kafka.Event)
		chClose      = func() { close(ch) }
		initialClose = func() { close(initial) }

		offset = int64(123)
		par    = int32(0)
	)

	gomock.InOrder(
		tm.EXPECT().Partitions(tableName(group)).Return([]int32{0}, nil),
		tm.EXPECT().Close(),
		consumer.EXPECT().Events().Do(initialClose).Return(ch),
	)
	gomock.InOrder(
		st.EXPECT().Open(),
		st.EXPECT().GetOffset(int64(-2)).Return(int64(123), nil),
		consumer.EXPECT().AddPartition(tableName(group), int32(par), int64(offset)),
	)
	gomock.InOrder(
		consumer.EXPECT().RemovePartition(tableName(group), int32(par)),
		consumer.EXPECT().Close().Do(chClose).Return(nil),
		st.EXPECT().Close(),
	)

	err := v.createPartitions(nil)
	ensure.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		errs := v.Run(ctx)
		ensure.Nil(t, errs)
		close(final)
	}()

	err = doTimed(t, func() {
		<-initial
		cancel()
		<-final
	})
	ensure.Nil(t, err)
}

func TestView_StartStopWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		st = mock.NewMockStorage(ctrl)
		sb = func(topic string, partition int32) (storage.Storage, error) {
			return st, nil
		}
		consumer = mock.NewMockConsumer(ctrl)
		tm       = mock.NewMockTopicManager(ctrl)
		v        = createTestView(t, consumer, sb, tm)
		final    = make(chan bool)
		ch       = make(chan kafka.Event)
	)

	tm.EXPECT().Partitions(tableName(group)).Return([]int32{0}, nil)
	tm.EXPECT().Close()
	err := v.createPartitions(nil)
	ensure.Nil(t, err)

	consumer.EXPECT().Events().Return(ch)
	st.EXPECT().Open()
	st.EXPECT().GetOffset(int64(-2)).Return(int64(0), errors.New("some error1"))
	st.EXPECT().Close()
	consumer.EXPECT().Close().Return(errors.New("some error2")).Do(func() { close(ch) })

	go func() {
		viewErrs := v.Run(context.Background())
		ensure.StringContains(t, viewErrs.Error(), "error1")
		ensure.StringContains(t, viewErrs.Error(), "error2")
		close(final)
	}()

	err = doTimed(t, func() {
		<-final
	})
	ensure.Nil(t, err)
}

func TestView_RestartNonRestartable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		st = mock.NewMockStorage(ctrl)
		sb = func(topic string, partition int32) (storage.Storage, error) {
			return st, nil
		}
		consumer     = mock.NewMockConsumer(ctrl)
		tm           = mock.NewMockTopicManager(ctrl)
		v            = createTestView(t, consumer, sb, tm)
		initial      = make(chan bool)
		final        = make(chan bool)
		ch           = make(chan kafka.Event)
		chClose      = func() { close(ch) }
		initialClose = func() { close(initial) }

		offset = int64(123)
		par    = int32(0)
	)
	v.opts.restartable = false

	gomock.InOrder(
		tm.EXPECT().Partitions(tableName(group)).Return([]int32{0}, nil),
		tm.EXPECT().Close(),
		consumer.EXPECT().Events().Do(initialClose).Return(ch),
	)
	gomock.InOrder(
		st.EXPECT().Open(),
		st.EXPECT().GetOffset(int64(-2)).Return(int64(123), nil),
		consumer.EXPECT().AddPartition(tableName(group), int32(par), int64(offset)),
	)
	gomock.InOrder(
		consumer.EXPECT().RemovePartition(tableName(group), int32(par)),
		consumer.EXPECT().Close().Do(chClose).Return(nil),
		st.EXPECT().Close(),
	)

	err := v.createPartitions(nil)
	ensure.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		errs := v.Run(ctx)
		ensure.Nil(t, errs)
		close(final)
	}()

	err = doTimed(t, func() {
		<-initial
		cancel()
		<-final
	})
	ensure.Nil(t, err)

	// restart view
	final = make(chan bool)

	go func() {
		err = v.Run(context.Background())
		ensure.NotNil(t, err)
		ensure.StringContains(t, err.Error(), "terminated")
		close(final)
	}()

	err = doTimed(t, func() {
		<-final
	})
	ensure.Nil(t, err)

	err = v.Terminate() // silent
	ensure.Nil(t, err)
}

func TestView_Restart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		st = mock.NewMockStorage(ctrl)
		sb = func(topic string, partition int32) (storage.Storage, error) {
			return st, nil
		}
		consumer     = mock.NewMockConsumer(ctrl)
		tm           = mock.NewMockTopicManager(ctrl)
		v            = createTestView(t, consumer, sb, tm)
		initial      = make(chan bool)
		final        = make(chan bool)
		ch           = make(chan kafka.Event)
		chClose      = func() { close(ch) }
		initialClose = func() { close(initial) }

		offset = int64(123)
		par    = int32(0)
	)
	v.opts.restartable = true

	gomock.InOrder(
		tm.EXPECT().Partitions(tableName(group)).Return([]int32{0}, nil),
		tm.EXPECT().Close(),
		consumer.EXPECT().Events().Do(initialClose).Return(ch),
	)
	gomock.InOrder(
		st.EXPECT().Open(),
		st.EXPECT().GetOffset(int64(-2)).Return(int64(123), nil),
		consumer.EXPECT().AddPartition(tableName(group), int32(par), int64(offset)),
	)
	gomock.InOrder(
		consumer.EXPECT().RemovePartition(tableName(group), int32(par)),
		consumer.EXPECT().Close().Do(chClose).Return(nil),
	)

	err := v.createPartitions(nil)
	ensure.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		errs := v.Run(ctx)
		ensure.Nil(t, errs)
		close(final)
	}()

	err = doTimed(t, func() {
		<-initial
		cancel()
		<-final
	})
	ensure.Nil(t, err)

	// restart view
	final = make(chan bool)
	initial = make(chan bool, 3)
	initialPush := func() { initial <- true }
	ch = make(chan kafka.Event)
	chClose = func() { close(ch) }

	// st.Open is not called because of openOnce in the storageProxy
	st.EXPECT().GetOffset(int64(-2)).Return(int64(123), nil)
	consumer.EXPECT().AddPartition(tableName(group), int32(0), int64(offset))
	consumer.EXPECT().Events().Return(ch)
	consumer.EXPECT().RemovePartition(tableName(group), int32(0))
	consumer.EXPECT().Close().Do(chClose).Return(nil)

	_ = initialPush
	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		err = v.Run(ctx)
		ensure.Nil(t, err)
		close(final)
	}()
	time.Sleep(2 * time.Second)

	err = doTimed(t, func() {
		cancel()
		<-final
	})
	ensure.Nil(t, err)

	st.EXPECT().Close()
	err = v.Terminate()
	ensure.Nil(t, err)
}

func TestView_GetErrors(t *testing.T) {
	v := &View{opts: &voptions{hasher: DefaultHasher()}}
	_, err := v.Get("hey")
	ensure.NotNil(t, err)

	_, err = v.Has("hey")
	ensure.NotNil(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		st = mock.NewMockStorage(ctrl)
		sb = func(topic string, partition int32) (storage.Storage, error) {
			return st, nil
		}
		consumer = mock.NewMockConsumer(ctrl)
		tm       = mock.NewMockTopicManager(ctrl)
	)

	v = createTestView(t, consumer, sb, tm)

	tm.EXPECT().Partitions(tableName(group)).Return([]int32{0}, nil)
	tm.EXPECT().Close()
	err = v.createPartitions(nil)
	ensure.Nil(t, err)

	st.EXPECT().Get("hey").Return(nil, errors.New("some error"))
	_, err = v.Get("hey")
	ensure.NotNil(t, err)
}

func TestNewView(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		consumer = mock.NewMockConsumer(ctrl)
		tm       = mock.NewMockTopicManager(ctrl)
	)
	_, err := NewView(nil, GroupTable(group), new(codec.Bytes), WithViewConsumerBuilder(createConsumerBuilder(nil)))
	ensure.NotNil(t, err)

	gomock.InOrder(
		tm.EXPECT().Partitions(tableName(group)).Return(nil, errors.New("some error")),
		tm.EXPECT().Close(),
	)
	_, err = NewView(nil, GroupTable(group), new(codec.Bytes),
		WithViewConsumerBuilder(createConsumerBuilder(consumer)),
		WithViewTopicManagerBuilder(createTopicManagerBuilder(tm)))
	ensure.NotNil(t, err)

	gomock.InOrder(
		tm.EXPECT().Partitions(tableName(group)).Return([]int32{0, 1, 2}, nil),
		tm.EXPECT().Close(),
	)
	v, err := NewView(nil, GroupTable(group), new(codec.Bytes),
		WithViewConsumerBuilder(createConsumerBuilder(consumer)),
		WithViewTopicManagerBuilder(createTopicManagerBuilder(tm)))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v.topic, tableName(group))
	ensure.DeepEqual(t, v.consumer, nil) // is set first upon start
	ensure.True(t, len(v.partitions) == 3)
}

func TestView_Evict(t *testing.T) {
	key := "some-key"
	val := "some-val"

	st := storage.NewMemory()
	err := st.Set(key, []byte(val))
	ensure.Nil(t, err)

	v := &View{
		partitions: []*partition{
			{st: &storageProxy{partition: 0, Storage: st}},
		},
		opts: &voptions{
			hasher: func() hash.Hash32 {
				return NewConstHasher(0)
			},
			tableCodec: new(codec.String),
		},
	}

	vinf, err := v.Get(key)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, vinf, val)

	err = v.Evict(key)
	ensure.Nil(t, err)

	vinf, err = v.Get(key)
	ensure.Nil(t, err)
	ensure.Nil(t, vinf)
}

func doTimed(t *testing.T, do func()) error {
	ch := make(chan bool)
	go func() {
		do()
		close(ch)
	}()

	select {
	case <-time.After(2 * time.Second):
		t.Fail()
		return errors.New("function took too long to complete")
	case <-ch:
	}

	return nil
}

func ExampleView_simple() {
	var (
		brokers       = []string{"localhost:9092"}
		group   Group = "group-name"
	)
	v, err := NewView(brokers, GroupTable(group), nil)
	if err != nil {
		panic(err)
	}
	if err = v.Run(context.Background()); err != nil {
		panic(err)
	}
}
