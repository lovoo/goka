package goka

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/mock"
	"github.com/lovoo/goka/storage"

	"github.com/facebookgo/ensure"
	"github.com/golang/mock/gomock"
	metrics "github.com/rcrowley/go-metrics"
)

var (
	rawCodec = new(codec.Bytes)
)

func syncWith(t *testing.T, ch chan kafka.Event, p ...int32) error {
	return doTimed(t, func() {
		for _, par := range p {
			ch <- &kafka.NOP{Partition: par}
			ch <- &kafka.NOP{Partition: -1}
			ch <- &kafka.NOP{Partition: par}
			ch <- &kafka.NOP{Partition: -1}
		}
		ch <- &kafka.NOP{Partition: -1}
	})
}

func createProcessorStateless(ctrl *gomock.Controller, consumer kafka.Consumer, npar int) *Processor {
	tm := mock.NewMockTopicManager(ctrl)
	producer := mock.NewMockProducer(ctrl)

	var partitions []int32
	for i := 0; i < npar; i++ {
		partitions = append(partitions, int32(i))
	}

	// successfully create processor
	tm.EXPECT().Partitions(topic).Return(partitions, nil)
	tm.EXPECT().Partitions(topic2).Return(partitions, nil)
	tm.EXPECT().EnsureStreamExists(loopName(group), len(partitions)).Return(nil)
	tm.EXPECT().Close().Return(nil)
	p, _ := NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Input(topic2, rawCodec, cb),
			Loop(rawCodec, cb),
		),
		WithTopicManager(tm),
		WithConsumer(consumer),
		WithProducer(producer),
		WithPartitionChannelSize(0),
	)
	return p
}

func createProcessor(ctrl *gomock.Controller, consumer kafka.Consumer, npar int, sb StorageBuilder) *Processor {
	tm := mock.NewMockTopicManager(ctrl)
	producer := mock.NewMockProducer(ctrl)

	var partitions []int32
	for i := 0; i < npar; i++ {
		partitions = append(partitions, int32(i))
	}

	// the prodcuer may be closed, but doesn't have to
	producer.EXPECT().Close().Return(nil).AnyTimes()

	// successfully create processor
	tm.EXPECT().Partitions(topic).Return(partitions, nil)
	tm.EXPECT().Partitions(topic2).Return(partitions, nil)
	tm.EXPECT().EnsureStreamExists(loopName(group), len(partitions)).Return(nil)
	tm.EXPECT().EnsureTableExists(tableName(group), len(partitions)).Return(nil)
	tm.EXPECT().Close().Return(nil)
	p, _ := NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Input(topic2, rawCodec, cb),
			Loop(rawCodec, cb),
			Persist(new(codec.String)),
		),
		WithTopicManager(tm),
		WithConsumer(consumer),
		WithProducer(producer),
		WithStorageBuilder(sb),
		WithPartitionChannelSize(0),
	)
	return p
}

func createProcessorWithTable(ctrl *gomock.Controller, consumer kafka.Consumer, npar int, sb StorageBuilder) *Processor {
	tm := mock.NewMockTopicManager(ctrl)
	producer := mock.NewMockProducer(ctrl)

	var partitions []int32
	for i := 0; i < npar; i++ {
		partitions = append(partitions, int32(i))
	}

	// successfully create processor
	tm.EXPECT().Partitions(topic).Return(partitions, nil)
	tm.EXPECT().Partitions(topic2).Return(partitions, nil)
	tm.EXPECT().Partitions(table).Return(partitions, nil)
	tm.EXPECT().EnsureStreamExists(loopName(group), len(partitions)).Return(nil)
	tm.EXPECT().EnsureTableExists(tableName(group), len(partitions)).Return(nil)
	tm.EXPECT().Close().Return(nil)
	p, _ := NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Input(topic2, rawCodec, cb),
			Loop(rawCodec, cb),
			Join(table, rawCodec),
			Persist(rawCodec),
		),
		WithTopicManager(tm),
		WithConsumer(consumer),
		WithProducer(producer),
		WithStorageBuilder(sb),
		WithPartitionChannelSize(0),
	)
	return p
}

var (
	topOff = map[string]int64{
		topic:           -1,
		loopName(group): -1,
		topic2:          -1,
	}
	errSome = errors.New("some error")
	cb      = func(ctx Context, msg interface{}) {}
)

const (
	topic2 = "topic2"
	table  = "table"
)

func TestProcessor_process(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		wg       sync.WaitGroup
		st       = mock.NewMockStorage(ctrl)
		consumer = mock.NewMockConsumer(ctrl)
		producer = mock.NewMockProducer(ctrl)
	)

	p := &Processor{
		graph: DefineGroup(group,
			Persist(new(codec.String)),
			Loop(c, cb),
			Input("sometopic", rawCodec, cb),
		),

		consumer: consumer,
		producer: producer,
	}

	consumer.EXPECT().Commit("sometopic", int32(1), int64(123))
	msg := &message{Topic: "sometopic", Partition: 1, Offset: 123}
	err := p.process(msg, st, &wg)
	ensure.Nil(t, err)
}

func TestNewProcessor(t *testing.T) {
	_, err := NewProcessor(nil, DefineGroup(group))
	ensure.NotNil(t, err)

	_, err = NewProcessor(nil, DefineGroup(group, Input("topic", rawCodec, nil)))
	ensure.NotNil(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		consumer = mock.NewMockConsumer(ctrl)
		producer = mock.NewMockProducer(ctrl)
		tm       = mock.NewMockTopicManager(ctrl)
	)

	// prepareTopics fails
	tm.EXPECT().Partitions(topic).Return([]int32{0, 1}, errors.New("some error"))
	tm.EXPECT().Close().Return(nil)
	_, err = NewProcessor(nil,
		DefineGroup(group, Input(topic, rawCodec, cb)),
		WithTopicManager(tm),
	)
	ensure.NotNil(t, err)

	// consumer builder fails
	tm.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)
	tm.EXPECT().Close().Return(nil)
	_, err = NewProcessor(nil,
		DefineGroup(group, Input(topic, rawCodec, cb)),
		WithTopicManager(tm),
		WithConsumer(nil),
		WithProducer(nil),
	)
	ensure.NotNil(t, err)

	// processor builder fails
	tm.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)
	tm.EXPECT().Close().Return(nil)
	_, err = NewProcessor(nil,
		DefineGroup(group, Input(topic, rawCodec, cb)),
		WithTopicManager(tm),
		WithConsumer(consumer),
		WithProducer(nil),
	)
	ensure.NotNil(t, err)

	// successfully create processor
	tm.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)
	tm.EXPECT().Partitions(string(topic2)).Return([]int32{0, 1}, nil)
	tm.EXPECT().EnsureStreamExists(loopName(group), 2).Return(nil)
	tm.EXPECT().EnsureTableExists(tableName(group), 2).Return(nil)
	tm.EXPECT().Close().Return(nil)
	p, err := NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Input(topic2, rawCodec, cb),
			Loop(rawCodec, cb),
			Persist(rawCodec),
		),
		WithTopicManager(tm),
		WithConsumer(consumer),
		WithProducer(producer),
	)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, p.graph.GroupTable().Topic(), tableName(group))
	ensure.DeepEqual(t, p.graph.LoopStream().Topic(), loopName(group))
	ensure.True(t, p.partitionCount == 2)
	ensure.True(t, len(p.graph.inputs()) == 2)
	ensure.False(t, p.isStateless())

	// successfully create stateless processor
	tm.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)
	tm.EXPECT().Partitions(string(topic2)).Return([]int32{0, 1}, nil)
	tm.EXPECT().Close().Return(nil)
	p, err = NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Input(topic2, rawCodec, cb),
		),
		WithTopicManager(tm),
		WithConsumer(consumer),
		WithProducer(producer),
	)
	ensure.Nil(t, err)
	ensure.True(t, p.graph.GroupTable() == nil)
	ensure.True(t, p.graph.LoopStream() == nil)
	ensure.True(t, p.partitionCount == 2)
	ensure.True(t, len(p.graph.inputs()) == 2)
	ensure.True(t, p.isStateless())

	// successfully create a processor with tables
	tm.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)
	tm.EXPECT().Partitions(table).Return([]int32{0, 1}, nil)
	tm.EXPECT().Close().Return(nil)
	p, err = NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Join(table, rawCodec),
		),
		WithTopicManager(tm),
		WithConsumer(consumer),
		WithProducer(producer),
	)
	ensure.Nil(t, err)
	ensure.True(t, p.graph.GroupTable() == nil)
	ensure.True(t, p.graph.LoopStream() == nil)
	ensure.True(t, p.partitionCount == 2)
	ensure.True(t, len(p.graph.inputs()) == 2)
	ensure.True(t, p.isStateless())

}

func TestProcessor_StartFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	consumer := mock.NewMockConsumer(ctrl)

	p := createProcessor(ctrl, consumer, 2, nil)

	// group consumer start fails
	consumer.EXPECT().Subscribe(topOff).Return(errSome)
	consumer.EXPECT().Close().Return(nil)

	errs := p.Start()
	ensure.NotNil(t, errs)
	p.Stop()
}

func TestProcessor_StartStopEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		consumer = mock.NewMockConsumer(ctrl)
		wait     = make(chan bool)
		ch       = make(chan kafka.Event)
		p        = createProcessor(ctrl, consumer, 2, nil)
	)

	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch)
	go func() {
		err := p.Start()
		ensure.Nil(t, err)
		close(wait)
	}()

	consumer.EXPECT().Close().Return(nil).Do(func() { close(ch) })
	err := doTimed(t, func() {
		p.Stop()
		<-wait
	})
	ensure.Nil(t, err)
}

func TestProcessor_StartStopEmptyError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		consumer = mock.NewMockConsumer(ctrl)
		wait     = make(chan bool)
		ch       = make(chan kafka.Event)
		p        = createProcessor(ctrl, consumer, 2, nil)
	)

	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch)
	go func() {
		err := p.Start()
		ensure.NotNil(t, err)
		close(wait)
	}()

	consumer.EXPECT().Close().Return(errors.New("some error")).Do(func() { close(ch) })
	err := doTimed(t, func() {
		p.Stop()
		<-wait
	})
	ensure.Nil(t, err)
}

// start processor and receives an error from Kafka in the events
// channel before rebalance.
func TestProcessor_StartWithErrorBeforeRebalance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	type TestCase struct {
		name  string
		event kafka.Event
	}
	tests := []TestCase{
		{"error", &kafka.Error{Err: errors.New("something")}},
		{"message", new(kafka.Message)},
		{"EOF", new(kafka.EOF)},
		{"BOF", new(kafka.BOF)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				err      error
				consumer = mock.NewMockConsumer(ctrl)
				st       = mock.NewMockStorage(ctrl)
				sb       = func(topic string, par int32, c Codec, r metrics.Registry) (storage.Storage, error) {
					return st, nil
				}
				final = make(chan bool)
				ch    = make(chan kafka.Event)
				p     = createProcessor(ctrl, consumer, 3, sb)
			)

			gomock.InOrder(
				consumer.EXPECT().Subscribe(topOff).Return(nil),
				consumer.EXPECT().Events().Return(ch),
				consumer.EXPECT().Close().Do(func() { close(ch) }),
			)
			go func() {
				err := p.Start()
				ensure.NotNil(t, err)
				close(final)
			}()

			ch <- tc.event

			err = doTimed(t, func() {
				<-final
				p.Stop()
			})
			ensure.Nil(t, err)
		})
	}
}

// start processor and receives an error from Kafka in the events
// channel after rebalance.
func TestProcessor_StartWithErrorAfterRebalance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		err      error
		consumer = mock.NewMockConsumer(ctrl)
		st       = mock.NewMockStorage(ctrl)
		sb       = func(topic string, par int32, c Codec, r metrics.Registry) (storage.Storage, error) {
			return st, nil
		}
		final = make(chan bool)
		ch    = make(chan kafka.Event)
		p     = createProcessor(ctrl, consumer, 3, sb)
	)

	// -- expectations --
	// 1. start
	gomock.InOrder(
		consumer.EXPECT().Subscribe(topOff).Return(nil),
		consumer.EXPECT().Events().Return(ch),
	)
	// 2. rebalance
	st.EXPECT().Open().Times(3)
	st.EXPECT().GetOffset(int64(-2)).Return(int64(123), nil).Times(3)
	consumer.EXPECT().AddPartition(tableName(group), int32(0), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(1), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(2), int64(123))
	// 3. message
	gomock.InOrder(
		st.EXPECT().SetEncoded("key", nil).Return(nil),
		st.EXPECT().SetOffset(int64(1)),
		st.EXPECT().Sync(),
	)

	// 4. error
	consumer.EXPECT().Close().Do(func() { close(ch) })
	consumer.EXPECT().RemovePartition(tableName(group), int32(0))
	consumer.EXPECT().RemovePartition(tableName(group), int32(1))
	consumer.EXPECT().RemovePartition(tableName(group), int32(2))
	st.EXPECT().Sync().Times(3)
	st.EXPECT().Close().Times(3)

	// -- test --
	// 1. start
	go func() {
		err := p.Start()
		ensure.NotNil(t, err)
		close(final)
	}()

	// 2. rebalance
	ensure.True(t, len(p.partitions) == 0)
	ch <- (*kafka.Assignment)(&map[int32]int64{0: -1, 1: -1, 2: -1})
	err = syncWith(t, ch, -1) // with processor
	ensure.Nil(t, err)
	ensure.True(t, len(p.partitions) == 3)

	// 3. message
	ch <- &kafka.Message{
		Topic:     tableName(group),
		Partition: 1,
		Offset:    1,
		Key:       "key",
	}
	err = syncWith(t, ch, 1) // with partition
	ensure.Nil(t, err)

	// 4. receive error
	ch <- new(kafka.Error)

	// 5. stop
	err = doTimed(t, func() {
		<-final
		p.Stop()
	})
	ensure.Nil(t, err)
}

func TestProcessor_Start(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		err      error
		consumer = mock.NewMockConsumer(ctrl)
		st       = mock.NewMockStorage(ctrl)
		sb       = func(topic string, par int32, c Codec, r metrics.Registry) (storage.Storage, error) {
			return st, nil
		}
		final = make(chan bool)
		ch    = make(chan kafka.Event)
		p     = createProcessor(ctrl, consumer, 3, sb)
	)

	// -- expectations --
	// 1. start
	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch)
	// 2. rebalance
	st.EXPECT().Open().Times(3)
	st.EXPECT().GetOffset(int64(-2)).Return(int64(123), nil).Times(3)
	consumer.EXPECT().AddPartition(tableName(group), int32(0), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(1), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(2), int64(123))
	// 3. load message partition 1
	st.EXPECT().SetEncoded("key", nil).Return(nil)
	st.EXPECT().SetOffset(int64(1))
	st.EXPECT().Sync()
	// 4. end of recovery partition 1
	gomock.InOrder(
		consumer.EXPECT().RemovePartition(tableName(group), int32(1)),
		consumer.EXPECT().AddGroupPartition(int32(1)),
	)
	// 5. process message partition 1
	consumer.EXPECT().Commit(topic, int32(1), int64(1))
	st.EXPECT().Sync() // run loop
	// 6. new assignment remove partition 1 and 2
	st.EXPECT().Sync()  // partition 1 final sync
	st.EXPECT().Close() // partition 1 close
	consumer.EXPECT().RemovePartition(tableName(group), int32(2))
	st.EXPECT().Sync()  // partition 2 final sync
	st.EXPECT().Close() // partition 2 close
	// 7. stop processor
	consumer.EXPECT().Close().Do(func() { close(ch) })
	consumer.EXPECT().RemovePartition(tableName(group), int32(0))
	st.EXPECT().Sync()
	st.EXPECT().Close()

	// -- test --
	// 1. start
	go func() {
		err := p.Start()
		ensure.Nil(t, err)
		close(final)
	}()

	// 2. rebalance
	ensure.True(t, len(p.partitions) == 0)
	ch <- (*kafka.Assignment)(&map[int32]int64{0: -1, 1: -1, 2: -1})
	err = syncWith(t, ch, -1) // with processor
	ensure.Nil(t, err)
	ensure.True(t, len(p.partitions) == 3)

	// 3. load message partition 1
	ch <- &kafka.Message{
		Topic:     tableName(group),
		Partition: 1,
		Offset:    1,
		Key:       "key",
	}
	err = syncWith(t, ch, 1) // with partition 1
	ensure.Nil(t, err)

	// 4. end of recovery partition 1
	ch <- &kafka.EOF{Partition: 1}
	err = syncWith(t, ch, 1) // with partition 1
	ensure.Nil(t, err)

	// 5. process message partition 1
	ch <- &kafka.Message{
		Topic:     topic,
		Partition: 1,
		Offset:    1,
		Key:       "key",
	}
	err = syncWith(t, ch, 1) // with partition 1
	ensure.Nil(t, err)

	// 6. new assignment remove partition 1 and 2
	ch <- (*kafka.Assignment)(&map[int32]int64{0: -1})
	err = syncWith(t, ch, 1, 2) // with partition 1 and 2
	ensure.Nil(t, err)
	ensure.True(t, len(p.partitions) == 1)

	// 7. stop processor
	err = doTimed(t, func() {
		p.Stop()
		<-final
	})
	ensure.Nil(t, err)

}

func TestProcessor_StartStateless(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		consumer = mock.NewMockConsumer(ctrl)
		final    = make(chan bool)
		ch       = make(chan kafka.Event)
		p        = createProcessorStateless(ctrl, consumer, 3)
		producer = p.producer.(*mock.MockProducer)
	)

	// -- expectactions --
	// 1. start
	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch)
	// 2. rebalance
	consumer.EXPECT().AddGroupPartition(int32(0))
	consumer.EXPECT().AddGroupPartition(int32(1))
	// 3. stop processor
	consumer.EXPECT().Close().Return(nil).Do(func() { close(ch) })
	producer.EXPECT().Close().Return(nil)

	// -- test --
	// 1. start
	go func() {
		err := p.Start()
		ensure.Nil(t, err)
		close(final)
	}()

	// 2. rebalance
	ensure.True(t, len(p.partitions) == 0)
	ch <- (*kafka.Assignment)(&map[int32]int64{0: -1, 1: -1})
	err := syncWith(t, ch, -1, 1, 2)
	ensure.Nil(t, err)
	ensure.True(t, len(p.partitions) == 2)

	// 3. stop processor
	err = doTimed(t, func() {
		p.Stop()
		<-final
	})
	ensure.Nil(t, err)
}

func TestProcessor_StartWithTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		err      error
		consumer = mock.NewMockConsumer(ctrl)
		st       = mock.NewMockStorage(ctrl)
		sb       = func(topic string, par int32, c Codec, r metrics.Registry) (storage.Storage, error) {
			return st, nil
		}
		final    = make(chan bool)
		ch       = make(chan kafka.Event)
		p        = createProcessorWithTable(ctrl, consumer, 3, sb)
		producer = p.producer.(*mock.MockProducer)
	)

	// -- expectations --
	// 1. start
	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch)
	// 2. rebalance
	st.EXPECT().Open().Times(6)
	st.EXPECT().GetOffset(int64(-2)).Return(int64(123), nil).Times(6)
	consumer.EXPECT().AddPartition(tableName(group), int32(0), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(1), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(2), int64(123))
	consumer.EXPECT().AddPartition(table, int32(0), int64(123))
	consumer.EXPECT().AddPartition(table, int32(1), int64(123))
	consumer.EXPECT().AddPartition(table, int32(2), int64(123))
	// 3. message to group table
	st.EXPECT().SetEncoded("key", nil).Return(nil)
	st.EXPECT().SetOffset(int64(1))
	st.EXPECT().Sync()
	// 4. finish recovery of partition 1
	gomock.InOrder(
		consumer.EXPECT().RemovePartition(tableName(group), int32(1)),
		consumer.EXPECT().AddGroupPartition(int32(1)),
	)
	// 5. process messages in partition 1
	gomock.InOrder(
		consumer.EXPECT().Commit(topic, int32(1), int64(1)),
		st.EXPECT().Sync(), // run loop
	)
	// 6. rebalance (only keep partition 0)
	st.EXPECT().Sync().Times(4)  // final sync
	st.EXPECT().Close().Times(4) // close group and other table partitions
	consumer.EXPECT().RemovePartition(table, int32(1))
	consumer.EXPECT().RemovePartition(table, int32(2))
	consumer.EXPECT().RemovePartition(tableName(group), int32(2))
	// 7. stop processor
	consumer.EXPECT().Close().Do(func() { close(ch) })
	consumer.EXPECT().RemovePartition(table, int32(0))
	consumer.EXPECT().RemovePartition(tableName(group), int32(0))
	st.EXPECT().Sync().Times(2)  // final sync
	st.EXPECT().Close().Times(2) // close group table and other table
	producer.EXPECT().Close().Return(nil)

	// -- test --
	// 1. start
	go func() {
		procErrs := p.Start()
		ensure.Nil(t, procErrs)
		close(final)
	}()

	// 2. rebalance
	ensure.True(t, len(p.partitions) == 0)
	ch <- (*kafka.Assignment)(&map[int32]int64{0: -1, 1: -1, 2: -1})
	syncWith(t, ch)
	ensure.True(t, len(p.partitions) == 3)

	// 3. message to group table
	ch <- &kafka.Message{
		Topic:     tableName(group),
		Partition: 1,
		Offset:    1,
		Key:       "key",
	}
	err = syncWith(t, ch, 1)
	ensure.Nil(t, err)

	// 4. finish recovery of partition 1
	ch <- &kafka.EOF{
		Partition: 1,
	}
	ensure.False(t, p.partitionViews[1][table].ready())
	time.Sleep(delayProxyInterval)
	ensure.False(t, p.partitionViews[1][table].ready())
	ch <- &kafka.EOF{
		Topic:     table,
		Partition: 1,
	}
	err = syncWith(t, ch)
	ensure.Nil(t, err)
	time.Sleep(delayProxyInterval)
	ensure.True(t, p.partitionViews[1][table].ready())

	// 5. process messages in partition 1
	ch <- &kafka.Message{
		Topic:     topic,
		Partition: 1,
		Offset:    1,
		Key:       "key",
	}
	err = syncWith(t, ch, 1)
	ensure.Nil(t, err)

	// 6. rebalance
	ch <- (*kafka.Assignment)(&map[int32]int64{0: -1})
	err = syncWith(t, ch, 1, 2) // synchronize with partitions 1 and 2
	ensure.Nil(t, err)
	ensure.True(t, len(p.partitions) == 1)

	// 7. stop processor
	err = doTimed(t, func() {
		p.Stop()
		<-final
	})
	ensure.Nil(t, err)
}

func TestProcessor_rebalanceError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		consumer = mock.NewMockConsumer(ctrl)
		wait     = make(chan bool)
		ch       = make(chan kafka.Event)
		p        = createProcessor(ctrl, consumer, 1,
			func(topic string, partition int32, c Codec, r metrics.Registry) (storage.Storage, error) {
				return nil, errors.New("some error")
			})
	)

	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch)
	consumer.EXPECT().Close().Return(nil).Do(func() {
		close(ch)
	})
	go func() {
		err := p.Start()
		ensure.NotNil(t, err)
		close(wait)
	}()

	// assignment arrives
	ensure.True(t, len(p.partitions) == 0)
	ch <- (*kafka.Assignment)(&map[int32]int64{0: -1})

	// stop processor
	err := doTimed(t, func() {
		p.Stop()
		<-wait
	})
	ensure.Nil(t, err)
}

func TestProcessor_HasGet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		st = mock.NewMockStorage(ctrl)
		sb = func(topic string, partition int32, c Codec, r metrics.Registry) (storage.Storage, error) {
			return st, nil
		}
		consumer = mock.NewMockConsumer(ctrl)
		ch       = make(chan kafka.Event)
		wait     = make(chan bool)
		p        = createProcessor(ctrl, consumer, 1, sb)
	)

	ensure.True(t, p.partitionCount == 1)

	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch)

	go func() {
		procErrs := p.Start()
		ensure.Nil(t, procErrs)
		close(wait)
	}()

	// assignment arrives

	ensure.True(t, len(p.partitions) == 0)
	gomock.InOrder(
		st.EXPECT().Open(),
		st.EXPECT().GetOffset(int64(-2)).Return(int64(123), nil),
		consumer.EXPECT().AddPartition(tableName(group), int32(0), int64(123)),
	)
	ch <- (*kafka.Assignment)(&map[int32]int64{0: -1})
	ch <- new(kafka.NOP)
	ensure.True(t, len(p.partitions) == 1)

	gomock.InOrder(
		st.EXPECT().Get("item1").Return("item1-value", nil),
	)

	value, err := p.Get("item1")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, value.(string), "item1-value")

	// stop processor
	gomock.InOrder(
		consumer.EXPECT().Close().Do(func() { close(ch) }),
		st.EXPECT().Sync(),
		consumer.EXPECT().RemovePartition(tableName(group), int32(0)),
		st.EXPECT().Close(),
	)

	err = doTimed(t, func() {
		p.Stop()
		<-wait
	})
	ensure.Nil(t, err)
}

func TestProcessor_HasGetStateless(t *testing.T) {
	p := &Processor{graph: DefineGroup(group)}
	_, err := p.Get("item1")
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "stateless processor")

	p = &Processor{graph: DefineGroup(group, Persist(c))}
	p.partitions = map[int32]*partition{
		0: new(partition),
	}
	p.partitionCount = 0
	_, err = p.Get("item1")
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "0 partitions")

	p = &Processor{graph: DefineGroup(group, Persist(c))}
	p.partitions = map[int32]*partition{
		0: new(partition),
	}
	p.partitionCount = 2
	_, err = p.Get("item1")
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "does not contain partition 1")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	st := mock.NewMockStorage(ctrl)
	p = &Processor{graph: DefineGroup(group, Persist(c))}
	p.partitions = map[int32]*partition{
		0: &partition{log: logger.Default(), st: &storageProxy{Storage: st, partition: 0}},
	}
	p.partitionCount = 1

	st.EXPECT().Get("item1").Return(nil, errors.New("some error"))
	_, err = p.Get("item1")
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "error getting item1")

	st.EXPECT().Get("item1").Return(nil, nil)
	value, err := p.Get("item1")
	ensure.Nil(t, err)
	ensure.True(t, value == nil)
}

// Example shows how to use a callback. For each partition of the topics, a new
// goroutine will be created. Topics should be co-partitioned (they should have
// the same number of partitions and be partitioned by the same key).
func ExampleProcessor_simplest() {
	var (
		brokers        = []string{"127.0.0.1:9092"}
		group   Group  = "group"
		topic   Stream = "topic"
	)

	consume := func(ctx Context, m interface{}) {
		fmt.Printf("Hello world: %v", m)
	}

	c, err := NewProcessor(brokers, DefineGroup(group, Input(topic, rawCodec, consume)))
	if err != nil {
		log.Fatalln(err)
	}

	// start consumer with a goroutine (blocks)
	go func() {
		err := c.Start()
		panic(err)
	}()

	// wait for bad things to happen
	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-wait
	c.Stop()
}

func TestProcessor_ProducerError(t *testing.T) {

	t.Run("SetValue", func(t *testing.T) {
		km := NewKafkaMock(t, "test")
		km.ReplaceEmitHandler(func(topic, key string, value []byte) *kafka.Promise {
			return kafka.NewPromise().Finish(errors.New("producer error"))
		})

		consume := func(ctx Context, msg interface{}) {
			ctx.SetValue(msg)
		}

		proc, err := NewProcessor([]string{"broker"},
			DefineGroup("test",
				Input("topic", new(codec.String), consume),
				Persist(new(codec.String)),
			),
			km.ProcessorOptions()...,
		)

		ensure.Nil(t, err)
		var (
			processorErrors error
			done            = make(chan struct{})
		)
		go func() {
			processorErrors = proc.Start()
			close(done)
		}()

		km.ConsumeString("topic", "key", "world")

		proc.Stop()
		<-done
		ensure.True(t, processorErrors != nil)
	})

	t.Run("Emit", func(t *testing.T) {
		km := NewKafkaMock(t, "test")
		km.ReplaceEmitHandler(func(topic, key string, value []byte) *kafka.Promise {
			return kafka.NewPromise().Finish(errors.New("producer error"))
		})

		consume := func(ctx Context, msg interface{}) {
			ctx.Emit("blubbb", "key", []byte("some message is emitted"))
		}

		proc, err := NewProcessor([]string{"broker"},
			DefineGroup("test",
				Input("topic", new(codec.String), consume),
				Persist(new(codec.String)),
			),
			km.ProcessorOptions()...,
		)

		ensure.Nil(t, err)
		var (
			processorErrors error
			done            = make(chan struct{})
		)
		go func() {
			processorErrors = proc.Start()
			close(done)
		}()

		km.ConsumeString("topic", "key", "world")

		proc.Stop()
		<-done
		ensure.True(t, processorErrors != nil)
	})

	t.Run("Value-stateless", func(t *testing.T) {
		km := NewKafkaMock(t, "test")
		km.ReplaceEmitHandler(func(topic, key string, value []byte) *kafka.Promise {
			return kafka.NewPromise().Finish(errors.New("producer error"))
		})

		consume := func(ctx Context, msg interface{}) {
			func() {
				defer PanicStringContains(t, "stateless")
				_ = ctx.Value()
			}()
		}

		proc, err := NewProcessor([]string{"broker"},
			DefineGroup("test",
				Input("topic", new(codec.String), consume),
			),
			append(km.ProcessorOptions())...,
		)

		ensure.Nil(t, err)
		var (
			processorErrors error
			done            = make(chan struct{})
		)
		go func() {
			processorErrors = proc.Start()
			close(done)
		}()

		km.ConsumeString("topic", "key", "world")

		// stopping the processor. It should actually not produce results
		proc.Stop()
		<-done
		ensure.Nil(t, processorErrors)
	})

}
func TestProcessor_consumeFail(t *testing.T) {
	km := NewKafkaMock(t, "test")

	consume := func(ctx Context, msg interface{}) {
		ctx.Fail(errors.New("consume-failed"))
	}

	proc, err := NewProcessor([]string{"broker"},
		DefineGroup("test",
			Input("topic", new(codec.String), consume),
		),
		append(km.ProcessorOptions())...,
	)

	ensure.Nil(t, err)
	var (
		processorErrors error
		done            = make(chan struct{})
	)
	go func() {
		processorErrors = proc.Start()
		close(done)
	}()

	km.ConsumeString("topic", "key", "world")

	proc.Stop()
	<-done
	ensure.True(t, strings.Contains(processorErrors.Error(), "consume-failed"))
}

func TestProcessor_consumePanic(t *testing.T) {
	km := NewKafkaMock(t, "test")

	consume := func(ctx Context, msg interface{}) {
		panic("panicking")
	}

	proc, err := NewProcessor([]string{"broker"},
		DefineGroup("test",
			Input("topic", new(codec.String), consume),
		),
		append(km.ProcessorOptions())...,
	)

	ensure.Nil(t, err)
	var (
		processorErrors error
		done            = make(chan struct{})
	)
	go func() {
		processorErrors = proc.Start()
		close(done)
	}()

	km.ConsumeString("topic", "key", "world")

	proc.Stop()
	<-done
	ensure.NotNil(t, processorErrors)
	ensure.True(t, strings.Contains(processorErrors.Error(), "panicking"))
}

func TestProcessor_failOnRecover(t *testing.T) {
	var (
		recovered       int
		processorErrors error
		done            = make(chan struct{})
		msgToRecover    = 100
	)

	km := NewKafkaMock(t, "test")

	consume := func(ctx Context, msg interface{}) {
		log.Println("consuming message..", ctx.Key())
	}

	km.SetGroupTableCreator(func() (string, []byte) {
		time.Sleep(10 * time.Millisecond)
		recovered++
		if recovered > msgToRecover {
			return "", nil
		}
		return "key", []byte(fmt.Sprintf("state-%d", recovered))
	})

	proc, err := NewProcessor([]string{"broker"},
		DefineGroup("test",
			Input("topic", new(codec.String), consume),
			Persist(rawCodec),
		),
		append(km.ProcessorOptions(),
			WithUpdateCallback(func(s storage.Storage, partition int32, key string, value []byte) error {
				log.Printf("recovered state: %s: %s", key, string(value))
				return nil
			}),
		)...,
	)

	ensure.Nil(t, err)

	go func() {
		processorErrors = proc.Start()
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	log.Println("stopping")
	proc.Stop()
	<-done
	log.Println("stopped")
	// make sure the recovery was aborted
	ensure.True(t, recovered < msgToRecover)
}
