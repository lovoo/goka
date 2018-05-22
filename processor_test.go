package goka

import (
	"context"
	"errors"
	"fmt"
	"hash"
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
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
	"github.com/lovoo/goka/tester"

	"github.com/facebookgo/ensure"
	"github.com/golang/mock/gomock"
)

var (
	rawCodec = new(codec.Bytes)
)

func nullStorageBuilder() storage.Builder {
	return func(topic string, partition int32) (storage.Storage, error) {
		return &storage.Null{}, nil
	}
}

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

func createStorageBuilder(st storage.Storage) storage.Builder {
	return func(topic string, partition int32) (storage.Storage, error) {
		return st, nil
	}
}

func createConsumerBuilder(c kafka.Consumer) kafka.ConsumerBuilder {
	return func(b []string, g, id string) (kafka.Consumer, error) {
		return c, nil
	}
}

func createFailedConsumerBuilder() kafka.ConsumerBuilder {
	return func(b []string, g, id string) (kafka.Consumer, error) {
		return nil, errors.New("failed creating consumer")
	}
}

func createProducerBuilder(p kafka.Producer) kafka.ProducerBuilder {
	return func(b []string, id string, hasher func() hash.Hash32) (kafka.Producer, error) {
		return p, nil
	}
}

func createFailedProducerBuilder() kafka.ProducerBuilder {
	return func(b []string, id string, hasher func() hash.Hash32) (kafka.Producer, error) {
		return nil, errors.New("failed creating producer")
	}
}

func createTopicManagerBuilder(tm kafka.TopicManager) kafka.TopicManagerBuilder {
	return func(b []string) (kafka.TopicManager, error) {
		return tm, nil
	}
}

func createFailedTopicManagerBuilder(tm kafka.TopicManager) kafka.TopicManagerBuilder {
	return func(b []string) (kafka.TopicManager, error) {
		return nil, errors.New("failed creating topic manager")
	}
}

func createProcessorStateless(ctrl *gomock.Controller, consumer kafka.Consumer, producer kafka.Producer, npar int) *Processor {
	tm := mock.NewMockTopicManager(ctrl)

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
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
		WithConsumerBuilder(createConsumerBuilder(consumer)),
		WithProducerBuilder(createProducerBuilder(producer)),
		WithPartitionChannelSize(0),
	)
	return p
}

func createProcessor(t *testing.T, ctrl *gomock.Controller, consumer kafka.Consumer, npar int, sb storage.Builder) *Processor {
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
	p, err := NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Input(topic2, rawCodec, cb),
			Loop(rawCodec, cb),
			Persist(new(codec.String)),
		),
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
		WithConsumerBuilder(createConsumerBuilder(consumer)),
		WithProducerBuilder(createProducerBuilder(producer)),
		WithStorageBuilder(sb),
		WithPartitionChannelSize(0),
	)
	ensure.Nil(t, err)
	return p
}

func createProcessorWithTable(t *testing.T, ctrl *gomock.Controller, consumer kafka.Consumer, producer kafka.Producer, npar int, sb storage.Builder) *Processor {
	tm := mock.NewMockTopicManager(ctrl)

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
	p, err := NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Input(topic2, rawCodec, cb),
			Loop(rawCodec, cb),
			Join(table, rawCodec),
			Persist(rawCodec),
		),
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
		WithConsumerBuilder(createConsumerBuilder(consumer)),
		WithProducerBuilder(createProducerBuilder(producer)),
		WithStorageBuilder(sb),
		WithPartitionChannelSize(0),
	)
	ensure.Nil(t, err)
	return p
}

func createProcessorWithLookupTable(t *testing.T, ctrl *gomock.Controller, consumer kafka.Consumer, npar int, sb storage.Builder) *Processor {
	tm := mock.NewMockTopicManager(ctrl)
	producer := mock.NewMockProducer(ctrl)

	var partitions []int32
	for i := 0; i < npar; i++ {
		partitions = append(partitions, int32(i))
	}

	// successfully create processor
	tm.EXPECT().Partitions(topic).Return(partitions, nil)
	tm.EXPECT().Partitions(table).Return(partitions, nil)
	tm.EXPECT().Close().Return(nil).Times(2)
	p, err := NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Lookup(table, rawCodec),
		),
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
		WithConsumerBuilder(createConsumerBuilder(consumer)),
		WithProducerBuilder(createProducerBuilder(producer)),
		WithStorageBuilder(sb),
		WithPartitionChannelSize(0),
	)
	ensure.Nil(t, err)
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
	table2 = "table2"
)

func TestProcessor_process(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		wg       sync.WaitGroup
		st       = mock.NewMockStorage(ctrl)
		consumer = mock.NewMockConsumer(ctrl)
		producer = mock.NewMockProducer(ctrl)
		pstats   = newPartitionStats()
	)

	p := &Processor{
		graph: DefineGroup(group,
			Persist(new(codec.String)),
			Loop(c, cb),
			Input("sometopic", rawCodec, cb),
			Output("anothertopic", new(codec.String)),
		),

		consumer: consumer,
		producer: producer,
	}

	// no emits
	consumer.EXPECT().Commit("sometopic", int32(1), int64(123))
	msg := &message{Topic: "sometopic", Partition: 1, Offset: 123, Data: []byte("something")}
	updates, err := p.process(msg, st, &wg, pstats)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, updates, 0)

	// emit something
	promise := new(kafka.Promise)
	gomock.InOrder(
		producer.EXPECT().Emit("anothertopic", "key", []byte("message")).Return(promise),
		consumer.EXPECT().Commit("sometopic", int32(1), int64(123)),
	)
	msg = &message{Topic: "sometopic", Partition: 1, Offset: 123, Data: []byte("something")}
	p.graph.callbacks["sometopic"] = func(ctx Context, msg interface{}) {
		ctx.Emit("anothertopic", "key", "message")
	}
	updates, err = p.process(msg, st, &wg, pstats)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, updates, 0)
	promise.Finish(nil)

	// store something
	promise = new(kafka.Promise)
	gomock.InOrder(
		st.EXPECT().Set("key", []byte("message")),
		producer.EXPECT().Emit(tableName(group), "key", []byte("message")).Return(promise),
		st.EXPECT().GetOffset(int64(0)).Return(int64(321), nil),
		st.EXPECT().SetOffset(int64(322)),
		consumer.EXPECT().Commit("sometopic", int32(1), int64(123)),
	)
	msg = &message{Topic: "sometopic", Key: "key", Partition: 1, Offset: 123, Data: []byte("something")}
	p.graph.callbacks["sometopic"] = func(ctx Context, msg interface{}) {
		ctx.SetValue("message")
	}
	updates, err = p.process(msg, st, &wg, pstats)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, updates, 1)
	promise.Finish(nil)

	// store something twice
	promise = new(kafka.Promise)
	promise2 := new(kafka.Promise)
	gomock.InOrder(
		st.EXPECT().Set("key", []byte("message")),
		producer.EXPECT().Emit(tableName(group), "key", []byte("message")).Return(promise),
		st.EXPECT().Set("key", []byte("message2")),
		producer.EXPECT().Emit(tableName(group), "key", []byte("message2")).Return(promise2),
		st.EXPECT().GetOffset(int64(0)).Return(int64(321), nil),
		st.EXPECT().SetOffset(int64(323)),
		consumer.EXPECT().Commit("sometopic", int32(1), int64(123)),
	)
	msg = &message{Topic: "sometopic", Key: "key", Partition: 1, Offset: 123, Data: []byte("something")}
	p.graph.callbacks["sometopic"] = func(ctx Context, msg interface{}) {
		ctx.SetValue("message")
		ctx.SetValue("message2")
	}
	updates, err = p.process(msg, st, &wg, pstats)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, updates, 2)
	promise.Finish(nil)
	promise2.Finish(nil)

}

func TestProcessor_processFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		wg       sync.WaitGroup
		st       = mock.NewMockStorage(ctrl)
		consumer = mock.NewMockConsumer(ctrl)
		producer = mock.NewMockProducer(ctrl)
		pstats   = newPartitionStats()
	)
	var canceled chan bool
	newProcessor := func() *Processor {
		canceled = make(chan bool)
		p := &Processor{
			graph: DefineGroup(group,
				Persist(new(codec.String)),
				Loop(c, cb),
				Input("sometopic", rawCodec, cb),
				Output("anothertopic", new(codec.String)),
			),

			consumer: consumer,
			producer: producer,
			opts:     new(poptions),

			errors: new(multierr.Errors),
			cancel: func() { close(canceled) },
		}

		p.opts.log = logger.Default()
		return p
	}
	// fail get offset
	p := newProcessor()
	promise := new(kafka.Promise)
	gomock.InOrder(
		st.EXPECT().Set("key", []byte("message")),
		producer.EXPECT().Emit(tableName(group), "key", []byte("message")).Return(promise),
		st.EXPECT().GetOffset(int64(0)).Return(int64(321), errors.New("getOffset failed")),
	)
	msg := &message{Topic: "sometopic", Key: "key", Partition: 1, Offset: 123, Data: []byte("something")}
	p.graph.callbacks["sometopic"] = func(ctx Context, msg interface{}) {
		ctx.SetValue("message")
	}
	updates, err := p.process(msg, st, &wg, pstats)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, updates, 1)
	promise.Finish(nil)
	err = doTimed(t, func() {
		<-canceled
	})
	ensure.Nil(t, err)

	// fail set offset
	promise = new(kafka.Promise)
	p = newProcessor()
	gomock.InOrder(
		st.EXPECT().Set("key", []byte("message")),
		producer.EXPECT().Emit(tableName(group), "key", []byte("message")).Return(promise),
		st.EXPECT().GetOffset(int64(0)).Return(int64(321), nil),
		st.EXPECT().SetOffset(int64(322)).Return(errors.New("setOffset failed")),
	)
	msg = &message{Topic: "sometopic", Key: "key", Partition: 1, Offset: 123, Data: []byte("something")}
	p.graph.callbacks["sometopic"] = func(ctx Context, msg interface{}) {
		ctx.SetValue("message")
	}
	updates, err = p.process(msg, st, &wg, pstats)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, updates, 1)
	promise.Finish(nil)
	err = doTimed(t, func() {
		<-canceled
	})
	ensure.Nil(t, err)

	// fail commit
	promise = new(kafka.Promise)
	p = newProcessor()
	gomock.InOrder(
		st.EXPECT().Set("key", []byte("message")),
		producer.EXPECT().Emit(tableName(group), "key", []byte("message")).Return(promise),
		st.EXPECT().GetOffset(int64(0)).Return(int64(321), nil),
		st.EXPECT().SetOffset(int64(322)),
		consumer.EXPECT().Commit("sometopic", int32(1), int64(123)).Return(errors.New("commit error")),
	)
	msg = &message{Topic: "sometopic", Key: "key", Partition: 1, Offset: 123, Data: []byte("something")}
	p.graph.callbacks["sometopic"] = func(ctx Context, msg interface{}) {
		ctx.SetValue("message")
	}
	updates, err = p.process(msg, st, &wg, pstats)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, updates, 1)
	promise.Finish(nil)
	err = doTimed(t, func() {
		<-canceled
	})
	ensure.Nil(t, err)

	// fail with panic (ctx.Fatal)
	p = newProcessor()
	// we dont add expect consumer.EXPECT().Commit() here, so if the context
	// would call it, the test would fail
	p.graph.callbacks["sometopic"] = func(ctx Context, msg interface{}) {
		ctx.Fail(errSome)
		t.Errorf("should never reach this point")
		t.Fail()
	}
	go func() {
		defer func() {
			if x := recover(); x != nil {
				ensure.StringContains(t, fmt.Sprintf("%v", x), errSome.Error())
			}
		}()
		updates, err = p.process(msg, st, &wg, pstats)
	}()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, updates, 1)
	err = doTimed(t, func() {
		<-canceled
	})
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

	// error in applyOptions
	_, err = NewProcessor(nil,
		DefineGroup(group, Input(topic, rawCodec, cb)),
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
		func(o *poptions) {
			o.builders.storage = nil
		},
	)
	ensure.NotNil(t, err)

	// error creating topic manager
	_, err = NewProcessor(nil,
		DefineGroup(group, Input(topic, rawCodec, cb)),
		WithTopicManagerBuilder(createFailedTopicManagerBuilder(tm)),
	)
	ensure.NotNil(t, err)

	// error closing topic manager
	tm.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)
	tm.EXPECT().Close().Return(errSome)
	_, err = NewProcessor(nil,
		DefineGroup(group, Input(topic, rawCodec, cb)),
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
	)
	ensure.NotNil(t, err)

	// error calling Partitions in copartitioned
	tm.EXPECT().Partitions(topic).Return([]int32{0, 1}, errSome)
	tm.EXPECT().Close().Return(nil)
	_, err = NewProcessor(nil,
		DefineGroup(group, Input(topic, rawCodec, cb)),
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
	)
	ensure.NotNil(t, err)

	// error with partition gap in copartitioned
	tm.EXPECT().Partitions(topic).Return([]int32{0, 2}, nil)
	tm.EXPECT().Close().Return(nil)
	_, err = NewProcessor(nil,
		DefineGroup(group, Input(topic, rawCodec, cb)),
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
	)
	ensure.NotNil(t, err)

	// error with non-copartitioned topics
	tm.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)
	tm.EXPECT().Partitions(topic2).Return([]int32{0, 1, 2}, nil)
	tm.EXPECT().Close().Return(nil)
	_, err = NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Input(topic2, rawCodec, cb),
		),
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
	)
	ensure.NotNil(t, err)

	// error ensuring streams
	tm.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)
	tm.EXPECT().EnsureStreamExists("group-loop", 2).Return(errSome)
	tm.EXPECT().Close().Return(nil)
	_, err = NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Loop(rawCodec, cb),
		),
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
	)
	ensure.NotNil(t, err)

	// error ensuring table
	tm.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)
	tm.EXPECT().EnsureTableExists("group-table", 2).Return(errSome)
	tm.EXPECT().Close().Return(nil)
	_, err = NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Persist(rawCodec),
		),
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
		WithStorageBuilder(storage.MemoryBuilder()),
	)
	ensure.NotNil(t, err)

	// error creating views
	tm.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)
	// lookup table is allowed to be not copartitioned with input
	tm.EXPECT().Partitions(table).Return([]int32{0, 1, 2}, errSome)
	tm.EXPECT().Close().Return(nil).Times(2)
	_, err = NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Lookup(table, rawCodec)),
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
		WithStorageBuilder(storage.MemoryBuilder()),
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
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
		WithStorageBuilder(storage.MemoryBuilder()),
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
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
		WithConsumerBuilder(createConsumerBuilder(consumer)),
		WithProducerBuilder(createProducerBuilder(producer)),
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
	tm.EXPECT().Partitions(table2).Return([]int32{0, 1, 2}, nil)
	tm.EXPECT().Close().Return(nil).Times(2)
	p, err = NewProcessor(nil,
		DefineGroup(group,
			Input(topic, rawCodec, cb),
			Join(table, rawCodec),
			Lookup(table2, rawCodec),
		),
		WithTopicManagerBuilder(createTopicManagerBuilder(tm)),
		WithConsumerBuilder(createConsumerBuilder(consumer)),
		WithProducerBuilder(createProducerBuilder(producer)),
		WithStorageBuilder(storage.MemoryBuilder()),
	)
	ensure.Nil(t, err)
	ensure.True(t, p.graph.GroupTable() == nil)
	ensure.True(t, p.graph.LoopStream() == nil)
	ensure.True(t, p.partitionCount == 2)
	ensure.True(t, len(p.views[table2].partitions) == 3)
	ensure.True(t, len(p.graph.copartitioned()) == 2)
	ensure.True(t, len(p.graph.inputs()) == 3)
	ensure.True(t, p.isStateless())
}

func TestProcessor_StartFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	consumer := mock.NewMockConsumer(ctrl)

	// error creating consumer
	done := make(chan bool)
	p := createProcessor(t, ctrl, consumer, 2, nullStorageBuilder())
	p.opts.builders.consumer = createFailedConsumerBuilder()
	go func() {
		errs := p.Run(context.Background())
		ensure.NotNil(t, errs)
		close(done)
	}()
	err := doTimed(t, func() { <-done })
	ensure.Nil(t, err)

	// error creating producer and closing consumer
	done = make(chan bool)
	p = createProcessor(t, ctrl, consumer, 2, nullStorageBuilder())
	p.opts.builders.producer = createFailedProducerBuilder()
	consumer.EXPECT().Close().Return(errSome)
	go func() {
		errs := p.Run(context.Background())
		ensure.NotNil(t, errs)
		ensure.StringContains(t, errs.Error(), "creating producer")
		ensure.StringContains(t, errs.Error(), "closing consumer")
		close(done)
	}()
	err = doTimed(t, func() { <-done })
	ensure.Nil(t, err)

	// error starting lookup tables and closing producer
	done = make(chan bool)
	st := mock.NewMockStorage(ctrl)
	p = createProcessorWithLookupTable(t, ctrl, consumer, 2, createStorageBuilder(st))
	producer := mock.NewMockProducer(ctrl)
	//	tm := mock.NewMockTopicManager(ctrl)
	p.opts.builders.producer = createProducerBuilder(producer)
	//	p.opts.builders.topicmgr = createTopicManagerBuilder(tm)
	wait := make(chan bool)
	ch := make(chan kafka.Event)
	consumer.EXPECT().Subscribe(map[string]int64{topic: -1}).Return(nil)
	consumer.EXPECT().Events().Return(ch).Do(func() { <-wait }).Times(2) // view + processor
	st.EXPECT().Open().Do(func() { close(wait) }).Return(errSome)
	st.EXPECT().Open().Return(errSome)
	st.EXPECT().Close().Times(2)
	consumer.EXPECT().Close().Return(nil).Times(2) // view + processor
	producer.EXPECT().Close().Return(errSome)
	go func() {
		errs := p.Run(context.Background())
		ensure.NotNil(t, errs)
		ensure.StringContains(t, errs.Error(), "closing producer")
		ensure.StringContains(t, errs.Error(), "opening storage")
		close(done)
	}()
	err = doTimed(t, func() { <-done })
	ensure.Nil(t, err)

	// error subscribing topics
	done = make(chan bool)
	p = createProcessor(t, ctrl, consumer, 2, nullStorageBuilder())
	consumer.EXPECT().Subscribe(topOff).Return(errSome)
	consumer.EXPECT().Close().Return(nil)
	go func() {
		errs := p.Run(context.Background())
		ensure.NotNil(t, errs)
		ensure.StringContains(t, errs.Error(), "subscribing topics")
		close(done)
	}()
	err = doTimed(t, func() { <-done })
	ensure.Nil(t, err)
}

func TestProcessor_StartStopEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		consumer = mock.NewMockConsumer(ctrl)
		wait     = make(chan bool)
		final    = make(chan bool)
		ch       = make(chan kafka.Event)
		p        = createProcessor(t, ctrl, consumer, 2, nullStorageBuilder())
	)

	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch).Do(func() { close(wait) })
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := p.Run(ctx)
		ensure.Nil(t, err)
		close(final)
	}()

	consumer.EXPECT().Close().Return(nil).Do(func() { close(ch) })
	err := doTimed(t, func() {
		<-wait
		cancel()
		<-final
	})
	ensure.Nil(t, err)
}

func TestProcessor_StartStopEmptyError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		consumer = mock.NewMockConsumer(ctrl)
		final    = make(chan bool)
		wait     = make(chan bool)
		ch       = make(chan kafka.Event)
		p        = createProcessor(t, ctrl, consumer, 2, nullStorageBuilder())
	)

	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch).Do(func() { close(wait) })
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := p.Run(ctx)
		ensure.NotNil(t, err)
		close(final)
	}()

	consumer.EXPECT().Close().Return(errors.New("some error")).Do(func() { close(ch) })
	err := doTimed(t, func() {
		<-wait
		cancel()
		<-final
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
				sb       = func(topic string, par int32) (storage.Storage, error) {
					return st, nil
				}
				final = make(chan bool)
				ch    = make(chan kafka.Event)
				p     = createProcessor(t, ctrl, consumer, 3, sb)
			)

			gomock.InOrder(
				consumer.EXPECT().Subscribe(topOff).Return(nil),
				consumer.EXPECT().Events().Return(ch),
				consumer.EXPECT().Close().Do(func() { close(ch) }),
			)
			go func() {
				err = p.Run(context.Background())
				ensure.NotNil(t, err)
				close(final)
			}()

			ch <- tc.event

			err = doTimed(t, func() {
				<-final
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
		sb       = func(topic string, par int32) (storage.Storage, error) {
			return st, nil
		}
		final = make(chan bool)
		ch    = make(chan kafka.Event)
		p     = createProcessor(t, ctrl, consumer, 3, sb)
		value = []byte("value")
	)
	// -- expectations --
	// 1. start
	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch).AnyTimes()
	// 2. rebalance
	st.EXPECT().Open().Times(3)
	st.EXPECT().GetOffset(int64(-2)).Return(int64(123), nil).Times(3)
	consumer.EXPECT().AddPartition(tableName(group), int32(0), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(1), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(2), int64(123))
	// 3. message
	gomock.InOrder(
		st.EXPECT().Set("key", value).Return(nil),
		st.EXPECT().SetOffset(int64(1)),
		st.EXPECT().MarkRecovered(),
	)
	// 4. error
	consumer.EXPECT().RemovePartition(tableName(group), int32(0))
	consumer.EXPECT().RemovePartition(tableName(group), int32(1))
	consumer.EXPECT().RemovePartition(tableName(group), int32(2))
	st.EXPECT().Close().Times(3)
	consumer.EXPECT().Close().Do(func() { close(ch) })

	// -- test --
	// 1. start
	go func() {
		err = p.Run(context.Background())
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
		Value:     value,
	}
	err = syncWith(t, ch, 1) // with partition
	ensure.Nil(t, err)

	// 4. receive error
	ch <- new(kafka.Error)

	// 5. stop
	err = doTimed(t, func() { <-final })
	ensure.Nil(t, err)
}

// start processor with table and receives an error from Kafka in the events
// channel after rebalance.
func TestProcessor_StartWithTableWithErrorAfterRebalance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		err      error
		consumer = mock.NewMockConsumer(ctrl)
		producer = mock.NewMockProducer(ctrl)
		st       = mock.NewMockStorage(ctrl)
		sb       = func(topic string, par int32) (storage.Storage, error) {
			return st, nil
		}
		final     = make(chan bool)
		ch        = make(chan kafka.Event)
		p         = createProcessorWithTable(t, ctrl, consumer, producer, 3, sb)
		value     = []byte("value")
		blockit   = make(chan bool)
		unblocked = make(chan bool)
	)
	p.graph.callbacks[topic] = func(ctx Context, msg interface{}) {
		fmt.Println("hallodfads", msg)
		defer close(unblocked)
		<-blockit
		fmt.Println("unblocked")
	}

	// -- expectations --
	// 1. start
	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch).AnyTimes()
	// 2. rebalance
	st.EXPECT().Open().Times(6)
	st.EXPECT().GetOffset(int64(-2)).Return(int64(123), nil).Times(6)
	consumer.EXPECT().AddPartition(tableName(group), int32(0), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(1), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(2), int64(123))
	consumer.EXPECT().AddPartition(table, int32(0), int64(123))
	consumer.EXPECT().AddPartition(table, int32(1), int64(123))
	consumer.EXPECT().AddPartition(table, int32(2), int64(123))
	// 3. EOF messages
	st.EXPECT().MarkRecovered().Times(3)
	// 4. messages
	consumer.EXPECT().Commit(topic, int32(1), int64(2))
	// 5. error
	consumer.EXPECT().Close().Do(func() { close(ch) })
	consumer.EXPECT().RemovePartition(tableName(group), int32(0))
	consumer.EXPECT().RemovePartition(tableName(group), int32(1))
	consumer.EXPECT().RemovePartition(tableName(group), int32(2))
	consumer.EXPECT().RemovePartition(table, int32(0))
	consumer.EXPECT().RemovePartition(table, int32(1))
	consumer.EXPECT().RemovePartition(table, int32(2))
	st.EXPECT().Close().Times(6)
	producer.EXPECT().Close()

	// -- test --
	// 1. start
	go func() {
		err = p.Run(context.Background())
		ensure.NotNil(t, err)
		close(final)
	}()

	// 2. rebalance
	ensure.True(t, len(p.partitions) == 0)
	ensure.True(t, len(p.partitionViews) == 0)
	ch <- (*kafka.Assignment)(&map[int32]int64{0: -1, 1: -1, 2: -1})
	err = syncWith(t, ch, -1) // with processor
	ensure.Nil(t, err)
	ensure.True(t, len(p.partitions) == 3)
	ensure.True(t, len(p.partitionViews) == 3)

	// 3. message
	ch <- &kafka.EOF{
		Topic:     tableName(group),
		Hwm:       0,
		Partition: 0,
	}
	err = syncWith(t, ch, 0) // with partition
	ensure.Nil(t, err)
	ch <- &kafka.EOF{
		Topic:     tableName(group),
		Hwm:       0,
		Partition: 1,
	}
	err = syncWith(t, ch, 1) // with partition
	ensure.Nil(t, err)
	ch <- &kafka.EOF{
		Topic:     tableName(group),
		Hwm:       0,
		Partition: 2,
	}
	err = syncWith(t, ch, 2) // with partition
	ensure.Nil(t, err)

	// 4. heavy message
	ch <- &kafka.Message{
		Topic:     topic,
		Partition: 1,
		Offset:    2,
		Key:       "key",
		Value:     value,
	}
	// dont wait for that

	// 4. receive error
	ch <- new(kafka.Error)

	// sync with partition (should be unblocked)
	close(blockit)
	<-unblocked

	// 5. stop
	err = doTimed(t, func() {
		<-final
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
		sb       = func(topic string, par int32) (storage.Storage, error) {
			return st, nil
		}
		final = make(chan bool)
		ch    = make(chan kafka.Event)
		p     = createProcessor(t, ctrl, consumer, 3, sb)
		value = []byte("value")
	)

	// -- expectations --
	// 1. start
	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch).AnyTimes()
	// 2. rebalance
	st.EXPECT().Open().Times(3)
	st.EXPECT().GetOffset(int64(-2)).Return(int64(123), nil).Times(3)
	consumer.EXPECT().AddPartition(tableName(group), int32(0), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(1), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(2), int64(123))
	// 3. load message partition 1
	st.EXPECT().Set("key", value).Return(nil)
	st.EXPECT().SetOffset(int64(1))
	st.EXPECT().MarkRecovered()
	// 4. end of recovery partition 1
	gomock.InOrder(
		consumer.EXPECT().RemovePartition(tableName(group), int32(1)),
		consumer.EXPECT().AddGroupPartition(int32(1)),
	)
	// 5. process message partition 1
	consumer.EXPECT().Commit(topic, int32(1), int64(1))
	// 6. new assignment remove partition 1 and 2
	st.EXPECT().Close() // partition 1 close
	consumer.EXPECT().RemovePartition(tableName(group), int32(2))
	st.EXPECT().Close() // partition 2 close
	// 7. stop processor
	consumer.EXPECT().Close() //.Do(func() { close(ch) })
	consumer.EXPECT().RemovePartition(tableName(group), int32(0))
	st.EXPECT().Close()

	// -- test --
	ctx, cancel := context.WithCancel(context.Background())
	// 1. start
	go func() {
		err = p.Run(ctx)
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
		Value:     value,
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
		Value:     value,
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
		cancel()
		<-final
	})
	ensure.Nil(t, err)
}

func TestProcessor_StartStateless(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		consumer = mock.NewMockConsumer(ctrl)
		producer = mock.NewMockProducer(ctrl)
		final    = make(chan bool)
		ch       = make(chan kafka.Event)
		p        = createProcessorStateless(ctrl, consumer, producer, 3)
	)

	// -- expectactions --
	// 1. start
	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch).AnyTimes()
	// 2. rebalance
	consumer.EXPECT().AddGroupPartition(int32(0))
	consumer.EXPECT().AddGroupPartition(int32(1))
	// 3. stop processor
	consumer.EXPECT().Close().Return(nil).Do(func() { close(ch) })
	producer.EXPECT().Close().Return(nil)

	// -- test --
	ctx, cancel := context.WithCancel(context.Background())
	// 1. start
	go func() {
		err := p.Run(ctx)
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
		cancel()
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
		producer = mock.NewMockProducer(ctrl)
		st       = mock.NewMockStorage(ctrl)
		sb       = func(topic string, par int32) (storage.Storage, error) {
			return st, nil
		}
		final = make(chan bool)
		ch    = make(chan kafka.Event)
		p     = createProcessorWithTable(t, ctrl, consumer, producer, 3, sb)
		value = []byte("value")
	)

	// -- expectations --
	// 1. start
	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch).AnyTimes()
	// 2. rebalance
	st.EXPECT().Open().Times(6)
	st.EXPECT().GetOffset(int64(-2)).Return(int64(123), nil).Times(6)
	consumer.EXPECT().AddPartition(tableName(group), int32(0), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(1), int64(123))
	consumer.EXPECT().AddPartition(tableName(group), int32(2), int64(123))
	consumer.EXPECT().AddPartition(table, int32(0), int64(123))
	consumer.EXPECT().AddPartition(table, int32(1), int64(123)).Times(2)
	consumer.EXPECT().AddPartition(table, int32(2), int64(123))
	// 3. message to group table
	st.EXPECT().Set("key", value).Return(nil)
	st.EXPECT().SetOffset(int64(1))
	st.EXPECT().MarkRecovered()
	// 4. finish recovery of partition 1
	gomock.InOrder(
		consumer.EXPECT().RemovePartition(tableName(group), int32(1)),
		consumer.EXPECT().AddGroupPartition(int32(1)),
	)
	// 5. process messages in partition 1
	gomock.InOrder(
		consumer.EXPECT().Commit(topic, int32(1), int64(1)),
	)
	// 6. rebalance (only keep partition 0)
	st.EXPECT().Close().Times(4) // close group and other table partitions
	consumer.EXPECT().RemovePartition(table, int32(1)).Times(2)
	consumer.EXPECT().RemovePartition(table, int32(2))
	consumer.EXPECT().RemovePartition(tableName(group), int32(2))
	// 7. stop processor
	consumer.EXPECT().Close().Do(func() { close(ch) })
	consumer.EXPECT().RemovePartition(table, int32(0))
	consumer.EXPECT().RemovePartition(tableName(group), int32(0))
	st.EXPECT().MarkRecovered()
	st.EXPECT().Close().Times(2) // close group table and other table
	producer.EXPECT().Close().Return(nil)

	// -- test --
	ctx, cancel := context.WithCancel(context.Background())
	// 1. start
	go func() {
		procErrs := p.Run(ctx)
		ensure.Nil(t, procErrs)
		close(final)
	}()

	// 2. rebalance
	ensure.True(t, len(p.partitions) == 0)
	ch <- (*kafka.Assignment)(&map[int32]int64{0: -1, 1: -1, 2: -1})
	err = syncWith(t, ch)
	ensure.Nil(t, err)
	ensure.True(t, len(p.partitions) == 3)

	// 3. message to group table
	ch <- &kafka.Message{
		Topic:     tableName(group),
		Partition: 1,
		Offset:    1,
		Key:       "key",
		Value:     value,
	}
	err = syncWith(t, ch, 1)
	ensure.Nil(t, err)

	// 4. finish recovery of partition 1
	ch <- &kafka.EOF{
		Partition: 1,
	}
	ensure.False(t, p.partitionViews[1][table].recovered())
	time.Sleep(delayProxyInterval)
	ensure.False(t, p.partitionViews[1][table].recovered())
	ch <- &kafka.EOF{
		Topic:     table,
		Partition: 1,
		Hwm:       123,
	}
	err = syncWith(t, ch)
	ensure.Nil(t, err)
	time.Sleep(delayProxyInterval)
	ensure.True(t, p.partitionViews[1][table].recovered())

	// 5. process messages in partition 1
	ch <- &kafka.Message{
		Topic:     topic,
		Partition: 1,
		Offset:    1,
		Key:       "key",
		Value:     value,
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
		cancel()
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
		p        = createProcessor(t, ctrl, consumer, 1,
			func(topic string, partition int32) (storage.Storage, error) {
				return nil, errors.New("some error")
			})
	)

	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch).AnyTimes()
	consumer.EXPECT().Close().Return(nil).Do(func() {
		close(ch)
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := p.Run(ctx)
		ensure.NotNil(t, err)
		close(wait)
	}()

	// assignment arrives
	ensure.True(t, len(p.partitions) == 0)
	ch <- (*kafka.Assignment)(&map[int32]int64{0: -1})

	// stop processor
	err := doTimed(t, func() {
		cancel()
		<-wait
	})
	ensure.Nil(t, err)
}

func TestProcessor_HasGet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		st = mock.NewMockStorage(ctrl)
		sb = func(topic string, partition int32) (storage.Storage, error) {
			return st, nil
		}
		consumer = mock.NewMockConsumer(ctrl)
		ch       = make(chan kafka.Event)
		wait     = make(chan bool)
		p        = createProcessor(t, ctrl, consumer, 1, sb)
	)

	ensure.True(t, p.partitionCount == 1)

	consumer.EXPECT().Subscribe(topOff).Return(nil)
	consumer.EXPECT().Events().Return(ch).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		procErrs := p.Run(ctx)
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
		st.EXPECT().Get("item1").Return([]byte("item1-value"), nil),
	)

	value, err := p.Get("item1")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, value.(string), "item1-value")

	// stop processor
	consumer.EXPECT().Close().Do(func() { close(ch) })
	consumer.EXPECT().RemovePartition(tableName(group), int32(0))
	st.EXPECT().Close()

	err = doTimed(t, func() {
		cancel()
		<-wait
	})
	ensure.Nil(t, err)
}

func TestProcessor_HasGetStateless(t *testing.T) {
	p := &Processor{graph: DefineGroup(group), opts: &poptions{hasher: DefaultHasher()}}
	_, err := p.Get("item1")
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "stateless processor")

	p = &Processor{graph: DefineGroup(group, Persist(c)), opts: &poptions{hasher: DefaultHasher()}}
	p.partitions = map[int32]*partition{
		0: new(partition),
	}
	p.partitionCount = 0
	_, err = p.Get("item1")
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "0 partitions")

	p = &Processor{graph: DefineGroup(group, Persist(c)), opts: &poptions{hasher: DefaultHasher()}}
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
	p = &Processor{graph: DefineGroup(group, Persist(c)), opts: &poptions{hasher: DefaultHasher()}}
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

func TestProcessor_StatelessContext(t *testing.T) {
	ctrl := mock.NewMockController(t)
	defer ctrl.Finish()
	var (
		tester = tester.New(t).SetCodec(new(codec.Bytes))
		//count int64
		//wait  = make(chan bool)
	)

	callPersist := func(ctx Context, message interface{}) {
		log.Println("processing")
		// call a random setvalue, this is expected to fail
		ctx.SetValue("value")
		t.Errorf("SetValue should panic. We should not have come to that point.")
	}

	proc, err := NewProcessor(nil,
		DefineGroup(
			"stateless-ctx",
			Input("input-topic", new(codec.Bytes), callPersist),
		),
		WithTester(tester),
	)
	ensure.Nil(t, err)
	done := make(chan bool)
	go func() {
		err = proc.Run(context.Background())
		ensure.NotNil(t, err)
		close(done)
	}()
	err = doTimed(t, func() {
		// consume a random key/message, the content doesn't matter as this should fail
		tester.ConsumeString("input-topic", "key", "msg")
		<-done
	})
	ensure.Nil(t, err)
}

func TestProcessor_ProducerError(t *testing.T) {

	t.Run("SetValue", func(t *testing.T) {
		tester := tester.New(t)
		tester.ReplaceEmitHandler(func(topic, key string, value []byte) *kafka.Promise {
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
			WithTester(tester),
		)

		ensure.Nil(t, err)
		var (
			processorErrors error
			done            = make(chan struct{})
			ctx, cancel     = context.WithCancel(context.Background())
		)
		go func() {
			processorErrors = proc.Run(ctx)
			close(done)
		}()

		tester.ConsumeString("topic", "key", "world")
		cancel()
		<-done
		ensure.True(t, processorErrors != nil)
	})

	t.Run("Emit", func(t *testing.T) {
		tester := tester.New(t)
		tester.ReplaceEmitHandler(func(topic, key string, value []byte) *kafka.Promise {
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
			WithTester(tester),
		)

		ensure.Nil(t, err)
		var (
			processorErrors error
			done            = make(chan struct{})
			ctx, cancel     = context.WithCancel(context.Background())
		)
		go func() {
			processorErrors = proc.Run(ctx)
			close(done)
		}()

		tester.ConsumeString("topic", "key", "world")

		cancel()
		<-done
		ensure.True(t, processorErrors != nil)
	})

	t.Run("Value-stateless", func(t *testing.T) {
		tester := tester.New(t)
		tester.ReplaceEmitHandler(func(topic, key string, value []byte) *kafka.Promise {
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
			WithTester(tester),
		)

		ensure.Nil(t, err)
		var (
			processorErrors error
			done            = make(chan struct{})
			ctx, cancel     = context.WithCancel(context.Background())
		)
		go func() {
			processorErrors = proc.Run(ctx)
			close(done)
		}()

		tester.ConsumeString("topic", "key", "world")

		// stopping the processor. It should actually not produce results
		cancel()
		<-done
		ensure.Nil(t, processorErrors)
	})

}

func TestProcessor_consumeFail(t *testing.T) {
	tester := tester.New(t)

	consume := func(ctx Context, msg interface{}) {
		ctx.Fail(errors.New("consume-failed"))
	}

	proc, err := NewProcessor([]string{"broker"},
		DefineGroup("test",
			Input("topic", new(codec.String), consume),
		),
		WithTester(tester),
	)

	ensure.Nil(t, err)
	var (
		processorErrors error
		done            = make(chan struct{})
		ctx, cancel     = context.WithCancel(context.Background())
	)
	go func() {
		processorErrors = proc.Run(ctx)
		close(done)
	}()

	tester.ConsumeString("topic", "key", "world")

	cancel()
	<-done
	ensure.True(t, strings.Contains(processorErrors.Error(), "consume-failed"))
}

func TestProcessor_consumePanic(t *testing.T) {
	tester := tester.New(t)

	consume := func(ctx Context, msg interface{}) {
		panic("panicking")
	}

	proc, err := NewProcessor([]string{"broker"},
		DefineGroup("test",
			Input("topic", new(codec.String), consume),
		),
		WithTester(tester),
	)

	ensure.Nil(t, err)
	var (
		processorErrors error
		done            = make(chan struct{})
		ctx, cancel     = context.WithCancel(context.Background())
	)
	go func() {
		processorErrors = proc.Run(ctx)
		close(done)
	}()

	tester.ConsumeString("topic", "key", "world")

	cancel()
	<-done
	ensure.NotNil(t, processorErrors)
	ensure.True(t, strings.Contains(processorErrors.Error(), "panicking"))
}

type nilValue struct{}
type nilCodec struct{}

func (nc *nilCodec) Decode(data []byte) (interface{}, error) {
	if data == nil {
		return new(nilValue), nil
	}
	return data, nil
}
func (nc *nilCodec) Encode(val interface{}) ([]byte, error) {
	return nil, nil
}

func TestProcessor_consumeNil(t *testing.T) {

	tests := []struct {
		name     string
		cb       ProcessCallback
		handling NilHandling
		codec    Codec
	}{
		{
			"ignore",
			func(ctx Context, msg interface{}) {
				t.Error("should never call consume")
				t.Fail()
			},
			NilIgnore,
			new(codec.String),
		},
		{
			"process",
			func(ctx Context, msg interface{}) {
				if msg != nil {
					t.Errorf("message should be nil:%v", msg)
					t.Fail()
				}
			},
			NilProcess,
			new(codec.String),
		},
		{
			"decode",
			func(ctx Context, msg interface{}) {
				if _, ok := msg.(*nilValue); !ok {
					t.Errorf("message should be a decoded nil value: %T", msg)
					t.Fail()
				}
			},
			NilDecode,
			new(nilCodec),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tester := tester.New(t)
			proc, err := NewProcessor([]string{"broker"},
				DefineGroup("test",
					Input("topic", tc.codec, tc.cb),
				),
				WithTester(tester),
				WithNilHandling(tc.handling),
			)

			ensure.Nil(t, err)
			var (
				processorErrors error
				done            = make(chan struct{})
				ctx, cancel     = context.WithCancel(context.Background())
			)
			go func() {
				processorErrors = proc.Run(ctx)
				close(done)
			}()

			tester.Consume("topic", "key", nil)

			cancel()
			<-done
			ensure.Nil(t, processorErrors)
		})
	}
}

func TestProcessor_failOnRecover(t *testing.T) {
	var (
		recovered       int
		processorErrors error
		_               = processorErrors // make linter happy
		done            = make(chan struct{})
		msgToRecover    = 100
	)

	tester := tester.New(t)

	consume := func(ctx Context, msg interface{}) {
		log.Println("consuming message..", ctx.Key())
	}

	tester.SetGroupTableCreator(func() (string, []byte) {
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
		WithTester(tester),
		WithUpdateCallback(func(s storage.Storage, partition int32, key string, value []byte) error {
			log.Printf("recovered state: %s: %s", key, string(value))
			return nil
		}),
	)

	ensure.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		processorErrors = proc.Run(ctx)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	log.Println("stopping")
	cancel()
	<-done
	log.Println("stopped")
	// make sure the recovery was aborted
	ensure.True(t, recovered < msgToRecover)
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

	p, err := NewProcessor(brokers, DefineGroup(group, Input(topic, rawCodec, consume)))
	if err != nil {
		log.Fatalln(err)
	}

	// start consumer with a goroutine (blocks)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := p.Run(ctx)
		panic(err)
	}()

	// wait for bad things to happen
	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-wait
	cancel()
}
