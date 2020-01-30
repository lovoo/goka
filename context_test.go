package goka

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/facebookgo/ensure"
	"github.com/golang/mock/gomock"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/logger"
)

func newEmitter(err error, done func(err error)) emitter {
	return func(topic string, key string, value []byte) *Promise {
		p := NewPromise()
		if done != nil {
			p.Then(done)
		}
		return p.Finish(err)
	}
}

func newEmitterW(wg *sync.WaitGroup, err error, done func(err error)) emitter {
	return func(topic string, key string, value []byte) *Promise {
		wg.Add(1)
		p := NewPromise()
		if done != nil {
			p.Then(done)
		}
		return p.Finish(err)
	}
}

func TestContext_Emit(t *testing.T) {
	var (
		ack           = 0
		emitted       = 0
		group   Group = "some-group"
	)

	ctx := &cbContext{
		graph:         DefineGroup(group),
		commit:        func() { ack++ },
		wg:            &sync.WaitGroup{},
		partProcStats: newPartitionProcStats(),
	}

	// after that the message is processed
	ctx.emitter = newEmitter(nil, func(err error) {
		emitted++
		ensure.Nil(t, err)
	})

	ctx.start()
	ctx.emit("emit-topic", "key", []byte("value"))
	ctx.finish(nil)

	// we can now for all callbacks -- it should also guarantee a memory fence
	// to the emitted variable (which is not being locked)
	ctx.wg.Wait()

	// check everything is done
	ensure.DeepEqual(t, emitted, 1)
	ensure.DeepEqual(t, ack, 1)
}

func TestContext_Timestamp(t *testing.T) {
	ts := time.Now()

	ctx := &cbContext{
		msg: &sarama.ConsumerMessage{
			Timestamp: ts,
		},
	}

	ensure.DeepEqual(t, ctx.Timestamp(), ts)
}

func TestContext_EmitError(t *testing.T) {
	var (
		ack             = 0
		emitted         = 0
		errToEmit       = errors.New("some error")
		group     Group = "some-group"
	)

	// test error case
	ctx := &cbContext{
		graph:         DefineGroup(group, Persist(new(codec.String))),
		commit:        func() { ack++ },
		wg:            &sync.WaitGroup{},
		partProcStats: newPartitionProcStats(),
		failer: func(err error) {
			ensure.StringContains(t, err.Error(), errToEmit.Error())
		},
	}
	ctx.emitter = newEmitter(errToEmit, func(err error) {
		emitted++
		ensure.NotNil(t, err)
		ensure.DeepEqual(t, err, errToEmit)
	})

	ctx.start()
	ctx.emit("emit-topic", "key", []byte("value"))
	ctx.finish(nil)

	// we can now for all callbacks -- it should also guarantee a memory fence
	// to the emitted variable (which is not being locked)
	ctx.wg.Wait()

	// check everything is done
	ensure.DeepEqual(t, emitted, 1)

	// nothing should be committed here
	ensure.DeepEqual(t, ack, 0)

}

func TestContext_EmitToStateTopic(t *testing.T) {
	var (
		group Group = "some-group"
	)

	ctx := &cbContext{graph: DefineGroup(group, Persist(c), Loop(c, cb))}
	func() {
		defer ensure.PanicDeepEqual(t, errors.New("cannot emit to table topic (use SetValue instead)"))
		ctx.Emit(Stream(tableName(group)), "key", []byte("value"))
	}()
	func() {
		defer ensure.PanicDeepEqual(t, errors.New("cannot emit to loop topic (use Loopback instead)"))
		ctx.Emit(Stream(loopName(group)), "key", []byte("value"))
	}()
	func() {
		defer ensure.PanicDeepEqual(t, errors.New("cannot emit to empty topic"))
		ctx.Emit("", "key", []byte("value"))
	}()
}

func TestContext_GetSetStateless(t *testing.T) {
	// ctx stateless since no storage passed
	ctx := &cbContext{graph: DefineGroup("group"), msg: new(sarama.ConsumerMessage)}
	func() {
		defer PanicStringContains(t, "stateless")
		_ = ctx.Value()
	}()
	func() {
		defer PanicStringContains(t, "stateless")
		ctx.SetValue("whatever")
	}()
}

func TestContext_Delete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		offset       = int64(123)
		key          = "key"
		ack          = 0
		group  Group = "some-group"
		st           = NewMockStorage(ctrl)
		pt           = &PartitionTable{
			st: &storageProxy{
				Storage: st,
			},
		}
	)

	st.EXPECT().Delete(key).Return(nil)

	ctx := &cbContext{
		graph:  DefineGroup(group, Persist(new(codec.String))),
		wg:     new(sync.WaitGroup),
		commit: func() { ack++ },
		msg:    &sarama.ConsumerMessage{Offset: offset},
		table:  pt,
	}

	ctx.emitter = newEmitter(nil, nil)

	ctx.start()
	err := ctx.deleteKey(key)
	ensure.Nil(t, err)
	ctx.finish(nil)

	ctx.wg.Wait()

	ensure.DeepEqual(t, ctx.counters, struct {
		emits  int
		dones  int
		stores int
	}{1, 1, 1})
}

func TestContext_DeleteStateless(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		offset       = int64(123)
		key          = "key"
		group  Group = "some-group"
	)

	ctx := &cbContext{
		graph: DefineGroup(group),
		wg:    new(sync.WaitGroup),
		msg:   &sarama.ConsumerMessage{Offset: offset},
	}
	ctx.emitter = newEmitter(nil, nil)

	err := ctx.deleteKey(key)
	ensure.Err(t, err, regexp.MustCompile("^Cannot access state in stateless processor$"))
}

func TestContext_DeleteStorageError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		offset       = int64(123)
		key          = "key"
		group  Group = "some-group"
		st           = NewMockStorage(ctrl)
		pt           = &PartitionTable{
			st: &storageProxy{
				Storage: st,
			},
		}
		retErr = errors.New("storage error")
	)

	st.EXPECT().Delete(key).Return(retErr)

	ctx := &cbContext{
		graph: DefineGroup(group, Persist(new(codec.String))),
		wg:    new(sync.WaitGroup),
		msg:   &sarama.ConsumerMessage{Offset: offset},
		table: pt,
	}

	ctx.emitter = newEmitter(nil, nil)

	err := ctx.deleteKey(key)
	ensure.Err(t, err, regexp.MustCompile("^error deleting key \\(key\\) from storage: storage error$"))
}

func TestContext_Set(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		offset       = int64(123)
		ack          = 0
		key          = "key"
		value        = "value"
		group  Group = "some-group"
		st           = NewMockStorage(ctrl)
		pt           = &PartitionTable{
			st: &storageProxy{
				Storage: st,
			},
		}
	)
	st.EXPECT().Set(key, []byte(value)).Return(nil)

	ctx := &cbContext{
		graph:         DefineGroup(group, Persist(new(codec.String))),
		wg:            new(sync.WaitGroup),
		commit:        func() { ack++ },
		partProcStats: newPartitionProcStats(),
		msg:           &sarama.ConsumerMessage{Key: []byte(key), Offset: offset},
		table:         pt,
	}

	ctx.emitter = newEmitter(nil, nil)

	ctx.start()
	err := ctx.setValueForKey(key, value)
	ensure.Nil(t, err)
	ctx.finish(nil)

	ctx.wg.Wait()

	ensure.DeepEqual(t, ctx.counters, struct {
		emits  int
		dones  int
		stores int
	}{1, 1, 1})
	ensure.DeepEqual(t, ack, 1)
}

func TestContext_GetSetStateful(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		group  Group = "some-group"
		key          = "key"
		value        = "value"
		offset       = int64(123)
		wg           = new(sync.WaitGroup)
		st           = NewMockStorage(ctrl)
		pt           = &PartitionTable{
			st: &storageProxy{
				Storage: st,
			},
		}
	)

	st.EXPECT().Get(key).Return(nil, nil)
	st.EXPECT().Set(key, []byte(value)).Return(nil)
	st.EXPECT().Get(key).Return([]byte(value), nil)

	graph := DefineGroup(group, Persist(new(codec.String)))
	ctx := &cbContext{
		table:         pt,
		wg:            wg,
		graph:         graph,
		partProcStats: newPartitionProcStats(),
		msg:           &sarama.ConsumerMessage{Key: []byte(key), Offset: offset},
		emitter: func(tp string, k string, v []byte) *Promise {
			wg.Add(1)
			ensure.DeepEqual(t, tp, graph.GroupTable().Topic())
			ensure.DeepEqual(t, string(k), key)
			ensure.DeepEqual(t, string(v), value)
			return NewPromise().Finish(nil)
		},
	}

	val := ctx.Value()
	ensure.True(t, val == nil)

	ctx.SetValue(value)

	val = ctx.Value()
	ensure.DeepEqual(t, val, value)
}

func TestContext_SetErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		group  Group = "some-group"
		key          = "key"
		value        = "value"
		offset int64 = 123
		wg           = new(sync.WaitGroup)
		st           = NewMockStorage(ctrl)
		pt           = &PartitionTable{
			st: &storageProxy{
				Storage: st,
			},
		}
		failed error
		_      = failed // make linter happy
	)

	ctx := &cbContext{
		table:         pt,
		partProcStats: newPartitionProcStats(),
		wg:            wg,
		graph:         DefineGroup(group, Persist(new(codec.String))),
		msg:           &sarama.ConsumerMessage{Key: []byte(key), Offset: offset},
		failer:        func(err error) { failed = err },
	}

	err := ctx.setValueForKey(key, nil)
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "cannot set nil")

	err = ctx.setValueForKey(key, 123) // cannot encode 123 as string
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "error encoding")

	st.EXPECT().Set(key, []byte(value)).Return(errors.New("some-error"))
	// st.EXPECT().Get(key).Return([]byte(value), nil)

	err = ctx.setValueForKey(key, value)
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "some-error")

	// TODO(jb): check if still valid
	// finish with error
	// ctx.emitter = newEmitterW(wg, fmt.Errorf("some-error"), func(err error) {
	// 	ensure.NotNil(t, err)
	// 	ensure.StringContains(t, err.Error(), "some-error")
	// })
	// err = ctx.setValueForKey(key, value)
	// ensure.Nil(t, err)
}

func TestContext_LoopbackNoLoop(t *testing.T) {
	// ctx has no loop set
	ctx := &cbContext{graph: DefineGroup("group", Persist(c)), msg: &sarama.ConsumerMessage{}}
	func() {
		defer PanicStringContains(t, "loop")
		ctx.Loopback("some-key", "whatever")
	}()
}

func TestContext_Loopback(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		key   = "key"
		value = "value"
		cnt   = 0
	)

	graph := DefineGroup("group", Persist(c), Loop(c, cb))
	ctx := &cbContext{
		graph:         graph,
		msg:           &sarama.ConsumerMessage{},
		partProcStats: newPartitionProcStats(),
		emitter: func(tp string, k string, v []byte) *Promise {
			cnt++
			ensure.DeepEqual(t, tp, graph.LoopStream().Topic())
			ensure.DeepEqual(t, string(k), key)
			ensure.DeepEqual(t, string(v), value)
			return NewPromise()
		},
	}

	ctx.Loopback(key, value)
	ensure.True(t, cnt == 1)
}

func TestContext_Join(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		key           = "key"
		value         = "value"
		table   Table = "table"
		errSome       = errors.New("some-error")
		st            = NewMockStorage(ctrl)
	)

	ctx := &cbContext{
		graph: DefineGroup("group", Persist(c), Loop(c, cb), Join(table, c)),
		msg:   &sarama.ConsumerMessage{Key: []byte(key)},
		pviews: map[string]*PartitionTable{
			string(table): &PartitionTable{
				log: logger.Default(),
				st: &storageProxy{
					Storage: st,
				},
			},
		},
	}

	st.EXPECT().Get(key).Return([]byte(value), nil)
	v := ctx.Join(table)
	ensure.DeepEqual(t, v, value)

	func() {
		defer PanicStringContains(t, errSome.Error())
		st.EXPECT().Get(key).Return(nil, errSome)
		_ = ctx.Join(table)
	}()

	func() {
		defer PanicStringContains(t, "not subs")
		_ = ctx.Join("other-table")
	}()

	ctx.pviews = nil
	func() {
		defer PanicStringContains(t, "not subs")
		_ = ctx.Join(table)
	}()
}

func TestContext_Lookup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		key           = "key"
		value         = "value"
		table   Table = "table"
		errSome       = errors.New("some-error")
		st            = NewMockStorage(ctrl)
	)

	ctx := &cbContext{
		graph: DefineGroup("group", Persist(c), Loop(c, cb)),
		msg:   &sarama.ConsumerMessage{Key: []byte(key)},
		views: map[string]*View{
			string(table): &View{
				opts: &voptions{
					tableCodec: c,
					hasher:     DefaultHasher(),
				},
				partitions: []*PartitionTable{
					&PartitionTable{
						st: &storageProxy{
							Storage: st,
						},
					},
				},
			},
		},
	}

	st.EXPECT().Get(key).Return([]byte(value), nil)
	v := ctx.Lookup(table, key)
	ensure.DeepEqual(t, v, value)

	func() {
		defer PanicStringContains(t, errSome.Error())
		st.EXPECT().Get(key).Return(nil, errSome)
		_ = ctx.Lookup(table, key)
	}()

	func() {
		defer PanicStringContains(t, "not subs")
		_ = ctx.Lookup("other-table", key)
	}()

	ctx.views = nil
	func() {
		defer PanicStringContains(t, "not subs")
		_ = ctx.Lookup(table, key)
	}()
}

func TestContext_Fail(t *testing.T) {
	ctx := new(cbContext)

	defer func() {
		err := recover()
		ensure.NotNil(t, err)
		ensure.True(t, strings.Contains(fmt.Sprintf("%v", err), "blubb"))
	}()

	ctx.Fail(errors.New("blubb"))

	// this must not be executed. ctx.Fail should stop execution
	ensure.True(t, false)
}
