package goka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/internal/test"
)

func newEmitter(err error, done func(err error)) emitter {
	return func(topic string, key string, value []byte, hdr Headers) *Promise {
		p := NewPromise()
		if done != nil {
			p.Then(done)
		}
		return p.finish(nil, err)
	}
}

func newEmitterW(wg *sync.WaitGroup, err error, done func(err error)) emitter {
	return func(topic string, key string, value []byte, hdr Headers) *Promise {
		wg.Add(1)
		p := NewPromise()
		if done != nil {
			p.Then(done)
		}
		return p.finish(nil, err)
	}
}

func TestContext_Emit(t *testing.T) {
	var (
		ack           = 0
		emitted       = 0
		group   Group = "some-group"
	)

	ctx := &cbContext{
		graph:            DefineGroup(group),
		commit:           func() { ack++ },
		wg:               &sync.WaitGroup{},
		trackOutputStats: func(ctx context.Context, topic string, size int) {},
	}

	// after that the message is processed
	ctx.emitter = newEmitter(nil, func(err error) {
		emitted++
		test.AssertNil(t, err)
	})

	ctx.start()
	ctx.emit("emit-topic", "key", []byte("value"), Headers{})
	ctx.finish(nil)

	// we can now for all callbacks -- it should also guarantee a memory fence
	// to the emitted variable (which is not being locked)
	ctx.wg.Wait()

	// check everything is done
	test.AssertEqual(t, emitted, 1)
	test.AssertEqual(t, ack, 1)
}

func TestContext_DeferCommit(t *testing.T) {
	var (
		ack         = 0
		group Group = "some-group"
	)

	ctx := &cbContext{
		graph:            DefineGroup(group),
		commit:           func() { ack++ },
		wg:               &sync.WaitGroup{},
		trackOutputStats: func(ctx context.Context, topic string, size int) {},
	}

	ctx.start()
	doneFunc := ctx.DeferCommit()
	ctx.finish(nil)

	// ack is not done
	test.AssertEqual(t, ack, 0)
	doneFunc(nil)
	test.AssertEqual(t, ack, 1)
}

func TestContext_DeferCommit_witherror(t *testing.T) {
	var (
		ack             = 0
		group     Group = "some-group"
		errToEmit       = errors.New("async error")
	)

	failer := func(err error) {
		test.AssertTrue(t, strings.Contains(err.Error(), errToEmit.Error()))
	}

	ctx := &cbContext{
		graph:            DefineGroup(group),
		commit:           func() { ack++ },
		wg:               &sync.WaitGroup{},
		trackOutputStats: func(ctx context.Context, topic string, size int) {},
		asyncFailer:      failer,
	}

	ctx.start()
	doneFunc := ctx.DeferCommit()
	ctx.finish(nil)

	// ack is not done
	test.AssertEqual(t, ack, 0)
	doneFunc(fmt.Errorf("async error"))
	// no commit, no ack, so we'll get the message again.
	test.AssertEqual(t, ack, 0)
	test.AssertEqual(t, ctx.errors.NilOrError().Error(), "async error")
}

func TestContext_Timestamp(t *testing.T) {
	ts := time.Now()

	ctx := &cbContext{
		msg: &message{
			timestamp: ts,
		},
	}

	test.AssertEqual(t, ctx.Timestamp(), ts)
}

func TestContext_EmitError(t *testing.T) {
	var (
		ack             = 0
		emitted         = 0
		errToEmit       = errors.New("some error")
		group     Group = "some-group"
	)

	failer := func(err error) {
		test.AssertTrue(t, strings.Contains(err.Error(), errToEmit.Error()))
	}

	// test error case
	ctx := &cbContext{
		graph:            DefineGroup(group, Persist(new(codec.String))),
		wg:               &sync.WaitGroup{},
		trackOutputStats: func(ctx context.Context, topic string, size int) {},
		syncFailer:       failer,
		asyncFailer:      failer,
	}
	ctx.emitter = newEmitter(errToEmit, func(err error) {
		emitted++
		test.AssertNotNil(t, err)
		test.AssertEqual(t, err, errToEmit)
	})

	ctx.start()
	ctx.emit("emit-topic", "key", []byte("value"), Headers{})
	ctx.finish(nil)

	// we can now for all callbacks -- it should also guarantee a memory fence
	// to the emitted variable (which is not being locked)
	ctx.wg.Wait()

	// check everything is done
	test.AssertEqual(t, emitted, 1)

	// nothing should be committed here
	test.AssertEqual(t, ack, 0)

}

func TestContext_EmitToStateTopic(t *testing.T) {
	var (
		group Group = "some-group"
	)

	ctx := &cbContext{
		graph:      DefineGroup(group, Persist(c), Loop(c, cb)),
		syncFailer: func(err error) { panic(err) },
	}
	func() {
		defer test.PanicAssertEqual(t, errors.New("cannot emit to table topic (use SetValue instead)"))
		ctx.Emit(Stream(tableName(group)), "key", []byte("value"))
	}()
	func() {
		defer test.PanicAssertEqual(t, errors.New("cannot emit to loop topic (use Loopback instead)"))
		ctx.Emit(Stream(loopName(group)), "key", []byte("value"))
	}()
	func() {
		defer test.PanicAssertEqual(t, errors.New("cannot emit to empty topic"))
		ctx.Emit("", "key", []byte("value"))
	}()
}

func TestContext_GetSetStateless(t *testing.T) {
	// ctx stateless since no storage passed
	ctx := &cbContext{
		graph:      DefineGroup("group"),
		msg:        new(message),
		syncFailer: func(err error) { panic(err) },
	}
	func() {
		defer test.PanicAssertStringContains(t, "stateless")
		_ = ctx.Value()
	}()
	func() {
		defer test.PanicAssertStringContains(t, "stateless")
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
			stats: newTableStats(),
		}
	)

	st.EXPECT().Delete(key).Return(nil)

	ctx := &cbContext{
		graph:  DefineGroup(group, Persist(new(codec.String))),
		wg:     new(sync.WaitGroup),
		commit: func() { ack++ },
		msg:    &message{offset: offset},
		table:  pt,
	}

	ctx.emitter = newEmitter(nil, nil)

	ctx.start()
	err := ctx.deleteKey(key, Headers{})
	test.AssertNil(t, err)
	ctx.finish(nil)

	ctx.wg.Wait()

	test.AssertEqual(t, ctx.counters, struct {
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
		msg:   &message{offset: offset},
	}
	ctx.emitter = newEmitter(nil, nil)

	err := ctx.deleteKey(key, Headers{})
	test.AssertTrue(t, strings.Contains(err.Error(), "Cannot access state in stateless processor"))
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
			stats: newTableStats(),
		}
		retErr = errors.New("storage error")
	)

	st.EXPECT().Delete(key).Return(retErr)

	ctx := &cbContext{
		graph: DefineGroup(group, Persist(new(codec.String))),
		wg:    new(sync.WaitGroup),
		msg:   &message{offset: offset},
		table: pt,
	}

	ctx.emitter = newEmitter(nil, nil)

	err := ctx.deleteKey(key, Headers{})
	test.AssertTrue(t, strings.Contains(err.Error(), "error deleting key (key) from storage: storage error"))
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
			stats:       newTableStats(),
			updateStats: make(chan func(), 10),
		}
	)
	st.EXPECT().Set(key, []byte(value)).Return(nil)

	ctx := &cbContext{
		graph:            DefineGroup(group, Persist(new(codec.String))),
		wg:               new(sync.WaitGroup),
		commit:           func() { ack++ },
		trackOutputStats: func(ctx context.Context, topic string, size int) {},
		msg:              &message{key: key, offset: offset},
		table:            pt,
		ctx:              context.Background(),
	}

	ctx.emitter = newEmitter(nil, nil)

	ctx.start()
	err := ctx.setValueForKey(key, value, Headers{})
	test.AssertNil(t, err)
	ctx.finish(nil)

	ctx.wg.Wait()

	test.AssertEqual(t, ctx.counters, struct {
		emits  int
		dones  int
		stores int
	}{1, 1, 1})
	test.AssertEqual(t, ack, 1)
}

func TestContext_GetSetStateful(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		group  Group = "some-group"
		key          = "key"
		value        = "value"
		hdr          = Headers{"key": []byte("headerValue")}
		offset       = int64(123)
		wg           = new(sync.WaitGroup)
		st           = NewMockStorage(ctrl)
		pt           = &PartitionTable{
			st: &storageProxy{
				Storage: st,
			},
			state:       newPartitionTableState().SetState(State(PartitionRunning)),
			stats:       newTableStats(),
			updateStats: make(chan func(), 10),
		}
	)

	st.EXPECT().Get(key).Return(nil, nil)
	st.EXPECT().Set(key, []byte(value)).Return(nil)
	st.EXPECT().Get(key).Return([]byte(value), nil)

	graph := DefineGroup(group, Persist(new(codec.String)))
	ctx := &cbContext{
		table:            pt,
		wg:               wg,
		graph:            graph,
		trackOutputStats: func(ctx context.Context, topic string, size int) {},
		msg:              &message{key: key, offset: offset},
		emitter: func(tp string, k string, v []byte, h Headers) *Promise {
			wg.Add(1)
			test.AssertEqual(t, tp, graph.GroupTable().Topic())
			test.AssertEqual(t, string(k), key)
			test.AssertEqual(t, string(v), value)
			test.AssertEqual(t, h, hdr)
			return NewPromise().finish(nil, nil)
		},
		ctx: context.Background(),
	}

	val := ctx.Value()
	test.AssertTrue(t, val == nil)

	ctx.SetValue(value, WithCtxEmitHeaders(hdr))

	val = ctx.Value()
	test.AssertEqual(t, val, value)
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
			stats: newTableStats(),
		}
		failed error
		_      = failed // make linter happy
	)

	failer := func(err error) { failed = err }

	ctx := &cbContext{
		table:            pt,
		trackOutputStats: func(ctx context.Context, topic string, size int) {},
		wg:               wg,
		graph:            DefineGroup(group, Persist(new(codec.String))),
		msg:              &message{key: key, offset: offset},
		syncFailer:       failer,
		asyncFailer:      failer,
	}

	err := ctx.setValueForKey(key, nil, Headers{})
	test.AssertNotNil(t, err)
	test.AssertTrue(t, strings.Contains(err.Error(), "cannot set nil"))

	err = ctx.setValueForKey(key, 123, Headers{}) // cannot encode 123 as string
	test.AssertNotNil(t, err)
	test.AssertTrue(t, strings.Contains(err.Error(), "error encoding"))

	st.EXPECT().Set(key, []byte(value)).Return(errors.New("some-error"))

	err = ctx.setValueForKey(key, value, Headers{})
	test.AssertNotNil(t, err)
	test.AssertTrue(t, strings.Contains(err.Error(), "some-error"))

	// TODO(jb): check if still valid
	// finish with error
	// ctx.emitter = newEmitterW(wg, fmt.Errorf("some-error"), func(err error) {
	// 	test.AssertNotNil(t, err)
	//	test.AssertTrue(t, strings.Contains(err.Error(), "some-error"))
	// })
	// err = ctx.setValueForKey(key, value)
	// test.AssertNil(t, err)
}

func TestContext_LoopbackNoLoop(t *testing.T) {
	// ctx has no loop set
	ctx := &cbContext{
		graph:      DefineGroup("group", Persist(c)),
		msg:        &message{},
		syncFailer: func(err error) { panic(err) },
	}
	func() {
		defer test.PanicAssertStringContains(t, "loop")
		ctx.Loopback("some-key", "whatever")
	}()
}

func TestContext_Loopback(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		key   = "key"
		value = "value"
		hdr   = Headers{"key": []byte("headerValue")}
		cnt   = 0
	)

	graph := DefineGroup("group", Persist(c), Loop(c, cb))
	ctx := &cbContext{
		graph:            graph,
		msg:              &message{},
		trackOutputStats: func(ctx context.Context, topic string, size int) {},
		emitter: func(tp string, k string, v []byte, h Headers) *Promise {
			cnt++
			test.AssertEqual(t, tp, graph.LoopStream().Topic())
			test.AssertEqual(t, string(k), key)
			test.AssertEqual(t, string(v), value)
			test.AssertEqual(t, h, hdr)
			return NewPromise()
		},
	}

	ctx.Loopback(key, value, WithCtxEmitHeaders(hdr))
	test.AssertTrue(t, cnt == 1)
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
		msg:   &message{key: key},
		pviews: map[string]*PartitionTable{
			string(table): {
				log: defaultLogger,
				st: &storageProxy{
					Storage: st,
				},
				stats: newTableStats(),
			},
		},
		syncFailer: func(err error) { panic(err) },
	}

	st.EXPECT().Get(key).Return([]byte(value), nil)
	v := ctx.Join(table)
	test.AssertEqual(t, v, value)

	func() {
		defer test.PanicAssertStringContains(t, errSome.Error())
		st.EXPECT().Get(key).Return(nil, errSome)
		_ = ctx.Join(table)
	}()

	func() {
		defer test.PanicAssertStringContains(t, "not subs")
		_ = ctx.Join("other-table")
	}()

	ctx.pviews = nil
	func() {
		defer test.PanicAssertStringContains(t, "not subs")
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
		msg:   &message{key: key},
		views: map[string]*View{
			string(table): {
				opts: &voptions{
					tableCodec: c,
					hasher:     DefaultHasher(),
				},
				partitions: []*PartitionTable{
					{
						st: &storageProxy{
							Storage: st,
						},
						state: newPartitionTableState().SetState(State(PartitionRunning)),
						stats: newTableStats(),
					},
				},
			},
		},
		syncFailer: func(err error) { panic(err) },
	}

	st.EXPECT().Get(key).Return([]byte(value), nil)
	v := ctx.Lookup(table, key)
	test.AssertEqual(t, v, value)

	func() {
		defer test.PanicAssertStringContains(t, errSome.Error())
		st.EXPECT().Get(key).Return(nil, errSome)
		_ = ctx.Lookup(table, key)
	}()

	func() {
		defer test.PanicAssertStringContains(t, "not subs")
		_ = ctx.Lookup("other-table", key)
	}()

	ctx.views = nil
	func() {
		defer test.PanicAssertStringContains(t, "not subs")
		_ = ctx.Lookup(table, key)
	}()
}

func TestContext_Headers(t *testing.T) {

	// context without headers will return empty map
	ctx := &cbContext{
		msg: &message{key: "key"},
	}
	hdr := ctx.Headers()
	test.AssertEqual(t, len(hdr), 0)

	ctx = &cbContext{
		msg: &message{key: "key", headers: Headers{"key": []byte("value")}},
	}
	hdr = ctx.Headers()
	test.AssertEqual(t, hdr["key"], []byte("value"))
}

func TestContext_Fail(t *testing.T) {
	ctx := &cbContext{
		syncFailer: func(err error) {
			panic(fmt.Errorf("%#v", err))
		},
	}

	defer func() {
		err := recover()
		test.AssertNotNil(t, err)
		test.AssertTrue(t, strings.Contains(fmt.Sprintf("%v", err), "blubb"))
	}()

	ctx.Fail(errors.New("blubb"))

	// this must not be executed. ctx.Fail should stop execution
	test.AssertTrue(t, false)
}
