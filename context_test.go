package goka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/lovoo/goka/codec"
)

func newEmitter(err error, done func(err error)) emitter {
	return func(topic string, key string, value []byte, headers Headers) *Promise {
		p := NewPromise()
		if done != nil {
			p.Then(done)
		}
		return p.finish(nil, err)
	}
}

func newEmitterW(wg *sync.WaitGroup, err error, done func(err error)) emitter {
	return func(topic string, key string, value []byte, headers Headers) *Promise {
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
		require.NoError(t, err)
	})

	ctx.start()
	ctx.emit("emit-topic", "key", []byte("value"), nil)
	ctx.finish(nil)

	// we can now for all callbacks -- it should also guarantee a memory fence
	// to the emitted variable (which is not being locked)
	ctx.wg.Wait()

	// check everything is done
	require.Equal(t, emitted, 1)
	require.Equal(t, ack, 1)
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
	require.Equal(t, ack, 0)
	doneFunc(nil)
	require.Equal(t, ack, 1)
}

func TestContext_DeferCommit_witherror(t *testing.T) {
	var (
		ack             = 0
		group     Group = "some-group"
		errToEmit       = errors.New("async error")
	)

	failer := func(err error) {
		require.True(t, strings.Contains(err.Error(), errToEmit.Error()))
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
	require.Equal(t, ack, 0)
	doneFunc(fmt.Errorf("async error"))
	// no commit, no ack, so we'll get the message again.
	require.Equal(t, ack, 0)
	require.Contains(t, ctx.errors.ErrorOrNil().Error(), "async error")
}

func TestContext_Timestamp(t *testing.T) {
	ts := time.Now()

	ctx := &cbContext{
		msg: &message{
			timestamp: ts,
		},
	}

	require.Equal(t, ctx.Timestamp(), ts)
}

func TestContext_EmitError(t *testing.T) {
	var (
		ack             = 0
		emitted         = 0
		errToEmit       = errors.New("some error")
		group     Group = "some-group"
	)

	failer := func(err error) {
		require.True(t, strings.Contains(err.Error(), errToEmit.Error()))
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
		require.Error(t, err)
		require.Equal(t, err, errToEmit)
	})

	ctx.start()
	ctx.emit("emit-topic", "key", []byte("value"), nil)
	ctx.finish(nil)

	// we can now for all callbacks -- it should also guarantee a memory fence
	// to the emitted variable (which is not being locked)
	ctx.wg.Wait()

	// check everything is done
	require.Equal(t, emitted, 1)

	// nothing should be committed here
	require.Equal(t, ack, 0)
}

func TestContext_EmitToStateTopic(t *testing.T) {
	var group Group = "some-group"

	ctx := &cbContext{
		graph:      DefineGroup(group, Persist(c), Loop(c, cb)),
		syncFailer: func(err error) { panic(err) },
	}
	require.PanicsWithError(t,
		"cannot emit to table topic (use SetValue instead)",
		func() { ctx.Emit(Stream(tableName(group)), "key", []byte("value")) },
	)
	require.PanicsWithError(t, "cannot emit to loop topic (use Loopback instead)",
		func() { ctx.Emit(Stream(loopName(group)), "key", []byte("value")) },
	)
	require.PanicsWithError(t, "cannot emit to empty topic",
		func() { ctx.Emit("", "key", []byte("value")) },
	)
}

func TestContext_GetSetStateless(t *testing.T) {
	// ctx stateless since no storage passed
	ctx := &cbContext{
		graph:      DefineGroup("group"),
		msg:        new(message),
		syncFailer: func(err error) { panic(err) },
	}
	require.PanicsWithError(t, "Cannot access state in stateless processor", func() { ctx.Value() })
	require.PanicsWithError(t, "Cannot access state in stateless processor", func() { ctx.SetValue("whatever") })
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
	err := ctx.deleteKey(key, nil)
	require.NoError(t, err)
	ctx.finish(nil)

	ctx.wg.Wait()

	require.Equal(t, ctx.counters, struct {
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

	err := ctx.deleteKey(key, nil)
	require.True(t, strings.Contains(err.Error(), "Cannot access state in stateless processor"))
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

	err := ctx.deleteKey(key, nil)
	require.True(t, strings.Contains(err.Error(), "error deleting key (key) from storage: storage error"))
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
	err := ctx.setValueForKey(key, value, nil)
	require.NoError(t, err)
	ctx.finish(nil)

	ctx.wg.Wait()

	require.Equal(t, ctx.counters, struct {
		emits  int
		dones  int
		stores int
	}{1, 1, 1})
	require.Equal(t, ack, 1)
}

func TestContext_GetSetStateful(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		group   Group = "some-group"
		key           = "key"
		value         = "value"
		headers       = Headers{"key": []byte("headerValue")}
		offset        = int64(123)
		wg            = new(sync.WaitGroup)
		st            = NewMockStorage(ctrl)
		pt            = &PartitionTable{
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
			require.Equal(t, tp, graph.GroupTable().Topic())
			require.Equal(t, string(k), key)
			require.Equal(t, string(v), value)
			require.Equal(t, h, headers)
			return NewPromise().finish(nil, nil)
		},
		ctx: context.Background(),
	}

	val := ctx.Value()
	require.True(t, val == nil)

	ctx.SetValue(value, WithCtxEmitHeaders(headers))

	val = ctx.Value()
	require.Equal(t, val, value)
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

	err := ctx.setValueForKey(key, nil, nil)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "cannot set nil"))

	err = ctx.setValueForKey(key, 123, nil) // cannot encode 123 as string
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "error encoding"))

	st.EXPECT().Set(key, []byte(value)).Return(errors.New("some-error"))

	err = ctx.setValueForKey(key, value, nil)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "some-error"))

	// TODO(jb): check if still valid
	// finish with error
	// ctx.emitter = newEmitterW(wg, fmt.Errorf("some-error"), func(err error) {
	// 	require.Error(t, err)
	//	require.True(t, strings.Contains(err.Error(), "some-error"))
	// })
	// err = ctx.setValueForKey(key, value)
	// require.NoError(t, err)
}

func TestContext_LoopbackNoLoop(t *testing.T) {
	// ctx has no loop set
	ctx := &cbContext{
		graph:      DefineGroup("group", Persist(c)),
		msg:        &message{},
		syncFailer: func(err error) { panic(err) },
	}
	require.PanicsWithError(t, "no loop topic configured", func() {
		ctx.Loopback("some-key", "whatever")
	})
}

func TestContext_Loopback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		key     = "key"
		value   = "value"
		headers = Headers{"key": []byte("headerValue")}
		cnt     = 0
	)

	graph := DefineGroup("group", Persist(c), Loop(c, cb))
	ctx := &cbContext{
		graph:            graph,
		msg:              &message{},
		trackOutputStats: func(ctx context.Context, topic string, size int) {},
		emitter: func(tp string, k string, v []byte, h Headers) *Promise {
			cnt++
			require.Equal(t, tp, graph.LoopStream().Topic())
			require.Equal(t, string(k), key)
			require.Equal(t, string(v), value)
			require.Equal(t, h, headers)
			return NewPromise()
		},
	}

	ctx.Loopback(key, value, WithCtxEmitHeaders(headers))
	require.True(t, cnt == 1)
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
	require.Equal(t, v, value)

	st.EXPECT().Get(key).Return(nil, errSome)
	require.Panics(t, func() { ctx.Join(table) })

	require.Panics(t, func() { ctx.Join("other-table") })

	ctx.pviews = nil
	require.Panics(t, func() { ctx.Join(table) })
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
				state: newViewSignal().SetState(State(ViewStateRunning)),
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
	require.Equal(t, v, value)

	st.EXPECT().Get(key).Return(nil, errSome)
	require.Panics(t, func() { ctx.Lookup(table, key) })
	require.Panics(t, func() { ctx.Lookup("other-table", key) })

	ctx.views = nil
	require.Panics(t, func() { ctx.Lookup(table, key) })
}

func TestContext_Headers(t *testing.T) {
	// context without headers will return empty map
	ctx := &cbContext{
		msg: &message{
			key: "key",
		},
	}
	headers := ctx.Headers()
	require.NotNil(t, headers)
	require.Equal(t, len(headers), 0)

	ctx = &cbContext{
		msg: &message{
			key: "key",
			headers: []*sarama.RecordHeader{
				{
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
		},
	}
	headers = ctx.Headers()
	require.Equal(t, headers["key"], []byte("value"))
}

func TestContext_Fail(t *testing.T) {
	ctx := &cbContext{
		syncFailer: func(err error) {
			panic(fmt.Errorf("%#v", err))
		},
	}

	require.Panics(t, func() { ctx.Fail(errors.New("blubb")) })
}
