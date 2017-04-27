package goka

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/mock"

	"github.com/facebookgo/ensure"
	"github.com/golang/mock/gomock"
)

func waitForValue(t *testing.T, value *int, targetValue int, timeout time.Duration) {
	maxTries := 25
	for i := 0; i < maxTries; i++ {
		if *value == targetValue {
			return
		}

		time.Sleep(time.Duration(int64(timeout) / int64(maxTries)))
	}
	t.Fatal("Timeout")
}

func newEmitter(err error, done func(err error)) emitter {
	return func(topic string, key string, value []byte) *kafka.Promise {
		p := kafka.NewPromise()
		if done != nil {
			p.Then(done)
		}
		return p.Finish(err)
	}
}

func newEmitterW(wg *sync.WaitGroup, err error, done func(err error)) emitter {
	return func(topic string, key string, value []byte) *kafka.Promise {
		wg.Add(1)
		p := kafka.NewPromise()
		if done != nil {
			p.Then(done)
		}
		return p.Finish(err)
	}
}

func TestContext_Emit(t *testing.T) {
	ack := 0
	emitted := 0

	ctx := &context{
		commit: func() { ack++ },
		wg:     &sync.WaitGroup{},
	}

	// for each context the goroutine of the partition adds 1 to wg
	ctx.wg.Add(1)

	// after that the message is processed
	ctx.emitter = newEmitter(nil, func(err error) {
		emitted++
		ensure.Nil(t, err)
	})

	err := ctx.emit("emit-topic", "key", []byte("value"))
	ensure.Nil(t, err)

	// and the partition-goroutine marks the context as done
	ctx.markDone()

	// we can now for all callbacks -- it should also guarantee a memory fence
	// to the emitted variable (which is not being locked)
	ctx.wg.Wait()

	// check everything is done
	ensure.DeepEqual(t, emitted, 1)
	ensure.DeepEqual(t, ack, 1)
}

func TestContext_EmitError(t *testing.T) {
	ack := 0
	emitted := 0
	errToEmit := errors.New("some error")

	// test error case
	ctx := &context{
		commit: func() { ack++ },
		wg:     &sync.WaitGroup{},
	}
	// for each context the goroutine of the partition adds 1 to wg
	ctx.wg.Add(1)

	ctx.emitter = newEmitter(errToEmit, func(err error) {
		emitted++
		ensure.NotNil(t, err)
		ensure.DeepEqual(t, err, errToEmit)
	})
	err := ctx.emit("emit-topic", "key", []byte("value"))
	ensure.Nil(t, err)

	// and the partition-goroutine marks the context as done
	ctx.markDone()

	// we can now for all callbacks -- it should also guarantee a memory fence
	// to the emitted variable (which is not being locked)
	ctx.wg.Wait()

	// check everything is done
	ensure.DeepEqual(t, emitted, 1)
	ensure.DeepEqual(t, ack, 1)

}

func TestContext_EmitToStateTopic(t *testing.T) {
	ctx := &context{tableTopic: Subscription{Name: "test"}, loopTopic: Subscription{Name: "loop"}}
	func() {
		defer ensure.PanicDeepEqual(t, errors.New("Cannot emit to table topic, use SetValue() instead."))
		ctx.Emit("test", "key", []byte("value"))
	}()
	func() {
		defer ensure.PanicDeepEqual(t, errors.New("Cannot emit to loop topic, use Loopback() instead."))
		ctx.Emit("loop", "key", []byte("value"))
	}()
	func() {
		defer ensure.PanicDeepEqual(t, errors.New("Cannot emit to empty topic"))
		ctx.Emit("", "key", []byte("value"))
	}()
}

func PanicStringContains(t *testing.T, s string) {
	if r := recover(); r != nil {
		err := r.(error)
		ensure.StringContains(t, err.Error(), s)
	} else {
		// there was no panic
		t.Errorf("panic expected")
		t.FailNow()
	}
}

func TestContext_GetSetStateless(t *testing.T) {
	// ctx stateless since no storage passed
	ctx := &context{tableTopic: Subscription{Name: "test"}, msg: new(message)}
	func() {
		defer PanicStringContains(t, "stateless")
		_ = ctx.Value()
	}()
	func() {
		defer PanicStringContains(t, "stateless")
		ctx.SetValue("whatever")
	}()
	func() {
		defer PanicStringContains(t, "stateless")
		_ = ctx.Has("something")
	}()
}

func TestContext_Set(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storage := mock.NewMockStorage(ctrl)

	offset := int64(123)
	ack := 0
	key := "key"
	value := "value"

	ctx := &context{
		codec:   new(codec.String),
		storage: storage,
		wg:      new(sync.WaitGroup),
		commit:  func() { ack++ },
		msg:     &message{Offset: offset},
	}
	ctx.wg.Add(1)

	gomock.InOrder(
		storage.EXPECT().Set(key, value).Return(nil),
		storage.EXPECT().SetOffset(offset).Return(nil),
	)
	ctx.emitter = newEmitter(nil, nil)
	err := ctx.setValueForKey(key, value)
	ensure.Nil(t, err)
	ctx.markDone()

	ctx.wg.Wait()

	ensure.DeepEqual(t, ctx.counters, struct {
		calls     int32
		callsDone int32
	}{1, 1})
	ensure.DeepEqual(t, ack, 1)
}

func TestContext_GetSetStateful(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		storage = mock.NewMockStorage(ctrl)
		key     = "key"
		value   = "value"
		offset  = int64(123)
		wg      = new(sync.WaitGroup)
	)
	ctx := &context{
		wg:         wg,
		tableTopic: tableTopic,
		msg:        &message{Key: key, Offset: offset},
		storage:    storage,
		codec:      new(codec.String),
		emitter: func(tp string, k string, v []byte) *kafka.Promise {
			wg.Add(1)
			ensure.DeepEqual(t, tp, tableTopic.Name)
			ensure.DeepEqual(t, string(k), key)
			ensure.DeepEqual(t, string(v), value)
			return kafka.NewPromise().Finish(nil)
		},
	}

	storage.EXPECT().Get(key).Return(nil, nil)
	val := ctx.Value()
	ensure.True(t, val == nil)

	storage.EXPECT().Set(key, value).Return(nil)
	storage.EXPECT().SetOffset(offset).Return(nil)
	ctx.SetValue(value)

	storage.EXPECT().Has(key).Return(true, nil)
	ok := ctx.Has(key)
	ensure.True(t, ok)

	storage.EXPECT().Get(key).Return(value, nil)
	val = ctx.Value()
	ensure.DeepEqual(t, val, value)
}

func TestContext_SetErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		storage       = mock.NewMockStorage(ctrl)
		key           = "key"
		value         = "value"
		offset  int64 = 123
		wg            = new(sync.WaitGroup)
		failed  error
	)

	ctx := &context{
		wg:         wg,
		tableTopic: tableTopic,
		msg:        &message{Key: key, Offset: offset},
		storage:    storage,
		failer:     func(err error) { failed = err },
		codec:      new(codec.String),
	}

	err := ctx.setValueForKey(key, nil)
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "Cannot set nil")

	storage.EXPECT().Set(key, 123).Return(nil)
	err = ctx.setValueForKey(key, 123) // cannot encode 123 as string
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "Error encoding")

	storage.EXPECT().Set(key, value).Return(fmt.Errorf("some error"))
	err = ctx.setValueForKey(key, value)
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "Error storing")

	// finish with error
	ctx.emitter = newEmitterW(wg, fmt.Errorf("some error X"), func(err error) {
		ensure.NotNil(t, err)
		ensure.StringContains(t, err.Error(), "error X")
	})
	gomock.InOrder(
		storage.EXPECT().Set(key, value).Return(nil),
	)
	err = ctx.setValueForKey(key, value)
	ensure.Nil(t, err)
	// SetOffset is not called because we finish with error

	// fail to write offset to local storage
	ctx.emitter = newEmitterW(wg, nil, nil)
	ctx.failer = func(err error) {
		ensure.NotNil(t, err)
	}
	gomock.InOrder(
		storage.EXPECT().Set(key, value).Return(nil),
		storage.EXPECT().SetOffset(offset).Return(fmt.Errorf("some error")),
	)
	err = ctx.setValueForKey(key, value)
	ensure.Nil(t, err)

}

func TestContext_LoopbackNoLoop(t *testing.T) {
	// ctx has no loop set
	ctx := &context{tableTopic: tableTopic, msg: new(message)}
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

	ctx := &context{
		tableTopic: tableTopic,
		loopTopic:  loopTopic,
		msg:        new(message),
		emitter: func(tp string, k string, v []byte) *kafka.Promise {
			cnt++
			ensure.DeepEqual(t, tp, loopTopic.Name)
			ensure.DeepEqual(t, string(k), key)
			ensure.DeepEqual(t, string(v), value)
			return kafka.NewPromise()
		},
	}

	ctx.Loopback(key, value)
	ensure.True(t, cnt == 1)
}

func TestContext_Join(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		key   = "key"
		value = "value"
		table = "table"
		st    = mock.NewMockStorage(ctrl)
	)

	ctx := &context{
		tableTopic: tableTopic,
		loopTopic:  loopTopic,
		msg:        &message{Key: key},
		views: map[string]*partition{
			table: &partition{
				st: &storageProxy{
					Storage: st,
				},
			},
		},
	}

	st.EXPECT().Get(key).Return(value, nil)
	v := ctx.Join(table)
	ensure.DeepEqual(t, v, value)

	func() {
		defer ensure.PanicDeepEqual(t, errSome)
		st.EXPECT().Get(key).Return(nil, errSome)
		_ = ctx.Join(table)
	}()

	func() {
		defer PanicStringContains(t, "not subs")
		_ = ctx.Join("other-table")
	}()

	ctx.views = nil
	func() {
		defer PanicStringContains(t, "not subs")
		_ = ctx.Join(table)
	}()
}

func TestContext_Fail(t *testing.T) {
	ctx := new(context)

	defer func() {
		err := recover()
		ensure.NotNil(t, err)
		ensure.True(t, strings.Contains(fmt.Sprintf("%v", err), "blubb"))
	}()

	ctx.Fail(errors.New("blubb"))

	// this must not be executed. ctx.Fail should stop execution
	ensure.True(t, false)
}
