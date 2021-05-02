package goka

import (
	"errors"
	"github.com/lovoo/goka/headers"
	"hash"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/internal/test"
)

var (
	emitterTestClientID = "161"
	emitterTestBrokers  = []string{"0"}
	emitterTestTopic    = Stream("emitter-stream")
	emitterIntCodec     = new(codec.Int64)
)

func createEmitter(t *testing.T, options ...EmitterOption) (*Emitter, *builderMock, *gomock.Controller) {
	ctrl := NewMockController(t)
	bm := newBuilderMock(ctrl)
	emitter, _ := NewEmitter(emitterTestBrokers, emitterTestTopic, emitterIntCodec, append([]EmitterOption{
		WithEmitterClientID(emitterTestClientID),
		WithEmitterTopicManagerBuilder(bm.getTopicManagerBuilder()),
		WithEmitterProducerBuilder(bm.getProducerBuilder()),
		WithEmitterHasher(func() hash.Hash32 { return newConstHasher(0) }),
	}, options...)...)
	return emitter, bm, ctrl
}

func TestEmitter_NewEmitter(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		ctrl := NewMockController(t)
		bm := newBuilderMock(ctrl)
		emitter, err := NewEmitter(emitterTestBrokers, emitterTestTopic, emitterIntCodec, []EmitterOption{
			WithEmitterClientID(emitterTestClientID),
			WithEmitterTopicManagerBuilder(bm.getTopicManagerBuilder()),
			WithEmitterProducerBuilder(bm.getProducerBuilder()),
			WithEmitterHasher(func() hash.Hash32 { return newConstHasher(0) }),
		}...)
		test.AssertNil(t, err)
		test.AssertNotNil(t, emitter)
		test.AssertTrue(t, emitter.codec == emitterIntCodec)
		test.AssertEqual(t, emitter.producer, bm.producer)
		test.AssertTrue(t, emitter.topic == string(emitterTestTopic))
	})
	t.Run("fail", func(t *testing.T) {
		ctrl := NewMockController(t)
		bm := newBuilderMock(ctrl)
		defer ctrl.Finish()
		emitter, err := NewEmitter(emitterTestBrokers, emitterTestTopic, emitterIntCodec, WithEmitterProducerBuilder(bm.getErrorProducerBuilder()))
		test.AssertNotNil(t, err)
		test.AssertNil(t, emitter)
	})
}

func TestEmitter_Emit(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key           = "some-key"
			intVal int64  = 1312
			data   []byte = []byte(strconv.FormatInt(intVal, 10))
		)

		bm.producer.EXPECT().Emit(emitter.topic, key, data).Return(NewPromise().finish(nil, nil))
		promise, err := emitter.Emit(key, intVal)
		test.AssertNil(t, err)
		test.AssertNotNil(t, promise)
	})
	t.Run("fail_producer_emit", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key           = "some-key"
			intVal int64  = 1312
			data   []byte = []byte(strconv.FormatInt(intVal, 10))
			retErr error  = errors.New("some-error")
		)

		bm.producer.EXPECT().Emit(emitter.topic, key, data).Return(NewPromise().finish(nil, retErr))
		promise, err := emitter.Emit(key, intVal)
		test.AssertNil(t, err)
		test.AssertEqual(t, promise.err, retErr)
	})
	t.Run("fail_closed", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key          = "some-key"
			intVal int64 = 1312
		)

		bm.producer.EXPECT().Close().Return(nil)

		emitter.Finish()
		promise, err := emitter.Emit(key, intVal)
		test.AssertNil(t, err)
		test.AssertEqual(t, promise.err, ErrEmitterAlreadyClosed)
	})
	t.Run("fail_encode", func(t *testing.T) {
		emitter, _, _ := createEmitter(t)

		var (
			key    = "some-key"
			intVal = "1312"
		)

		_, err := emitter.Emit(key, intVal)
		test.AssertNotNil(t, err)
	})
	t.Run("default_headers", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		emitter.defaultHeaders = headers.Headers{"header-key": []byte("header-val")}
		defer ctrl.Finish()

		var (
			key          = "some-key"
			intVal int64 = 1312
			data         = []byte(strconv.FormatInt(intVal, 10))
		)

		bm.producer.EXPECT().EmitWithHeaders(emitter.topic, key, data, emitter.defaultHeaders).
			Return(NewPromise().finish(nil, nil))
		promise, err := emitter.Emit(key, intVal)
		test.AssertNil(t, err)
		test.AssertNotNil(t, promise)
	})
}

func TestEmitter_EmitSync(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key           = "some-key"
			intVal int64  = 1312
			data   []byte = []byte(strconv.FormatInt(intVal, 10))
		)

		bm.producer.EXPECT().Emit(emitter.topic, key, data).Return(NewPromise().finish(nil, nil))
		err := emitter.EmitSync(key, intVal)
		test.AssertNil(t, err)
	})
	t.Run("fail_producer_emit", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key           = "some-key"
			intVal int64  = 1312
			data   []byte = []byte(strconv.FormatInt(intVal, 10))
			retErr error  = errors.New("some-error")
		)

		bm.producer.EXPECT().Emit(emitter.topic, key, data).Return(NewPromise().finish(nil, retErr))
		err := emitter.EmitSync(key, intVal)
		test.AssertEqual(t, err, retErr)
	})
	t.Run("fail_closed", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key          = "some-key"
			intVal int64 = 1312
		)

		bm.producer.EXPECT().Close().Return(nil)

		emitter.Finish()
		err := emitter.EmitSync(key, intVal)
		test.AssertEqual(t, err, ErrEmitterAlreadyClosed)
	})
	t.Run("fail_encode", func(t *testing.T) {
		emitter, _, _ := createEmitter(t)

		var (
			key    = "some-key"
			intVal = "1312"
		)

		err := emitter.EmitSync(key, intVal)
		test.AssertNotNil(t, err)
	})
}

func TestEmitter_Finish(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key             = "some-key"
			intVal   int64  = 1312
			data     []byte = []byte(strconv.FormatInt(intVal, 10))
			msgCount        = 200
		)

		bm.producer.EXPECT().Emit(emitter.topic, key, data).Return(NewPromise().finish(nil, nil)).MaxTimes(msgCount)
		bm.producer.EXPECT().Close().Return(nil)

		go func() {
			for i := 0; i < msgCount; i++ {
				_, err := emitter.Emit(key, intVal)
				test.AssertNil(t, err)
				// promise errors are not checked here since they are expected
			}
		}()

		time.Sleep(time.Nanosecond * 45)
		err := emitter.Finish()
		test.AssertNil(t, err)
	})
}
