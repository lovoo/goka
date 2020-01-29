package goka

import (
	"errors"
	"hash"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/lovoo/goka/codec"
)

var (
	clientID string   = "161"
	brokers  []string = []string{"0"}
	topic    Stream   = "emitter-stream"
	intCodec Codec    = new(codec.Int64)
)

func createEmitter(t *testing.T, options ...EmitterOption) (*Emitter, *builderMock, *gomock.Controller) {
	ctrl := NewMockController(t)
	bm := newBuilderMock(ctrl)
	emitter, _ := NewEmitter(brokers, topic, intCodec, append([]EmitterOption{
		WithEmitterClientID(clientID),
		WithEmitterTopicManagerBuilder(bm.getTopicManagerBuilder()),
		WithEmitterProducerBuilder(bm.getProducerBuilder()),
		WithEmitterHasher(func() hash.Hash32 { return NewConstHasher(0) }),
	}, options...)...)
	return emitter, bm, ctrl
}

func TestEmitter_NewEmitter(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		ctrl := NewMockController(t)
		bm := newBuilderMock(ctrl)
		emitter, err := NewEmitter(brokers, topic, intCodec, []EmitterOption{
			WithEmitterClientID(clientID),
			WithEmitterTopicManagerBuilder(bm.getTopicManagerBuilder()),
			WithEmitterProducerBuilder(bm.getProducerBuilder()),
			WithEmitterHasher(func() hash.Hash32 { return NewConstHasher(0) }),
		}...)
		assertNil(t, err)
		assertNotNil(t, emitter)
		assertTrue(t, emitter.codec == intCodec)
		assertEqual(t, emitter.producer, bm.producer)
		assertTrue(t, emitter.topic == string(topic))
	})
	t.Run("fail", func(t *testing.T) {
		ctrl := NewMockController(t)
		bm := newBuilderMock(ctrl)
		defer ctrl.Finish()
		emitter, err := NewEmitter(brokers, topic, intCodec, WithEmitterProducerBuilder(bm.getErrorProducerBuilder()))
		assertNotNil(t, err)
		assertNil(t, emitter)
	})
}

func TestEmitter_Emit(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key    string = "some-key"
			intVal int64  = 1312
			data   []byte = []byte(strconv.FormatInt(intVal, 10))
		)

		bm.producer.EXPECT().Emit(emitter.topic, key, data).Return(NewPromise().Finish(nil))
		promise, err := emitter.Emit(key, intVal)
		assertNil(t, err)
		assertNotNil(t, promise)
	})
	t.Run("fail_producer_emit", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key    string = "some-key"
			intVal int64  = 1312
			data   []byte = []byte(strconv.FormatInt(intVal, 10))
			retErr error  = errors.New("some-error")
		)

		bm.producer.EXPECT().Emit(emitter.topic, key, data).Return(NewPromise().Finish(retErr))
		promise, err := emitter.Emit(key, intVal)
		assertNil(t, err)
		assertEqual(t, promise.err, retErr)
	})
	t.Run("fail_closed", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key    string = "some-key"
			intVal int64  = 1312
		)

		bm.producer.EXPECT().Close().Return(nil)

		emitter.Finish()
		promise, err := emitter.Emit(key, intVal)
		assertNil(t, err)
		assertEqual(t, promise.err, ErrEmitterAlreadyClosed)
	})
	t.Run("fail_encode", func(t *testing.T) {
		emitter, _, _ := createEmitter(t)

		var (
			key    string = "some-key"
			intVal string = "1312"
		)

		_, err := emitter.Emit(key, intVal)
		assertNotNil(t, err)
	})
}

func TestEmitter_EmitSync(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key    string = "some-key"
			intVal int64  = 1312
			data   []byte = []byte(strconv.FormatInt(intVal, 10))
		)

		bm.producer.EXPECT().Emit(emitter.topic, key, data).Return(NewPromise().Finish(nil))
		err := emitter.EmitSync(key, intVal)
		assertNil(t, err)
	})
	t.Run("fail_producer_emit", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key    string = "some-key"
			intVal int64  = 1312
			data   []byte = []byte(strconv.FormatInt(intVal, 10))
			retErr error  = errors.New("some-error")
		)

		bm.producer.EXPECT().Emit(emitter.topic, key, data).Return(NewPromise().Finish(retErr))
		err := emitter.EmitSync(key, intVal)
		assertEqual(t, err, retErr)
	})
	t.Run("fail_closed", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key    string = "some-key"
			intVal int64  = 1312
		)

		bm.producer.EXPECT().Close().Return(nil)

		emitter.Finish()
		err := emitter.EmitSync(key, intVal)
		assertEqual(t, err, ErrEmitterAlreadyClosed)
	})
	t.Run("fail_encode", func(t *testing.T) {
		emitter, _, _ := createEmitter(t)

		var (
			key    string = "some-key"
			intVal string = "1312"
		)

		err := emitter.EmitSync(key, intVal)
		assertNotNil(t, err)
	})
}

func TestEmitter_Finish(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		emitter, bm, ctrl := createEmitter(t)
		defer ctrl.Finish()

		var (
			key      string = "some-key"
			intVal   int64  = 1312
			data     []byte = []byte(strconv.FormatInt(intVal, 10))
			msgCount int    = 200
		)

		bm.producer.EXPECT().Emit(emitter.topic, key, data).Return(NewPromise().Finish(nil)).MaxTimes(msgCount)
		bm.producer.EXPECT().Close().Return(nil)

		go func() {
			for i := 0; i < msgCount; i++ {
				_, err := emitter.Emit(key, intVal)
				assertNil(t, err)
				// promise errors are not checked here since they are expected
			}
		}()

		time.Sleep(time.Nanosecond * 45)
		err := emitter.Finish()
		assertNil(t, err)
	})
}
