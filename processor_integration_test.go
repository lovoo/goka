package goka_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/facebookgo/ensure"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/mock"
	"github.com/lovoo/goka/storage"
	"github.com/lovoo/goka/tester"
)

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
