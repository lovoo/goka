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
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/mock"
	"github.com/lovoo/goka/storage"
	"github.com/lovoo/goka/tester"
)

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

func TestProcessor_StatelessContext(t *testing.T) {
	ctrl := mock.NewMockController(t)
	defer ctrl.Finish()
	var (
		tester = tester.New(t)
	)

	callPersist := func(ctx goka.Context, message interface{}) {
		log.Println("processing")
		// call a random setvalue, this is expected to fail
		ctx.SetValue("value")
		t.Errorf("SetValue should panic. We should not have come to that point.")
	}

	proc, err := goka.NewProcessor(nil,
		goka.DefineGroup(
			"stateless-ctx",
			goka.Input("input-topic", new(codec.Bytes), callPersist),
		),
		goka.WithTester(tester),
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
		tester.Consume("input-topic", "key", "msg")
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

		consume := func(ctx goka.Context, msg interface{}) {
			ctx.SetValue(msg)
		}

		proc, err := goka.NewProcessor([]string{"broker"},
			goka.DefineGroup("test",
				goka.Input("topic", new(codec.String), consume),
				goka.Persist(new(codec.String)),
			),
			goka.WithTester(tester),
		)

		ensure.Nil(t, err)
		var (
			procErrors  error
			done        = make(chan struct{})
			ctx, cancel = context.WithCancel(context.Background())
		)
		go func() {
			procErrors = proc.Run(ctx)
			close(done)
		}()

		tester.Consume("topic", "key", "world")
		cancel()
		<-done
		ensure.NotNil(t, procErrors)
	})

	t.Run("Emit", func(t *testing.T) {
		tester := tester.New(t)
		tester.ReplaceEmitHandler(func(topic, key string, value []byte) *kafka.Promise {
			return kafka.NewPromise().Finish(errors.New("producer error"))
		})

		consume := func(ctx goka.Context, msg interface{}) {
			ctx.Emit("blubbb", "key", []byte("some message is emitted"))
		}

		proc, err := goka.NewProcessor([]string{"broker"},
			goka.DefineGroup("test",
				goka.Input("topic", new(codec.String), consume),
				goka.Persist(new(codec.String)),
			),
			goka.WithTester(tester),
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

		tester.Consume("topic", "key", "world")

		cancel()
		<-done
		ensure.True(t, processorErrors != nil)
	})

	t.Run("Value-stateless", func(t *testing.T) {
		tester := tester.New(t)
		tester.ReplaceEmitHandler(func(topic, key string, value []byte) *kafka.Promise {
			return kafka.NewPromise().Finish(errors.New("producer error"))
		})

		consume := func(ctx goka.Context, msg interface{}) {
			func() {
				defer goka.PanicStringContains(t, "stateless")
				_ = ctx.Value()
			}()
		}

		proc, err := goka.NewProcessor([]string{"broker"},
			goka.DefineGroup("test",
				goka.Input("topic", new(codec.String), consume),
			),
			goka.WithTester(tester),
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

		tester.Consume("topic", "key", "world")

		// stopping the processor. It should actually not produce results
		cancel()
		<-done
		ensure.Nil(t, processorErrors)
	})

}

func TestProcessor_consumeFail(t *testing.T) {
	tester := tester.New(t)

	consume := func(ctx goka.Context, msg interface{}) {
		ctx.Fail(errors.New("consume-failed"))
	}

	proc, err := goka.NewProcessor([]string{"broker"},
		goka.DefineGroup("test",
			goka.Input("topic", new(codec.String), consume),
		),
		goka.WithTester(tester),
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

	tester.Consume("topic", "key", "world")

	cancel()
	<-done
	ensure.True(t, strings.Contains(processorErrors.Error(), "consume-failed"))
}

func TestProcessor_consumePanic(t *testing.T) {
	tester := tester.New(t)

	consume := func(ctx goka.Context, msg interface{}) {
		panic("panicking")
	}

	proc, err := goka.NewProcessor([]string{"broker"},
		goka.DefineGroup("test",
			goka.Input("topic", new(codec.String), consume),
		),
		goka.WithTester(tester),
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

	tester.Consume("topic", "key", "world")

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
		cb       goka.ProcessCallback
		handling goka.NilHandling
		codec    goka.Codec
	}{
		{
			"ignore",
			func(ctx goka.Context, msg interface{}) {
				t.Error("should never call consume")
				t.Fail()
			},
			goka.NilIgnore,
			new(codec.String),
		},
		{
			"process",
			func(ctx goka.Context, msg interface{}) {
				if msg != nil {
					t.Errorf("message should be nil:%v", msg)
					t.Fail()
				}
			},
			goka.NilProcess,
			new(codec.String),
		},
		{
			"decode",
			func(ctx goka.Context, msg interface{}) {
				if _, ok := msg.(*nilValue); !ok {
					t.Errorf("message should be a decoded nil value: %T", msg)
					t.Fail()
				}
			},
			goka.NilDecode,
			new(nilCodec),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tester := tester.New(t)
			proc, err := goka.NewProcessor([]string{"broker"},
				goka.DefineGroup("test",
					goka.Input("topic", tc.codec, tc.cb),
				),
				goka.WithTester(tester),
				goka.WithNilHandling(tc.handling),
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

// tests shutting down the processor during recovery
func TestProcessor_failOnRecover(t *testing.T) {
	var (
		recovered       int
		processorErrors error
		_               = processorErrors // make linter happy
		done            = make(chan struct{})
		msgToRecover    = 100
	)

	tester := tester.New(t)

	consume := func(ctx goka.Context, msg interface{}) {
		log.Println("consuming message..", ctx.Key())
	}

	proc, err := goka.NewProcessor([]string{"broker"},
		goka.DefineGroup("test",
			goka.Input("topic", new(codec.String), consume),
			goka.Persist(new(codec.Bytes)),
		),
		goka.WithTester(tester),
		goka.WithUpdateCallback(func(s storage.Storage, partition int32, key string, value []byte) error {
			log.Printf("recovered state: %s: %s", key, string(value))
			recovered++
			time.Sleep(1 * time.Millisecond)
			return nil
		}),
	)
	ensure.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		processorErrors = proc.Run(ctx)
		close(done)
	}()

	for i := 0; i < msgToRecover; i++ {
		tester.Consume("test-table", "key", []byte(fmt.Sprintf("state-%d", i)))
	}

	// let's wait until half of them are roughly recovered
	time.Sleep(50 * time.Millisecond)
	// stop the processor and wait for it
	cancel()
	<-done
	// make sure the recovery was aborted, so we have recovered something but not all

	// we can't test that anymore since there is no recovery-functionality in the tester implemented
	//ensure.True(t, recovered > 0 && recovered < msgToRecover)
}
