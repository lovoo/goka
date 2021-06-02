package integrationtest

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/internal/test"
	"github.com/lovoo/goka/tester"
)

// codec that fails on decode
type failingDecode struct {
	codec goka.Codec
}

func (fc *failingDecode) Decode(_ []byte) (interface{}, error) {
	return nil, fmt.Errorf("Error decoding")
}

func (fc *failingDecode) Encode(msg interface{}) ([]byte, error) {
	return fc.codec.Encode(msg)
}

// Tests that errors inside the callback lead to processor shutdown
func TestErrorCallback(t *testing.T) {

	for _, tcase := range []struct {
		name    string
		consume func(ctx goka.Context, msg interface{})
		codec   goka.Codec
	}{
		{
			name: "panic",
			consume: func(ctx goka.Context, msg interface{}) {
				panic("failing")
			},
			codec: new(codec.Int64),
		},
		{
			name: "decode",
			consume: func(ctx goka.Context, msg interface{}) {
			},
			codec: &failingDecode{
				codec: new(codec.Int64),
			},
		},
		{
			name: "invalid-context-op",
			consume: func(ctx goka.Context, msg interface{}) {
				ctx.SetValue(0)
			},
			codec: new(codec.Int64),
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			gkt := tester.New(t)

			proc, _ := goka.NewProcessor(nil,
				goka.DefineGroup(
					"test",
					goka.Input("input-topic", tcase.codec, tcase.consume),
				),
				goka.WithTester(gkt),
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var (
				err  error
				done = make(chan struct{})
			)
			go func() {
				defer close(done)
				err = proc.Run(ctx)
			}()

			gkt.Consume("input-topic", "a", int64(123))

			select {
			case <-done:
				test.AssertNotNil(t, err)
				log.Printf("error. %v", err)
				test.AssertTrue(t, strings.Contains(err.Error(), "error processing message"))
			case <-time.After(10 * time.Second):
				t.Errorf("processor did not shut down as expected")
			}
		})
	}
}

func TestHeaders(t *testing.T) {
	var (
		gkt              = tester.New(t)
		processorHeaders goka.Headers
		outputHeaders    goka.Headers
	)

	// create a new processor, registering the tester
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			processorHeaders = ctx.Headers()
			ctx.Emit("output", ctx.Key(), fmt.Sprintf("forwarded: %v", msg),
				goka.WithCtxEmitHeaders(
					goka.Headers{
						"Header1": []byte("to output"),
						"Header2": []byte("to output2"),
					}),
				goka.WithCtxEmitHeaders(
					goka.Headers{
						"Header2": []byte("to output3"),
						"Header3": []byte("to output4"),
					}),
			)
		}),
		goka.Output("output", new(codec.String)),
	),
		goka.WithTester(gkt),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	// start it
	go func() {
		defer close(done)
		err := proc.Run(ctx)
		if err != nil {
			t.Errorf("error running processor: %v", err)
		}
	}()

	// create a new message tracker so we can check that the message was being emitted.
	// If we created the message tracker after the Consume, there wouldn't be a message.
	mt := gkt.NewQueueTracker("output")

	// send some message
	gkt.Consume("input", "key", "some-message", tester.WithHeaders(
		goka.Headers{
			"Header1": []byte("value 1"),
			"Header2": []byte("value 2"),
		}),
		tester.WithHeaders(
			goka.Headers{
				"Header2": []byte("value 3"),
				"Header3": []byte("value 4"),
			}),
	)

	// make sure received the message in the output
	outputHeaders, key, value, valid := mt.NextWithHeaders()
	test.AssertTrue(t, valid)
	test.AssertEqual(t, key, "key")
	test.AssertEqual(t, value, "forwarded: some-message")

	// Check headers sent by Emit...
	test.AssertEqual(t, string(outputHeaders["Header1"]), "to output")
	test.AssertEqual(t, string(outputHeaders["Header2"]), "to output3")
	test.AssertEqual(t, string(outputHeaders["Header3"]), "to output4")

	// Check headers sent to processor
	test.AssertEqual(t, string(processorHeaders["Header1"]), "value 1")
	test.AssertEqual(t, string(processorHeaders["Header2"]), "value 3")
	test.AssertEqual(t, string(processorHeaders["Header3"]), "value 4")

	cancel()
	<-done
}

// Tests that errors inside the callback lead to processor shutdown
func TestProcessorVisit(t *testing.T) {
	gkt := tester.New(t)

	proc, _ := goka.NewProcessor(nil,
		goka.DefineGroup(
			"test",
			goka.Input("input-topic", new(codec.Int64), func(ctx goka.Context, msg interface{}) {
				ctx.SetValue(msg)
			}),
			goka.Persist(new(codec.Int64)),
			goka.Output("output", new(codec.Int64)),
			goka.Visitor("reset", func(ctx goka.Context, msg interface{}) {
				ctx.Emit("output", ctx.Key(), msg)
				ctx.SetValue(msg)
			}),
		),
		goka.WithTester(gkt),
	)

	outputTracker := gkt.NewQueueTracker("output")

	var (
		done = make(chan struct{})
		err  error
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		defer close(done)
		err = proc.Run(ctx)
	}()

	gkt.Consume("input-topic", "a", int64(123))
	test.AssertEqual(t, gkt.TableValue("test-table", "a"), int64(123))

	// no output yet
	_, _, ok := outputTracker.Next()
	test.AssertFalse(t, ok)

	proc.VisitAll(ctx, "reset", int64(15))
	time.Sleep(100 * time.Millisecond)
	k, v, ok := outputTracker.Next()
	test.AssertTrue(t, ok)
	test.AssertEqual(t, k, "a")
	test.AssertEqual(t, v, int64(15))
	test.AssertEqual(t, gkt.TableValue("test-table", "a"), int64(15))

	cancel()
	<-done
	test.AssertNil(t, err)
}

/*
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
	ctrl := NewMockController(t)
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
		tester.Consume("input-topic", "key", []byte("msg"))
		<-done
	})
	ensure.Nil(t, err)
}

func TestProcessor_ProducerError(t *testing.T) {

	t.Run("SetValue", func(t *testing.T) {
		tester := tester.New(t)
		tester.ReplaceEmitHandler(func(topic, key string, value []byte) *goka.Promise {
			return goka.NewPromise().Finish(errors.New("producer error"))
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
		tester.ReplaceEmitHandler(func(topic, key string, value []byte) *goka.Promise {
			return goka.NewPromise().Finish(errors.New("producer error"))
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
		tester.ReplaceEmitHandler(func(topic, key string, value []byte) *goka.Promise {
			return goka.NewPromise().Finish(errors.New("producer error"))
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
*/
