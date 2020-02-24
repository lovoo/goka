package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/internal/test"
	"github.com/lovoo/goka/tester"
)

// Scenario (1)
// One processor with only one input
func Test_1Input(t *testing.T) {
	var (
		tt              = tester.New(t)
		receivedMessage string
	)

	// create a new processor, registering the tester
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			receivedMessage = msg.(string)
		}),
	),
		goka.WithTester(tt),
	)
	// start it
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := proc.Run(context.Background())
		if err != nil {
			t.Fatalf("processor run failed with: %v", err)
		}
	}()

	// consume a message
	tt.Consume("input", "key", "some message")

	// ensure the message was received
	test.AssertEqual(t, receivedMessage, "some message")

	// stop the processor and wait to finish
	proc.Stop()
	<-done
}

// Scenario (2)
// One processor with only one input and one output
func Test_2InputOutput(t *testing.T) {
	var (
		gkt = tester.New(t)
	)

	// create a new processor, registering the tester
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.Emit("output", ctx.Key(), fmt.Sprintf("forwarded: %v", msg))
		}),
		goka.Output("output", new(codec.String)),
	),
		goka.WithTester(gkt),
	)

	// start it
	go proc.Run(context.Background())

	// create a new message tracker so we can check that the message was being emitted.
	// If we created the message tracker after the Consume, there wouldn't be a message.
	mt := gkt.NewQueueTracker("output")

	// send some message
	gkt.Consume("input", "key", "some-message")

	// make sure received the message in the output
	key, value, valid := mt.Next()
	test.AssertTrue(t, valid)
	test.AssertEqual(t, key, "key")
	test.AssertEqual(t, value, "forwarded: some-message")
}

// Scenario (3)
// Instead of an output we will persist the message
func Test_3Persist(t *testing.T) {
	var (
		gkt = tester.New(t)
	)

	// create a new processor, registering the tester
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.SetValue(fmt.Sprintf("state: %v", msg))
		}),
		goka.Persist(new(codec.String)),
	),
		goka.WithTester(gkt),
	)

	// start it
	go proc.Run(context.Background())

	// send some message
	gkt.Consume("input", "key", "some-message")

	// make sure it's correctly persisted in the state
	value := gkt.TableValue("group-table", "key")
	test.AssertEqual(t, value, "state: some-message")
}

// Scenario (4)
// Often setting up a processor requires quite some boiler plate. This example
// shows how to reuse it using subtests
func Test_Subtest(t *testing.T) {
	var (
		gkt = tester.New(t)
	)

	// create a new processor, registering the tester
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.SetValue(fmt.Sprintf("state: %v", msg))
			ctx.Emit("output", "output-key", fmt.Sprintf("forwarded: %v", msg))
		}),
		goka.Persist(new(codec.String)),
		goka.Output("output", new(codec.String)),
	),
		goka.WithTester(gkt),
	)
	go proc.Run(context.Background())

	t.Run("test-1", func(t *testing.T) {
		// clear all values so we can start with an empty state
		gkt.ClearValues()
		// in a subtest we can't know what messages already exists in the topics left
		// by other tests, so let's start a message tracker from here.
		mt := gkt.NewQueueTracker("output")

		// send a message
		gkt.Consume("input", "bob", "hello")

		// check it was emitted
		key, value, ok := mt.Next()
		test.AssertTrue(t, ok)
		test.AssertEqual(t, key, "output-key")
		test.AssertEqual(t, value, "forwarded: hello")

		// we should be at the end
		test.AssertEqual(t, mt.Hwm(), int64(1))
		test.AssertEqual(t, mt.NextOffset(), int64(1))

		// this is equivalent
		_, _, ok = mt.Next()
		test.AssertFalse(t, ok)
	})
	t.Run("test-2", func(t *testing.T) {
		// clear all values so we can start with an empty state
		gkt.ClearValues()

		// send a message
		gkt.Consume("input", "bob", "hello")

		// do some state checks
		value := gkt.TableValue("group-table", "bob")
		test.AssertEqual(t, value, "state: hello")
	})
}

// Scenario (5)
// It's perfectly fine to have loops and use multiple processors in one tester.
func Test_Chain(t *testing.T) {
	var (
		gkt = tester.New(t)
	)

	// First processor:
	// input -> loop -> output1
	proc1, _ := goka.NewProcessor([]string{}, goka.DefineGroup("proc1",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.Loopback(ctx.Key(), fmt.Sprintf("loop: %v", msg))
		}),
		goka.Loop(new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.Emit("proc1-out", ctx.Key(), fmt.Sprintf("proc1-out: %v", msg))
		}),
		goka.Output("proc1-out", new(codec.String)),
	),
		goka.WithTester(gkt),
	)

	// Second processor:
	// input -> persist
	// create a new processor, registering the tester
	proc2, _ := goka.NewProcessor([]string{}, goka.DefineGroup("proc2",
		goka.Input("proc1-out", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.SetValue(fmt.Sprintf("persist: %v", msg))
		}),
		goka.Persist(new(codec.String)),
	),
		goka.WithTester(gkt),
	)

	go proc1.Run(context.Background())
	go proc2.Run(context.Background())

	// Now send a message to input
	// when this method terminates, we know that the message and all subsequent
	// messages that
	gkt.Consume("input", "bob", "hello world")

	// the value should be persisted in the second processor's table
	value := gkt.TableValue("proc2-table", "bob")

	test.AssertEqual(t, value, "persist: proc1-out: loop: hello world")
}
