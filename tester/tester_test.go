package tester

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/facebookgo/ensure"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

// simple consume function that is used in different tests
func increment(ctx goka.Context, msg interface{}) {
	value := ctx.Value()
	var state int64
	if value != nil {
		state = value.(int64)
	}
	state++
	ctx.SetValue(state)
}

func runProcOrFail(proc *goka.Processor) {
	go func() {
		err := proc.Run(context.Background())
		panic(fmt.Errorf("Processor run errors: %v", err))
	}()
}

func Test_SimpleConsume(t *testing.T) {
	gkt := New(t)

	var receivedMessage string
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			receivedMessage = msg.(string)
		}),
	),
		goka.WithTester(gkt),
	)
	go proc.Run(context.Background())
	for i := 0; i < 101; i++ {
		gkt.ConsumeString("input", "key", fmt.Sprintf("%d", i))
	}

	if receivedMessage != "100" {
		t.Fatalf("Message did not get through...")
	}
}

func Test_InputOutput(t *testing.T) {
	gkt := New(t)

	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.Emit("output", ctx.Key(), msg)
		}),
		goka.Output("output", new(codec.String)),
	),
		goka.WithTester(gkt),
	)
	go proc.Run(context.Background())

	mt := gkt.NewMessageTrackerFromEnd()

	mt.ExpectEmpty("output")

	gkt.ConsumeString("input", "key", "value")

	key, value, ok := mt.NextMessage("output")

	if key != "key" || !reflect.DeepEqual(value, "value") || !ok {
		t.Fatalf("Message was not received in the output queue")
	}
}

func Test_SimplePersist(t *testing.T) {

	gkt := New(t)

	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("input", new(codec.String), increment),
		goka.Persist(new(codec.Int64)),
	),
		goka.WithTester(gkt),
	)
	go func() {
		err := proc.Run(context.Background())
		t.Fatalf("Processor run errors: %v", err)
	}()

	for i := 0; i < 100; i++ {
		gkt.ConsumeString("input", "key", fmt.Sprintf("message - %d", i))
	}

	value := gkt.TableValue("group-table", "key")
	if value.(int64) != 100 {
		t.Fatalf("Message did not get through. was %d", value.(int64))
	}
}

func Test_Persist_InitialState(t *testing.T) {

	gkt := New(t)

	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("input", new(codec.String), increment),
		goka.Persist(new(codec.Int64)),
	),
		goka.WithTester(gkt),
	)

	go func() {
		err := proc.Run(context.Background())
		t.Fatalf("Processor run errors: %v", err)
	}()

	gkt.SetTableValue("group-table", "existing", int64(150))
	gkt.ConsumeString("input", "existing", "")

	if gkt.TableValue("group-table", "existing").(int64) != 151 {
		t.Fatalf("initial state was not loaded. Expected 151, got %v", gkt.TableValue("group-table", "existing"))
	}
}

// Tests multiple processors in a single mock
func Test_MultiProcessor(t *testing.T) {

	gkt := New(t)

	// first processor gets input and emits to increment topic
	input, _ := goka.NewProcessor([]string{}, goka.DefineGroup("numbers",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			time.Sleep(10 * time.Millisecond)
			ctx.Emit("forward1", ctx.Key(), "")
		}),
		goka.Output("forward1", new(codec.String)),
	),
		goka.WithTester(gkt),
	)

	forward, _ := goka.NewProcessor([]string{}, goka.DefineGroup("forward1",
		goka.Input("forward1", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.Emit("forward2", ctx.Key(), "")
		}),
		goka.Output("forward2", new(codec.String)),
	),
		goka.WithTester(gkt),
	)
	forward2, _ := goka.NewProcessor([]string{}, goka.DefineGroup("forward2",
		goka.Input("forward2", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.Emit("forward3", ctx.Key(), "")
		}),
		goka.Output("forward3", new(codec.String)),
	),
		goka.WithTester(gkt),
	)
	forward3, _ := goka.NewProcessor([]string{}, goka.DefineGroup("forward3",
		goka.Input("forward3", new(codec.String), func(ctx goka.Context, msg interface{}) {
			// sleep in between so we know for sure when the waiting implementation is somehow buggy
			time.Sleep(10 * time.Millisecond)
			ctx.Emit("increment", ctx.Key(), "")
		}),
		goka.Output("increment", new(codec.String)),
	),
		goka.WithTester(gkt),
	)
	// second processor increments its state
	incrementer, _ := goka.NewProcessor([]string{}, goka.DefineGroup("accu",
		goka.Input("increment", new(codec.String), increment),
		goka.Persist(new(codec.Int64)),
	),
		goka.WithTester(gkt),
	)

	runProcOrFail(input)
	runProcOrFail(forward)
	runProcOrFail(forward2)
	runProcOrFail(forward3)
	runProcOrFail(incrementer)

	gkt.ConsumeString("input", "test", "")

	if gkt.TableValue("accu-table", "test").(int64) != 1 {
		t.Fatalf("the message did not reached the end")
	}
}

func Test_Loop(t *testing.T) {
	gkt := New(t)

	// first processor gets input and emits to increment topic
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("looptest",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.Loopback("loop-key", "loopvalue")
		}),
		goka.Persist(new(codec.Int64)),
		goka.Loop(new(codec.String), increment),
	),
		goka.WithTester(gkt),
	)
	runProcOrFail(proc)

	gkt.ConsumeString("input", "test", "")
	if gkt.TableValue("looptest-table", "loop-key").(int64) != 1 {
		t.Fatalf("loop failed")
	}
}

func Test_Lookup(t *testing.T) {

	gkt := New(t)

	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("lookup",
		goka.Input("set", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.SetValue(msg)
		}),
		goka.Persist(new(codec.String)),
	),
		goka.WithTester(gkt),
	)

	// add a lookup table
	lookupProc, err := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			val := ctx.Lookup("lookup-table", "somekey").(string)
			if val != "42" {
				ctx.Fail(fmt.Errorf("lookup value was unexpected"))
			}
		}),
		goka.Lookup("lookup-table", new(codec.String)),
	),
		goka.WithTester(gkt),
	)

	if err != nil {
		t.Fatalf("Error creating processor: %v", err)
	}
	runProcOrFail(proc)
	runProcOrFail(lookupProc)
	gkt.Consume("set", "somekey", "42")
	gkt.Consume("input", "sender", "message")
}

func Test_Join(t *testing.T) {

	gkt := New(t)

	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("join",
		goka.Input("set", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.SetValue(msg)
		}),
		goka.Persist(new(codec.String)),
	),
		goka.WithTester(gkt),
	)

	// add a lookup table
	lookupProc, err := goka.NewProcessor([]string{}, goka.DefineGroup("group",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			val := ctx.Lookup("join-table", "somekey").(string)
			if val != "42" {
				ctx.Fail(fmt.Errorf("join value was unexpected"))
			}
		}),
		goka.Lookup("join-table", new(codec.String)),
	),
		goka.WithTester(gkt),
	)

	if err != nil {
		t.Fatalf("Error creating processor: %v", err)
	}
	runProcOrFail(proc)
	runProcOrFail(lookupProc)
	gkt.Consume("set", "somekey", "42")
	gkt.Consume("input", "sender", "message")
}

func Test_MessageTracker_Default(t *testing.T) {

	gkt := New(t)
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("lookup",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.Emit("output", ctx.Key(), msg)
		}),
		goka.Output("output", new(codec.String)),
	),
		goka.WithTester(gkt),
	)
	runProcOrFail(proc)
	mt := gkt.NewMessageTrackerFromEnd()
	gkt.Consume("input", "somekey", "123")

	mt.ExpectEmit("output", "somekey", func(value []byte) {
		if string(value) != "123" {
			t.Fatalf("unexpected output. expected '123', got %s", string(value))
		}
	})

	gkt.Consume("input", "somekey", "124")
	key, value, hasNext := mt.NextMessage("output")
	if key != "somekey" || value.(string) != "124" || !hasNext {
		t.Fatalf("next emitted was something unexpected (key=%s, value=%s, hasNext=%t)", key, value.(string), hasNext)
	}
	_, _, hasNext = mt.NextMessage("output")
	if hasNext {
		t.Fatalf("got another emitted message which shouldn't be there")
	}

	mt.ExpectEmpty("output")

}

func Test_MessageTracker_Extra(t *testing.T) {

	gkt := New(t)
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("lookup",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
			ctx.Emit("output", ctx.Key(), msg)
		}),
		goka.Output("output", new(codec.String)),
	),
		goka.WithTester(gkt),
	)
	runProcOrFail(proc)
	gkt.Consume("input", "somekey", "123")

	tracker := gkt.NewMessageTrackerFromEnd()

	// the new message tracker should start at the end, so the already emitted message
	// shouldn't appear
	tracker.ExpectEmpty("output")

	gkt.Consume("input", "somekey", "124")
	key, value, hasNext := tracker.NextMessage("output")
	if key != "somekey" || value.(string) != "124" || !hasNext {
		t.Fatalf("next emitted was something unexpected (key=%s, value=%s, hasNext=%t)", key, value.(string), hasNext)
	}
}

func Test_Shutdown(t *testing.T) {
	gkt := New(t)
	proc, _ := goka.NewProcessor([]string{}, goka.DefineGroup("lookup",
		goka.Input("input", new(codec.String), func(ctx goka.Context, msg interface{}) {
		}),
	),
		goka.WithTester(gkt),
	)

	ctx, cancel := context.WithCancel(context.Background())
	var (
		wg      sync.WaitGroup
		procErr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		procErr = proc.Run(ctx)
	}()

	gkt.Consume("input", "test", "test")

	time.Sleep(10 * time.Millisecond)
	cancel()

	gkt.Consume("input", "test", "test")

	wg.Wait()
	ensure.Nil(t, procErr, "no error, we cancelled the processor")
}

func Test_LookupWithInitialData(t *testing.T) {
	gkt := New(t)

	proc, _ := goka.NewProcessor([]string{},
		goka.DefineGroup("group",
			goka.Inputs(goka.Streams{"input-a", "input-b"},
				new(codec.String), func(ctx goka.Context, msg interface{}) {
					ctx.Loopback(ctx.Key(), "first-loop")
				}),
			goka.Loop(new(codec.String), func(ctx goka.Context, msg interface{}) {
				if msg.(string) == "first-loop" {
					ctx.Loopback(ctx.Key(), "second-loop")
				} else {
					lookupValue := ctx.Lookup("lookup-table", "somekey")
					if lookupValue != nil {
						ctx.SetValue(fmt.Sprintf("%d", lookupValue))
					}
					ctx.Emit("output", ctx.Key(), msg)
				}
			}),
			goka.Output("output", new(codec.String)),
			goka.Lookup("lookup-table", new(codec.Int64)),
			goka.Persist(new(codec.String)),
		),
		goka.WithTester(gkt),
	)

	go proc.Run(context.Background())
	gkt.Consume("lookup-table", "somekey", int64(123))

	// regression test: this used to block
	gkt.Consume("input-a", "key", "value")
}

func Test_MultiLookup(t *testing.T) {
	gkt := New(t)
	proc, _ := goka.NewProcessor([]string{},
		goka.DefineGroup("group",
			goka.Inputs(goka.Streams{"input"},
				new(codec.String), func(ctx goka.Context, msg interface{}) {
					ctx.SetValue(msg)
				}),
			goka.Persist(new(codec.String)),
		),
		goka.WithTester(gkt),
	)

	var foundValue int

	lookup1, _ := goka.NewProcessor([]string{},
		goka.DefineGroup("lookup1",
			goka.Inputs(goka.Streams{"trigger"},
				new(codec.String), func(ctx goka.Context, msg interface{}) {
					lookupValue := ctx.Lookup("group-table", ctx.Key())
					if lookupValue.(string) != msg.(string) {
						t.Fatalf("expected %s, got %s", msg, lookupValue)
					} else {
						foundValue++
					}
				}),
			goka.Lookup("group-table", new(codec.String)),
		),
		goka.WithTester(gkt),
	)
	lookup2, _ := goka.NewProcessor([]string{},
		goka.DefineGroup("lookup2",
			goka.Inputs(goka.Streams{"trigger"},
				new(codec.String), func(ctx goka.Context, msg interface{}) {
					lookupValue := ctx.Lookup("group-table", ctx.Key())
					if lookupValue.(string) != msg.(string) {
						t.Fatalf("expected %s, got %s", msg, lookupValue)
					} else {
						foundValue++
					}
				}),
			goka.Lookup("group-table", new(codec.String)),
		),
		goka.WithTester(gkt),
	)

	go proc.Run(context.Background())
	go lookup1.Run(context.Background())
	go lookup2.Run(context.Background())

	// set the lookup table value
	gkt.Consume("input", "value-from-input", "43")
	gkt.Consume("trigger", "value-from-input", "43")
	if foundValue != 2 {
		t.Fatalf("did not find value in lookup table")
	}

	foundValue = 0

	gkt.SetTableValue("group-table", "set-in-table", "44")
	gkt.Consume("trigger", "set-in-table", "44")
	if foundValue != 2 {
		t.Fatalf("did not find value in lookup table")
	}
}

func Test_ManyConsume(t *testing.T) {
	var inputs goka.Streams
	for i := 0; i < 100; i++ {
		inputs = append(inputs, goka.Stream(fmt.Sprintf("input-%d", i)))
	}

	received := map[string]bool{}

	gkt := New(t)
	proc, _ := goka.NewProcessor([]string{},
		goka.DefineGroup("group",
			goka.Inputs(inputs, new(codec.String), func(ctx goka.Context, msg interface{}) { received[string(ctx.Topic())] = true }),
		),
		goka.WithTester(gkt),
	)
	go proc.Run(context.Background())

	// we'll just try to get something consumed
	for i := 0; i < 100; i++ {
		gkt.Consume(fmt.Sprintf("input-%d", i), "something", "something")
	}
	if len(received) != 100 {
		t.Fatalf("did not receive all messages")
	}
}
