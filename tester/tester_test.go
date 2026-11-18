package tester

import (
	"context"
	"testing"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/stretchr/testify/require"
)

type failingT struct {
	failed bool
}

func (ft *failingT) Errorf(format string, args ...any) {
	ft.failed = true
}

func (ft *failingT) Fatalf(format string, args ...any) {
	ft.failed = true
}

func (ft *failingT) Fatal(a ...any) {
	ft.failed = true
}

func TestTesterInvalidGroup(t *testing.T) {
	var ft failingT
	gkt := New(&ft)

	goka.NewProcessor(nil,
		goka.DefineGroup("", goka.Input("input", new(codec.Int64), func(ctx goka.Context, msg any) {
			// nothing called
		})),
		goka.WithTester(gkt))

	// make sure that an invalid group graph fails the test
	require.True(t, ft.failed)
}

func TestTesterConsume(t *testing.T) {
	var ft failingT
	gkt := New(&ft)

	var (
		value     string
		key       string
		timestamp time.Time
	)
	proc, err := goka.NewProcessor(nil,
		goka.DefineGroup("test", goka.Input("input", new(codec.String), func(ctx goka.Context, msg any) {
			value = msg.(string)
			key = ctx.Key()
			timestamp = ctx.Timestamp()
		})),
		goka.WithTester(gkt))

	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		require.NoError(t, proc.Run(ctx))
	}()

	// consume with default time (now)
	gkt.Consume("input", "some-key", "value")
	require.EqualValues(t, "some-key", key)
	require.EqualValues(t, "value", value)
	// the test should not take longer than 1 second
	require.True(t, timestamp.IsZero())

	// consume with timestamp option
	gkt.Consume("input", "anotherkey", "value", WithTime(time.Unix(123, 0)))
	require.EqualValues(t, "anotherkey", key)
	require.EqualValues(t, "value", value)
	// the test should not take longer than 1 second
	require.EqualValues(t, 123, timestamp.Unix())

	// stop the processor and wait for it to finish
	cancel()
	<-done
}

// Stopping a processor while the tester is still waiting for a message to be
// processed must not race with the tester closing the claim (#481).
func TestTesterStopWhileConsuming(t *testing.T) {
	for range 10 {
		gkt := New(t)

		proc, err := goka.NewProcessor(nil,
			goka.DefineGroup("test", goka.Input("input", new(codec.String), func(ctx goka.Context, msg any) {
				time.Sleep(50 * time.Millisecond)
			})),
			goka.WithTester(gkt))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- proc.Run(ctx)
		}()
		require.NoError(t, proc.WaitForReadyContext(ctx))

		consumed := make(chan struct{})
		go func() {
			defer close(consumed)
			gkt.Consume("input", "key", "value")
		}()

		// stop the processor while the message is being processed. This sleeps
		// instead of waiting on the callback, because waiting on it would order
		// the tester's send before the cancel and hide the race.
		time.Sleep(10 * time.Millisecond)
		cancel()
		require.NoError(t, <-done)
		<-consumed
	}
}
