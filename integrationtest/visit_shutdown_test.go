package integrationtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/tester"
	"github.com/stretchr/testify/require"
)

// TestVisitRacesShutdown races Processor.VisitAllWithStats against the
// cancellation of the context passed to Processor.Run.
//
// Without the fix this panics -- usually within the first handful of
// iterations -- with either "send on closed channel" (VisitValues enqueueing
// onto a visitInput that Stop has already closed) or "sync: WaitGroup is
// reused before previous Wait has returned" (VisitValues joining runnerGroup
// that Stop is already waiting on). It panics with and without -race; under
// -race the detector additionally reports the underlying races first.
func TestVisitRacesShutdown(t *testing.T) {
	const (
		iterations = 200
		keys       = 200
	)

	for i := 0; i < iterations; i++ {
		gkt := tester.New(t)

		proc, err := goka.NewProcessor(nil,
			goka.DefineGroup("visit-race",
				goka.Input(goka.Stream("input"), new(codec.String), func(ctx goka.Context, msg any) {
					ctx.SetValue(msg)
				}),
				goka.Visitor("visitor", func(ctx goka.Context, meta any) {
					ctx.SetValue(ctx.Value())
				}),
				goka.Persist(new(codec.String)),
			),
			goka.WithTester(gkt),
		)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		procDone := make(chan struct{})
		go func() {
			defer close(procDone)
			_ = proc.Run(ctx)
		}()
		require.NoError(t, proc.WaitForReadyContext(ctx))

		// Give the visitor something to iterate over.
		for k := 0; k < keys; k++ {
			gkt.Consume("input", fmt.Sprintf("key-%d", k), "value")
		}

		// Start a visit and shut the processor down underneath it. Both an
		// error and a clean nil are acceptable outcomes; a panic is not.
		visitDone := make(chan struct{})
		go func() {
			defer close(visitDone)
			_, _ = proc.VisitAllWithStats(ctx, "visitor", nil)
		}()
		cancel()

		<-visitDone
		<-procDone
	}
}
