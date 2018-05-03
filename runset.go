package goka

import (
	"context"

	"github.com/lovoo/goka/multierr"
)

// Runnable represents a component that runs goroutines.
type Runnable interface {
	// Run starts the runnable and canceling the context stops it.
	Run(context.Context) error
}

// Runset manages the lifecyle of a set of runnables (processors or
// views). All runnables are started together and as soon as the first stops,
// all other runnables are also stopped.
type Runset struct {
	ctx    context.Context
	cancel func()
	grp    *multierr.ErrGroup
}

// Start one or more runnables and return a Runset object.
func Start(runnables ...Runnable) *Runset {
	ctx, cancel := context.WithCancel(context.Background())
	grp, ctx := multierr.NewErrGroup(ctx)

	s := Runset{ctx, cancel, grp}

	for _, r := range runnables {
		grp.Go(func() error {
			defer cancel()
			return r.Run(ctx)
		})
	}

	return &s
}

// Stop all runnables in the runset.
func (r *Runset) Stop() {
	r.cancel()
}

// Wait for all runnables to stop, returning the aggregated errors if any.
func (r *Runset) Wait() error {
	return r.grp.Wait().NilOrError()
}

// Done returns a channel that is closed once the Runset is stopping. Runnables
// may not yet have returned when the channel is closed.
func (r *Runset) Done() <-chan struct{} {
	return r.ctx.Done()
}
