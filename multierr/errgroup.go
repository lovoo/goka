package multierr

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// ErrGroup implements a group of parallel running tasks allowing to
// wait for all routines to terminate, as well as error handling.
type ErrGroup struct {
	*errgroup.Group
	err Errors
}

// NewErrGroup creates a new ErrGroup using passed context.
func NewErrGroup(ctx context.Context) (*ErrGroup, context.Context) {
	g, ctx := errgroup.WithContext(ctx)
	return &ErrGroup{Group: g}, ctx
}

// Wait blocks until all goroutines of the error group have terminated and returns
// the accumulated errors.
func (g *ErrGroup) Wait() *Errors {
	_ = g.Group.Wait()
	return &g.err
}

// WaitChan returns a channel that is closed after the error group terminates, possibly
// containing the error
func (g *ErrGroup) WaitChan() <-chan *Errors {
	errs := make(chan *Errors, 1)
	go func() {
		defer close(errs)
		errs <- g.Wait()
	}()
	return errs
}

// Go starts a new goroutine. Termination of all functions can be checked via
// Wait or WaitChan.
// The ErrGroup closes the internal context when the first go-routine returns with an error.
func (g *ErrGroup) Go(f func() error) {
	g.Group.Go(func() error {
		if err := f(); err != nil {
			g.err.Collect(err)
			return err
		}
		return nil
	})
}
