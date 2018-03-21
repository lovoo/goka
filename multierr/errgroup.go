package multierr

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type ErrGroup struct {
	*errgroup.Group
	err Errors
}

func NewErrGroup(ctx context.Context) (*ErrGroup, context.Context) {
	g, ctx := errgroup.WithContext(ctx)
	return &ErrGroup{Group: g}, ctx
}

func (g *ErrGroup) Wait() *Errors {
	_ = g.Group.Wait()
	return &g.err
}

func (g *ErrGroup) Go(f func() error) {
	g.Group.Go(func() error {
		if err := f(); err != nil {
			g.err.Collect(err)
			return err
		}
		return nil
	})
}
