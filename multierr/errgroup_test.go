package multierr

import (
	"context"
	"fmt"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestErrGroup_Go(t *testing.T) {
	bctx := context.Background()

	// no errors
	g, ctx := NewErrGroup(bctx)
	g.Go(func() error { return nil })
	errs := g.Wait()
	err := errs.NilOrError()
	ensure.Nil(t, err)
	ensure.NotNil(t, ctx.Err())
	ensure.StringContains(t, ctx.Err().Error(), "context canceled")

	// with one error
	g, ctx = NewErrGroup(bctx)
	g.Go(func() error { return fmt.Errorf("some error") })
	errs = g.Wait()
	err = errs.NilOrError()
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "some error")
	ensure.NotNil(t, ctx.Err())
	ensure.StringContains(t, ctx.Err().Error(), "context canceled")

	// with one error
	g, ctx = NewErrGroup(bctx)
	g.Go(func() error { return fmt.Errorf("some error") })
	g.Go(func() error { return fmt.Errorf("some error2") })
	errs = g.Wait()
	err = errs.NilOrError()
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "some error")
	ensure.StringContains(t, err.Error(), "some error2")
	ensure.NotNil(t, ctx.Err())
	ensure.StringContains(t, ctx.Err().Error(), "context canceled")
}
