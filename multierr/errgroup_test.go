package multierr

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestErrGroup_Go(t *testing.T) {
	bctx := context.Background()

	// no errors
	g, ctx := NewErrGroup(bctx)
	g.Go(func() error { return nil })
	errs := g.Wait()
	err := errs.ErrorOrNil()
	require.NoError(t, err)
	require.Error(t, ctx.Err())
	require.Contains(t, ctx.Err().Error(), "context canceled")

	// with one error
	g, ctx = NewErrGroup(bctx)
	g.Go(func() error { return fmt.Errorf("some error") })
	errs = g.Wait()
	err = errs.ErrorOrNil()
	require.Error(t, err)
	require.Contains(t, err.Error(), "some error")
	require.Error(t, ctx.Err())
	require.Contains(t, ctx.Err().Error(), "context canceled")

	// with one error
	g, ctx = NewErrGroup(bctx)
	g.Go(func() error { return fmt.Errorf("some error") })
	g.Go(func() error { return fmt.Errorf("some error2") })
	errs = g.Wait()
	err = errs.ErrorOrNil()
	require.Error(t, err)
	require.Contains(t, err.Error(), "some error")
	require.Contains(t, err.Error(), "some error2")
	require.Error(t, ctx.Err())
	require.Contains(t, ctx.Err().Error(), "context canceled")
}

func TestErrGroup_Empty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errg, errgCtx := NewErrGroup(ctx)

	require.NoError(t, errg.Wait().ErrorOrNil())
	select {
	case <-errgCtx.Done():
	default:
		t.Errorf("context of errgroup was not cancelled after err group terminated")
	}

	select {
	case <-ctx.Done():
		t.Errorf("context timed out")
	default:
	}
}
