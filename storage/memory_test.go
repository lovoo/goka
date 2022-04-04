package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lovoo/goka/multierr"
	"github.com/stretchr/testify/require"
)

func TestMemoryStorage(t *testing.T) {
	t.Run("concurrent", func(t *testing.T) {
		mem := NewMemory()
		defer mem.Close()

		ctx, cancel := context.WithCancel(context.Background())
		errg, ctx := multierr.NewErrGroup(ctx)

		// setter
		errg.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
				}
			}()
			for i := 0; ; i++ {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				mem.Set(fmt.Sprintf("%d", i%5), []byte(fmt.Sprintf("%d", i)))
			}
		})

		// getter
		errg.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
				}
			}()

			for i := 0; ; i++ {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				mem.Get(fmt.Sprintf("%d", i%5))
			}
		})

		// get offset
		errg.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
				}
			}()

			for i := 0; ; i++ {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				mem.GetOffset(0)
			}
		})

		// set offset
		errg.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
				}
			}()

			for i := 0; ; i++ {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				mem.SetOffset(123)
			}
		})

		time.Sleep(1 * time.Second)
		cancel()

		require.NoError(t, errg.Wait().ErrorOrNil())
	})
}
