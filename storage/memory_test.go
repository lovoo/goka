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
				mem.Set(fmt.Sprintf("%d", i%5), fmt.Appendf(nil, "%d", i))
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

// IteratorWithRange's range is half-open, [start, limit), as the Storage
// interface documents: limit itself is excluded.
func TestMemoryStorage_IteratorWithRangeExcludesLimit(t *testing.T) {
	mem := NewMemory()
	defer mem.Close()
	for i := 0; i < 300; i++ {
		require.NoError(t, mem.Set(fmt.Sprintf("key%05d", i), []byte("v")))
	}

	it, err := mem.IteratorWithRange([]byte("key00100"), []byte("key00200"))
	require.NoError(t, err)
	defer it.Release()

	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	require.Len(t, keys, 100)
	require.Contains(t, keys, "key00100")
	require.NotContains(t, keys, "key00200")
}
