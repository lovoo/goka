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

func TestMemStorageDelete(t *testing.T) {
	storage := NewMemory()

	has, err := storage.Has("key-1")
	require.NoError(t, err)
	require.False(t, has)
	require.Equal(t, 0, len(storage.(*memory).keys))

	err = storage.Set("key-1", []byte("content-1"))
	require.NoError(t, err)

	has, err = storage.Has("key-1")
	require.NoError(t, err)
	require.True(t, has)
	require.Equal(t, 1, len(storage.(*memory).keys))

	err = storage.Delete("key-1")
	require.NoError(t, err)

	has, err = storage.Has("key-1")
	require.NoError(t, err)
	require.False(t, has)
	require.Equal(t, 0, len(storage.(*memory).keys))
}

func TestMemIter(t *testing.T) {
	storage := NewMemory()

	kv := map[string]string{
		"key-1": "val-1",
		"key-2": "val-2",
		"key-3": "val-3",
	}

	storage.Set(offsetKey, []byte("not-returned"))
	for k, v := range kv {
		storage.Set(k, []byte(v))
	}

	// released iterator should be immediately exhausted
	iter, err := storage.Iterator()
	require.NoError(t, err)
	iter.Release()
	require.False(t, iter.Next())
	require.False(t, iter.Seek([]byte("key-2")))

	iter, err = storage.Iterator()
	require.NoError(t, err)
	i := 1
	for iter.Next() {
		raw, err := iter.Value()
		require.NoError(t, err)

		key := string(iter.Key())
		val := string(raw)

		require.Equal(t, fmt.Sprintf("key-%d", i), key)
		require.Equal(t, fmt.Sprintf("val-%d", i), val)
		i++
	}

	key := iter.Key()
	val, err := iter.Value()
	require.NoError(t, err)
	require.True(t, key == nil)
	require.True(t, val == nil)

	k := []byte("key-1")
	iter, err = storage.IteratorWithRange(k, nil)
	require.NoError(t, err)

	require.True(t, iter.Next())
	require.Equal(t, k, iter.Key())

	iter, err = storage.Iterator()
	require.NoError(t, err)
	ok := iter.Seek([]byte("key-2"))
	require.True(t, ok)
	require.Equal(t, []byte("key-2"), iter.Key())
	val, err = iter.Value()
	require.NoError(t, err)
	require.Equal(t, []byte("val-2"), val)

	ok = iter.Seek([]byte("key-4"))
	require.False(t, ok)
	require.Nil(t, iter.Key())
}

func TestGetHas(t *testing.T) {
	storage := NewMemory()

	var (
		err    error
		hasKey bool
	)

	hasKey, err = storage.Has("test-key")
	require.NoError(t, err)
	require.False(t, hasKey)

	value, err := storage.Get("test-key")
	require.True(t, value == nil)
	require.NoError(t, err)

	err = storage.Set("test-key", []byte("test"))
	require.NoError(t, err)

	hasKey, err = storage.Has("test-key")
	require.NoError(t, err)
	require.True(t, hasKey)

	value, err = storage.Get("test-key")
	require.NoError(t, err)
	require.Equal(t, []byte("test"), value)

	hasKey, err = storage.Has("nil-value")
	require.NoError(t, err)
	require.False(t, hasKey)

	err = storage.Set("nil-value", nil)
	require.Error(t, err)
}
