package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"
)

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

func TestSetGet(t *testing.T) {
	var (
		err    error
		hasKey bool
	)

	tmpdir, err := ioutil.TempDir("", "goka_storage_TestSetGet")
	require.NoError(t, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	require.NoError(t, err)

	storage, err := New(db)
	require.NoError(t, err)

	hasKey, err = storage.Has("example1")
	require.NoError(t, err)
	require.False(t, hasKey)

	value, err := storage.Get("example1")
	require.True(t, value == nil)
	require.NoError(t, err)

	err = storage.Set("example1", []byte("example-message"))
	require.NoError(t, err)

	hasKey, err = storage.Has("example1")
	require.NoError(t, err)
	require.True(t, hasKey)

	value, err = storage.Get("example1")
	require.NoError(t, err)

	require.NoError(t, storage.Delete("example1"))
	hasKey, err = storage.Has("example1")
	require.NoError(t, err)
	require.False(t, hasKey)

	// test iteration
	require.NoError(t, storage.Set("key1", []byte("value1")))
	require.NoError(t, storage.Set("key2", []byte("value2")))
	iter, err := storage.Iterator()
	require.NoError(t, err)
	defer iter.Release()
	messages := make(map[string]string)

	for iter.Next() {
		key := string(iter.Key())
		val, err := iter.Value()
		require.NoError(t, err)
		messages[key] = string(val)
	}
	require.True(t, len(messages) == 2)
	require.Equal(t, "value1", messages["key1"])
	require.Equal(t, "value2", messages["key2"])

	recoveredValue := string(value)
	require.Equal(t, "example-message", recoveredValue)
}

func TestLeveldbStorage(t *testing.T) {
	path, err := ioutil.TempDir("", "goka_storage_leveldb_test")
	require.NoError(t, err)

	newStorage := func(delete bool, t *testing.T) Storage {
		if delete {
			os.RemoveAll(path)
		}

		db, err := leveldb.OpenFile(path, nil)
		require.NoError(t, err)

		st, err := New(db)
		require.NoError(t, err)
		return st
	}

	t.Run("getset", func(t *testing.T) {
		st := newStorage(true, t)

		st.Open()
		time.Sleep(1 * time.Second)
		offset, err := st.GetOffset(0)
		require.Equal(t, int64(0), offset)
		require.NoError(t, err)

		require.NoError(t, st.SetOffset(100))
		offset, err = st.GetOffset(0)
		require.Equal(t, int64(100), offset)
		require.NoError(t, err)
	})

	t.Run("set-reopen", func(t *testing.T) {
		st := newStorage(true, t)

		st.Open()
		time.Sleep(1 * time.Second)
		offset, err := st.GetOffset(0)
		require.Equal(t, int64(0), offset)
		require.NoError(t, err)

		require.NoError(t, st.SetOffset(100))
		require.NoError(t, st.Close())

		st = newStorage(false, t)
		offset, err = st.GetOffset(0)
		require.Equal(t, int64(100), offset)
		require.NoError(t, err)
	})

	t.Run("mark-recovered-reopen", func(t *testing.T) {
		st := newStorage(true, t)

		st.Open()
		time.Sleep(1 * time.Second)
		offset, err := st.GetOffset(0)
		require.Equal(t, int64(0), offset)
		require.NoError(t, err)

		require.NoError(t, st.SetOffset(100))

		st.MarkRecovered()

		require.NoError(t, st.SetOffset(101))

		require.NoError(t, st.Close())
		st = newStorage(false, t)
		offset, err = st.GetOffset(0)
		require.Equal(t, int64(101), offset)
		require.NoError(t, err)
	})
}
