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

var (
	keys      []string
	numKeys   = 100
	numWrites = 200000
)

func init() {
	for i := 0; i < numKeys; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkStateStorage_unbatched(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "benchmark_statestorage_unbatched")
	require.NoError(b, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	require.NoError(b, err)

	storage, err := New(db)
	require.NoError(b, err)
	require.NoError(b, storage.MarkRecovered())
	b.ResetTimer()
	for i := 0; i < b.N*numWrites; i++ {
		storage.Set(keys[i%len(keys)], []byte(fmt.Sprintf("value-%d", i)))
	}
	storage.Close()
}

func BenchmarkStateStorage_transactioned(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "benchmark_statestorage_transactioned")
	require.NoError(b, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	require.NoError(b, err)

	storage, err := New(db)
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N*numWrites; i++ {
		storage.Set(keys[i%len(keys)], []byte(fmt.Sprintf("value-%d", i)))
	}
	require.NoError(b, storage.MarkRecovered())
	storage.Close()
}

func TestLeveldbStorage(t *testing.T) {
	path, err := os.MkdirTemp("", "goka_storage_leveldb_test")
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

func TestSetGet(t *testing.T) {
	var (
		err    error
		hasKey bool
	)

	tmpdir, err := os.MkdirTemp("", "goka_storage_TestSetGet")
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

func TestIterator(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "goka_storage_TestIterator")
	require.Nil(t, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	require.Nil(t, err)

	st, err := New(db)
	require.Nil(t, err)

	kv := map[string]string{
		"key-1": "val-1",
		"key-2": "val-2",
		"key-3": "val-3",
	}

	for k, v := range kv {
		require.Nil(t, st.Set(k, []byte(v)))
	}

	require.Nil(t, st.SetOffset(777))

	iter, err := st.Iterator()
	require.Nil(t, err)
	defer iter.Release()
	count := 0

	// accessing iterator before Next should only return nils
	val, err := iter.Value()
	require.True(t, val == nil)
	require.Nil(t, err)

	for iter.Next() {
		count++
		key := string(iter.Key())
		expected, ok := kv[key]
		if !ok {
			t.Fatalf("unexpected key from iterator: %s", key)
		}

		val, err := iter.Value()
		require.Nil(t, err)
		require.Equal(t, expected, string(val))
	}
	require.Equal(t, count, len(kv))
}
