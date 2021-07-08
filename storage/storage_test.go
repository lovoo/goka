package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/lovoo/goka/internal/test"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestMemStorageDelete(t *testing.T) {
	storage := NewMemory()

	has, err := storage.Has("key-1")
	test.AssertNil(t, err)
	test.AssertFalse(t, has)
	test.AssertEqual(t, len(storage.(*memory).keys), 0)

	err = storage.Set("key-1", []byte("content-1"))
	test.AssertNil(t, err)

	has, err = storage.Has("key-1")
	test.AssertNil(t, err)
	test.AssertTrue(t, has)
	test.AssertEqual(t, len(storage.(*memory).keys), 1)

	err = storage.Delete("key-1")
	test.AssertNil(t, err)

	has, err = storage.Has("key-1")
	test.AssertNil(t, err)
	test.AssertFalse(t, has)
	test.AssertEqual(t, len(storage.(*memory).keys), 0)
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
	test.AssertNil(t, err)
	iter.Release()
	test.AssertFalse(t, iter.Next())
	test.AssertFalse(t, iter.Seek([]byte("key-2")))

	iter, err = storage.Iterator()
	test.AssertNil(t, err)
	i := 1
	for iter.Next() {
		raw, err := iter.Value()
		test.AssertNil(t, err)

		key := string(iter.Key())
		val := string(raw)

		test.AssertEqual(t, key, fmt.Sprintf("key-%d", i))
		test.AssertEqual(t, val, fmt.Sprintf("val-%d", i))
		i++
	}

	key := iter.Key()
	val, err := iter.Value()
	test.AssertNil(t, err)
	test.AssertTrue(t, key == nil)
	test.AssertTrue(t, val == nil)

	k := []byte("key-1")
	iter, err = storage.IteratorWithRange(k, nil)
	test.AssertNil(t, err)

	test.AssertTrue(t, iter.Next())
	test.AssertEqual(t, iter.Key(), k)

	iter, err = storage.Iterator()
	test.AssertNil(t, err)
	ok := iter.Seek([]byte("key-2"))
	test.AssertTrue(t, ok)
	test.AssertEqual(t, iter.Key(), []byte("key-2"))
	val, err = iter.Value()
	test.AssertNil(t, err)
	test.AssertEqual(t, val, []byte("val-2"))

	ok = iter.Seek([]byte("key-4"))
	test.AssertFalse(t, ok)
	test.AssertNil(t, iter.Key())
}

func TestGetHas(t *testing.T) {
	storage := NewMemory()

	var (
		err    error
		hasKey bool
	)

	hasKey, err = storage.Has("test-key")
	test.AssertNil(t, err)
	test.AssertFalse(t, hasKey)

	value, err := storage.Get("test-key")
	test.AssertTrue(t, value == nil)
	test.AssertNil(t, err)

	err = storage.Set("test-key", []byte("test"))
	test.AssertNil(t, err)

	hasKey, err = storage.Has("test-key")
	test.AssertNil(t, err)
	test.AssertTrue(t, hasKey)

	value, err = storage.Get("test-key")
	test.AssertNil(t, err)
	test.AssertEqual(t, value, []byte("test"))

	hasKey, err = storage.Has("nil-value")
	test.AssertNil(t, err)
	test.AssertFalse(t, hasKey)

	err = storage.Set("nil-value", nil)
	test.AssertNotNil(t, err)
}

func TestSetGet(t *testing.T) {
	var (
		err    error
		hasKey bool
	)

	tmpdir, err := ioutil.TempDir("", "goka_storage_TestSetGet")
	test.AssertNil(t, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	test.AssertNil(t, err)

	storage, err := New(db)
	test.AssertNil(t, err)

	hasKey, err = storage.Has("example1")
	test.AssertNil(t, err)
	test.AssertFalse(t, hasKey)

	value, err := storage.Get("example1")
	test.AssertTrue(t, value == nil)
	test.AssertNil(t, err)

	err = storage.Set("example1", []byte("example-message"))
	test.AssertNil(t, err)

	hasKey, err = storage.Has("example1")
	test.AssertNil(t, err)
	test.AssertTrue(t, hasKey)

	value, err = storage.Get("example1")
	test.AssertNil(t, err)

	test.AssertNil(t, storage.Delete("example1"))
	hasKey, err = storage.Has("example1")
	test.AssertNil(t, err)
	test.AssertFalse(t, hasKey)

	// test iteration
	test.AssertNil(t, storage.Set("key1", []byte("value1")))
	test.AssertNil(t, storage.Set("key2", []byte("value2")))
	iter, err := storage.Iterator()
	test.AssertNil(t, err)
	defer iter.Release()
	messages := make(map[string]string)

	for iter.Next() {
		key := string(iter.Key())
		val, err := iter.Value()
		test.AssertNil(t, err)
		messages[key] = string(val)
	}
	test.AssertTrue(t, len(messages) == 2)
	test.AssertEqual(t, messages["key1"], "value1")
	test.AssertEqual(t, messages["key2"], "value2")

	recoveredValue := string(value)
	test.AssertEqual(t, recoveredValue, "example-message")
}

func TestLeveldbStorage(t *testing.T) {

	path, err := ioutil.TempDir("", "goka_storage_leveldb_test")
	test.AssertNil(t, err)

	newStorage := func(delete bool, t *testing.T) Storage {

		if delete {
			os.RemoveAll(path)
		}

		db, err := leveldb.OpenFile(path, nil)
		test.AssertNil(t, err)

		st, err := New(db)
		test.AssertNil(t, err)
		return st
	}

	t.Run("getset", func(t *testing.T) {
		st := newStorage(true, t)

		st.Open()
		time.Sleep(1 * time.Second)
		offset, err := st.GetOffset(0)
		test.AssertEqual(t, offset, int64(0))
		test.AssertNil(t, err)

		test.AssertNil(t, st.SetOffset(100))
		offset, err = st.GetOffset(0)
		test.AssertEqual(t, offset, int64(100))
		test.AssertNil(t, err)
	})

	t.Run("set-reopen", func(t *testing.T) {
		st := newStorage(true, t)

		st.Open()
		time.Sleep(1 * time.Second)
		offset, err := st.GetOffset(0)
		test.AssertEqual(t, offset, int64(0))
		test.AssertNil(t, err)

		test.AssertNil(t, st.SetOffset(100))
		test.AssertNil(t, st.Close())

		st = newStorage(false, t)
		offset, err = st.GetOffset(0)
		test.AssertEqual(t, offset, int64(100))
		test.AssertNil(t, err)
	})

	t.Run("mark-recovered-reopen", func(t *testing.T) {
		st := newStorage(true, t)

		st.Open()
		time.Sleep(1 * time.Second)
		offset, err := st.GetOffset(0)
		test.AssertEqual(t, offset, int64(0))
		test.AssertNil(t, err)

		test.AssertNil(t, st.SetOffset(100))

		st.MarkRecovered()

		test.AssertNil(t, st.SetOffset(101))

		test.AssertNil(t, st.Close())
		st = newStorage(false, t)
		offset, err = st.GetOffset(0)
		test.AssertEqual(t, offset, int64(101))
		test.AssertNil(t, err)

	})
}
