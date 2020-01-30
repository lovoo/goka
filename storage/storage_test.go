package storage

import (
	"io/ioutil"
	"sort"
	"testing"

	"github.com/lovoo/goka/internal/test"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestMemStorageDelete(t *testing.T) {
	storage := NewMemory()

	has, err := storage.Has("key-1")
	test.AssertNil(t, err)
	test.AssertFalse(t, has)

	err = storage.Set("key-1", []byte("content-1"))
	test.AssertNil(t, err)

	has, err = storage.Has("key-1")
	test.AssertNil(t, err)
	test.AssertTrue(t, has)

	err = storage.Delete("key-1")
	test.AssertNil(t, err)

	has, err = storage.Has("key-1")
	test.AssertNil(t, err)
	test.AssertFalse(t, has)
}

func TestMemIter(t *testing.T) {
	storage := NewMemory()

	kv := map[string]string{
		"key-1": "val-1",
		"key-2": "val-2",
		"key-3": "val-3",
	}

	found := map[string]string{}

	storage.Set(offsetKey, []byte("not-returned"))
	for k, v := range kv {
		storage.Set(k, []byte(v))
	}

	// released iterator should be immediately exhausted
	iter, err := storage.Iterator()
	test.AssertNil(t, err)
	iter.Release()
	test.AssertFalse(t, iter.Next())

	iter, err = storage.Iterator()
	test.AssertNil(t, err)
	for iter.Next() {
		raw, err := iter.Value()
		test.AssertNil(t, err)

		key := string(iter.Key())
		val := string(raw)

		v, ok := kv[key]
		test.AssertTrue(t, ok)
		test.AssertEqual(t, val, v)

		found[key] = val
	}

	key := iter.Key()
	val, err := iter.Value()
	test.AssertNil(t, err)
	test.AssertTrue(t, key == nil)
	test.AssertTrue(t, val == nil)

	test.AssertEqual(t, found, kv)

	k := []byte("key-1")
	iter, err = storage.IteratorWithRange(k, nil)
	sort.Strings(iter.(*memiter).keys) // make iteration order deterministic

	test.AssertTrue(t, iter.Next())
	test.AssertEqual(t, iter.Key(), k)

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
