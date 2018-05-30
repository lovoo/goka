package storage

import (
	"fmt"
	"io/ioutil"
	"sort"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/facebookgo/ensure"
)

func TestMemStorageDelete(t *testing.T) {
	storage := NewMemory()

	has, err := storage.Has("key-1")
	ensure.Nil(t, err)
	ensure.False(t, has)

	err = storage.Set("key-1", []byte("content-1"))
	ensure.Nil(t, err)

	has, err = storage.Has("key-1")
	ensure.Nil(t, err)
	ensure.True(t, has)

	err = storage.Delete("key-1")
	ensure.Nil(t, err)

	has, err = storage.Has("key-1")
	ensure.Nil(t, err)
	ensure.False(t, has)
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
	ensure.Nil(t, err)
	iter.Release()
	ensure.False(t, iter.Next(), "released iterator had a next")

	iter, err = storage.Iterator()
	ensure.Nil(t, err)
	for iter.Next() {
		raw, err := iter.Value()
		ensure.Nil(t, err)

		key := string(iter.Key())
		val := string(raw)

		v, ok := kv[key]
		ensure.True(t, ok, fmt.Sprintf("unexpected key returned from iterator: %s", key))
		ensure.DeepEqual(t, val, v, fmt.Sprintf("iterator returned wrong value: %s, expected: %s", val, v))

		found[key] = val
	}

	key := iter.Key()
	val, err := iter.Value()
	ensure.Nil(t, err, "exhausted iterator should not return error")
	ensure.True(t, key == nil, fmt.Sprintf("exhausted iterator should return nil key, returned: %s", key))
	ensure.True(t, val == nil, "exhausted iterator should return nil value, returned %s", val)

	ensure.DeepEqual(t, found, kv, "found doesn't match kv, iterator probably didn't return all values")

	k := []byte("key-1")
	iter, err = storage.IteratorWithRange(k, nil)
	sort.Strings(iter.(*memiter).keys) // make iteration order deterministic

	ensure.True(t, iter.Next(), "next should return true after a IteratorWithRange")
	ensure.DeepEqual(t, iter.Key(), k, "the first matching key in IteratorWithRange is not corresponding to the value")

}

func TestGetHas(t *testing.T) {
	storage := NewMemory()

	var (
		err    error
		hasKey bool
	)

	hasKey, err = storage.Has("test-key")
	ensure.Nil(t, err)
	ensure.False(t, hasKey)

	value, err := storage.Get("test-key")
	ensure.True(t, value == nil)
	ensure.Nil(t, err)

	err = storage.Set("test-key", []byte("test"))
	ensure.Nil(t, err)

	hasKey, err = storage.Has("test-key")
	ensure.Nil(t, err)
	ensure.True(t, hasKey)

	value, err = storage.Get("test-key")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, value, []byte("test"))

	hasKey, err = storage.Has("nil-value")
	ensure.Nil(t, err)
	ensure.False(t, hasKey)

	err = storage.Set("nil-value", nil)
	ensure.NotNil(t, err)
}

func TestSetGet(t *testing.T) {
	var (
		err    error
		hasKey bool
	)

	tmpdir, err := ioutil.TempDir("", "goka_storage_TestSetGet")
	ensure.Nil(t, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	ensure.Nil(t, err)

	storage, err := New(db)
	ensure.Nil(t, err)

	hasKey, err = storage.Has("example1")
	ensure.Nil(t, err)
	ensure.False(t, hasKey)

	value, err := storage.Get("example1")
	ensure.True(t, value == nil)
	ensure.Nil(t, err)

	err = storage.Set("example1", []byte("example-message"))
	ensure.Nil(t, err)

	hasKey, err = storage.Has("example1")
	ensure.Nil(t, err)
	ensure.True(t, hasKey)

	value, err = storage.Get("example1")
	ensure.Nil(t, err)

	ensure.Nil(t, storage.Delete("example1"))
	hasKey, err = storage.Has("example1")
	ensure.Nil(t, err)
	ensure.False(t, hasKey)

	// test iteration
	ensure.Nil(t, storage.Set("key1", []byte("value1")))
	ensure.Nil(t, storage.Set("key2", []byte("value2")))
	iter, err := storage.Iterator()
	ensure.Nil(t, err)
	defer iter.Release()
	messages := make(map[string]string)

	for iter.Next() {
		key := string(iter.Key())
		val, err := iter.Value()
		ensure.Nil(t, err)
		messages[key] = string(val)
	}
	ensure.True(t, len(messages) == 2, fmt.Sprintf("expected 2 messages, got: %d", len(messages)))
	ensure.DeepEqual(t, messages["key1"], "value1")
	ensure.DeepEqual(t, messages["key2"], "value2")

	recoveredValue := string(value)
	ensure.DeepEqual(t, recoveredValue, "example-message")
}
