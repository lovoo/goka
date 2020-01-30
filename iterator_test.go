package goka

import (
	"io/ioutil"
	"testing"

	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/internal/test"
	"github.com/lovoo/goka/storage"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestIterator(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "goka_storage_TestIterator")
	test.AssertNil(t, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	test.AssertNil(t, err)

	st, err := storage.New(db)
	test.AssertNil(t, err)

	kv := map[string]string{
		"key-1": "val-1",
		"key-2": "val-2",
		"key-3": "val-3",
	}

	for k, v := range kv {
		test.AssertNil(t, st.Set(k, []byte(v)))
	}

	test.AssertNil(t, st.SetOffset(777))

	iter, err := st.Iterator()
	test.AssertNil(t, err)

	it := &iterator{
		iter:  storage.NewMultiIterator([]storage.Iterator{iter}),
		codec: new(codec.String),
	}
	defer it.Release()
	count := 0

	// accessing iterator before Next should only return nils
	val, err := it.Value()
	test.AssertTrue(t, val == nil)
	test.AssertNil(t, err)

	for it.Next() {
		count++
		key := it.Key()
		expected, ok := kv[key]
		if !ok {
			t.Fatalf("unexpected key from iterator: %s", key)
		}

		val, err := it.Value()
		test.AssertNil(t, err)
		test.AssertEqual(t, expected, val.(string))
	}

	if err := it.Err(); err != nil {
		t.Fatalf("unexpected iteration error: %v", err)
	}

	test.AssertEqual(t, count, len(kv))
}
