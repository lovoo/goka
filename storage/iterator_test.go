package storage

import (
	"io/ioutil"
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestIterator(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "goka_storage_TestIterator")
	ensure.Nil(t, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	ensure.Nil(t, err)

	st, err := New(db)
	ensure.Nil(t, err)

	kv := map[string]string{
		"key-1": "val-1",
		"key-2": "val-2",
		"key-3": "val-3",
	}

	for k, v := range kv {
		ensure.Nil(t, st.Set(k, []byte(v)))
	}

	ensure.Nil(t, st.SetOffset(777))

	iter, err := st.Iterator()
	ensure.Nil(t, err)
	defer iter.Release()
	count := 0

	// accessing iterator before Next should only return nils
	val, err := iter.Value()
	ensure.True(t, val == nil)
	ensure.Nil(t, err)

	for iter.Next() {
		count++
		key := string(iter.Key())
		expected, ok := kv[key]
		if !ok {
			t.Fatalf("unexpected key from iterator: %s", key)
		}

		val, err := iter.Value()
		ensure.Nil(t, err)
		ensure.DeepEqual(t, expected, string(val))
	}
	ensure.DeepEqual(t, count, len(kv))
}
