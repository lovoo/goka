package goka

import (
	"os"
	"testing"

	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/storage"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestIterator(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "goka_storage_TestIterator")
	require.NoError(t, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	require.NoError(t, err)

	st, err := storage.New(db)
	require.NoError(t, err)

	kv := map[string]string{
		"key-1": "val-1",
		"key-2": "val-2",
		"key-3": "val-3",
	}

	for k, v := range kv {
		require.NoError(t, st.Set(k, []byte(v)))
	}

	require.NoError(t, st.SetOffset(777))

	iter, err := st.Iterator()
	require.NoError(t, err)

	it := &iterator{
		iter:  storage.NewMultiIterator([]storage.Iterator{iter}),
		codec: convertOrFakeCodec(&codec.String{}),
	}
	defer it.Release()
	count := 0

	// accessing iterator before Next should only return nils
	val, err := it.Value()
	require.True(t, val == nil)
	require.NoError(t, err)

	for it.Next() {
		count++
		key := it.Key()
		expected, ok := kv[key]
		if !ok {
			t.Fatalf("unexpected key from iterator: %s", key)
		}

		val, err := it.Value()
		require.NoError(t, err)
		require.Equal(t, val.(string), expected)
	}

	if err := it.Err(); err != nil {
		t.Fatalf("unexpected iteration error: %v", err)
	}

	require.Equal(t, len(kv), count)
}
