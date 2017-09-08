package storage

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/lovoo/goka/codec"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestIterator(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "goka_storage_TestIterator")
	ensure.Nil(t, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	ensure.Nil(t, err)

	st, err := New(db, &codec.String{})
	ensure.Nil(t, err)

	kv := map[string]string{
		"key-1": "val-1",
		"key-2": "val-2",
		"key-3": "val-3",
	}

	for k, v := range kv {
		ensure.Nil(t, st.Set(k, v))
	}

	ensure.Nil(t, st.SetOffset(777))

	iter, err := st.Iterator()
	ensure.Nil(t, err)
	defer iter.Release()
	count := 0

	// accessing iterator before Next should only return nils
	val, err := iter.Value()
	ensure.Nil(t, val)
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
		ensure.DeepEqual(t, expected, val.(string))
	}
	ensure.DeepEqual(t, count, len(kv))
}

type erroringCodec struct{}

func (ec *erroringCodec) Encode(val interface{}) ([]byte, error) {
	return []byte(val.(string)), nil
}

func (ec *erroringCodec) Decode(data []byte) (interface{}, error) {
	return nil, fmt.Errorf("decoding error")
}

func TestIterator_DecodingError(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "goka_storage_TestIterator")
	ensure.Nil(t, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	ensure.Nil(t, err)

	st, err := New(db, &erroringCodec{})
	ensure.Nil(t, err)

	ensure.Nil(t, st.Set("key-1", "val-1"))

	iter, err := st.Iterator()
	ensure.Nil(t, err)
	defer iter.Release()

	count := 0
	for iter.Next() {
		count++
		val, err := iter.Value()
		ensure.Err(t, err, regexp.MustCompile("decoding error$"))
		ensure.Nil(t, val)
	}
	ensure.DeepEqual(t, count, 1)
}
