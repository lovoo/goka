package storage

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/lovoo/goka/codec"
	"github.com/syndtr/goleveldb/leveldb"
)

var keys []string
var numKeys = 100
var numWrites = 200000

func init() {
	for i := 0; i < numKeys; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkStateStorage_unbatched(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "benchmark_statestorage_unbatched")
	ensure.Nil(b, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	ensure.Nil(b, err)

	storage, err := New(db, &codec.Bytes{})
	ensure.Nil(b, err)
	ensure.Nil(b, storage.MarkRecovered())
	b.ResetTimer()
	for i := 0; i < b.N*numWrites; i++ {
		storage.Set(keys[i%len(keys)], []byte(fmt.Sprintf("value-%d", i)))
	}
	storage.Close()
}

func BenchmarkStateStorage_transactioned(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "benchmark_statestorage_transactioned")
	ensure.Nil(b, err)

	db, err := leveldb.OpenFile(tmpdir, nil)
	ensure.Nil(b, err)

	storage, err := New(db, &codec.Bytes{})
	ensure.Nil(b, err)
	b.ResetTimer()
	for i := 0; i < b.N*numWrites; i++ {
		storage.Set(keys[i%len(keys)], []byte(fmt.Sprintf("value-%d", i)))
	}
	ensure.Nil(b, storage.MarkRecovered())
	storage.Close()
}
