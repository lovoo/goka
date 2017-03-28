package storage

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/facebookgo/ensure"
)

var keys []string
var numKeys = 100
var numWrites = defaultStorageMaxBatchSize

func init() {
	for i := 0; i < numKeys; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkStateStorage_unbatched(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "benchmark_statestorage_unbatched")
	ensure.Nil(b, err)
	storage, err := newLeveldbStorage(tmpdir, false)
	ensure.Nil(b, err)
	b.ResetTimer()
	for i := 0; i < b.N*numWrites; i++ {
		storage.Set(keys[i%len(keys)], []byte(fmt.Sprintf("value-%d", i)))
	}
	storage.Close()
}

func BenchmarkStateStorage_batched(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "benchmark_statestorage_batched")
	ensure.Nil(b, err)
	storage, err := newLeveldbStorage(tmpdir, true)
	ensure.Nil(b, err)
	b.ResetTimer()
	for i := 0; i < b.N*numWrites; i++ {
		storage.Set(keys[i%len(keys)], []byte(fmt.Sprintf("value-%d", i)))
	}
	storage.Close()
}
