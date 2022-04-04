package storage

import (
	"fmt"
	"io/ioutil"
	"testing"

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
