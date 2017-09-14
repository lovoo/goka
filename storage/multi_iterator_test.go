package storage

import (
	"fmt"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestMultiIterator(t *testing.T) {
	numStorages := 3
	numValues := 3

	storages := make([]Storage, numStorages)
	expected := map[string]string{}

	for i := 0; i < numStorages; i++ {
		storages[i] = NewMemory()
		for j := 0; j < numValues; j++ {
			key := fmt.Sprintf("storage-%d", i)
			val := fmt.Sprintf("value-%d", j)
			expected[key] = val
			storages[i].Set(key, []byte(val))
		}
	}

	iters := make([]Iterator, len(storages))
	for i := range storages {
		iter, err := storages[i].Iterator()
		ensure.Nil(t, err)
		iters[i] = iter
	}

	iter := NewMultiIterator(iters)
	count := 0
	for iter.Next() {
		val, err := iter.Value()
		ensure.Nil(t, err)
		ensure.DeepEqual(t, expected[string(iter.Key())], string(val))
		count++
	}

	ensure.DeepEqual(t, count, len(expected))
}
