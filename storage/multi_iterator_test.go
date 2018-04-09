package storage

import (
	"fmt"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestMultiIterator(t *testing.T) {
	var (
		numStorages = 3
		numValues   = 3
		storages    = make([]Storage, numStorages)
		expected    = make(map[string]string)
	)

	for i := 0; i < numStorages; i++ {
		storages[i] = NewMemory()
		for j := 0; j < numValues; j++ {
			key := fmt.Sprintf("key-%d-%d", i, j)
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

	k := []byte("key-2")
	iter = NewMultiIterator(iters)
	ensure.True(t, iter.Seek(k), "seek return false should return true")

	total := 0
	for iter.Next() {
		_, err := iter.Value()
		ensure.Nil(t, err)
		total++
	}
	ensure.DeepEqual(t, total, 3, "not enough element found in iter seek")
}

func TestMultiIteratorOneValue(t *testing.T) {
	numStorages := 3
	storages := make([]Storage, numStorages)

	// first two storages are empty
	storages[0] = NewMemory()
	storages[1] = NewMemory()

	// add one value to the last one
	storages[numStorages-1] = NewMemory()
	key := fmt.Sprintf("storage-%d", numStorages-1)
	val := fmt.Sprintf("value-%d", 1)
	storages[numStorages-1].Set(key, []byte(val))

	iters := make([]Iterator, len(storages))
	for i := range storages {
		iter, err := storages[i].Iterator()
		ensure.Nil(t, err)
		iters[i] = iter
	}

	iter := NewMultiIterator(iters)
	k := []byte("storage-2")
	ensure.True(t, iter.Next(), "Iterator should have a value")
	ensure.DeepEqual(t, iter.Key(), k, "key mismatch")
	ensure.False(t, iter.Next())
}

func TestMultiIteratorMixedValues(t *testing.T) {
	var (
		n        = 0
		storages = make([]Storage, 3)
		expected = make(map[string]string)
		counts   = make(map[string]int)
	)

	// first storage has two values
	storages[0] = NewMemory()
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("key-%d", n)
		val := fmt.Sprintf("value-%d", n)
		expected[key] = val
		storages[0].Set(key, []byte(val))
		n++
	}

	// second storage has 0 values
	storages[1] = NewMemory()

	// third storage has three values
	storages[2] = NewMemory()
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key-%d", n)
		val := fmt.Sprintf("value-%d", n)
		expected[key] = val
		storages[2].Set(key, []byte(val))
		n++
	}

	iters := make([]Iterator, len(storages))
	for i := range storages {
		iter, err := storages[i].Iterator()
		ensure.Nil(t, err)
		iters[i] = iter
	}

	iter := NewMultiIterator(iters)
	for iter.Next() {
		val, err := iter.Value()
		ensure.Nil(t, err)
		ensure.DeepEqual(t, expected[string(iter.Key())], string(val))
		counts[string(iter.Key())]++
	}

	// uniques and totals should match
	unique := 0
	count := 0
	for _, c := range counts {
		unique++
		count += c
	}
	ensure.DeepEqual(t, count, len(expected))
	ensure.DeepEqual(t, unique, len(expected))
}
