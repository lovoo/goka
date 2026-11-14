package storage

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// shuffledKeys returns n distinct keys in random order, as a recovering partition receives them.
func shuffledKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%08d", i)
	}
	rnd := rand.New(rand.NewSource(1))
	rnd.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	return keys
}

func collectKeys(t *testing.T, it Iterator) []string {
	t.Helper()
	defer it.Release()
	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	require.NoError(t, it.Err())
	return keys
}

func TestMemoryIteratorOrder(t *testing.T) {
	mem := NewMemory()
	keys := shuffledKeys(1000)
	for _, k := range keys {
		require.NoError(t, mem.Set(k, []byte(k)))
	}
	// overwrite some keys and delete others: neither may duplicate or resurrect a key
	for _, k := range keys[:100] {
		require.NoError(t, mem.Set(k, []byte("updated")))
	}
	deleted := keys[100:200]
	for _, k := range deleted {
		require.NoError(t, mem.Delete(k))
	}
	require.NoError(t, mem.Delete("never-written"))

	want := append([]string(nil), keys[:100]...)
	want = append(want, keys[200:]...)
	sort.Strings(want)

	it, err := mem.Iterator()
	require.NoError(t, err)
	require.Equal(t, want, collectKeys(t, it))

	// a range iterator returns the matching keys in order too
	it, err = mem.IteratorWithRange([]byte("key-0000"), nil)
	require.NoError(t, err)
	var wantRange []string
	for _, k := range want {
		if k[:8] == "key-0000" {
			wantRange = append(wantRange, k)
		}
	}
	require.Equal(t, wantRange, collectKeys(t, it))

	// keys written after an iterator was taken are not visible to it, but are to the next one
	it, err = mem.Iterator()
	require.NoError(t, err)
	require.NoError(t, mem.Set("key-99999999", []byte("late")))
	require.Equal(t, want, collectKeys(t, it))
	it, err = mem.Iterator()
	require.NoError(t, err)
	require.Equal(t, append(want, "key-99999999"), collectKeys(t, it))
}

// BenchmarkMemoryRecover writes n distinct keys in random order, which is what recovering a
// table partition into memory storage does.
func BenchmarkMemoryRecover(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 50_000} {
		keys := shuffledKeys(n)
		value := []byte("value")
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			for b.Loop() {
				mem := NewMemory()
				for _, k := range keys {
					if err := mem.Set(k, value); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkMemoryDelete deletes n keys in random order, as a partition full of tombstones does.
func BenchmarkMemoryDelete(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 50_000} {
		keys := shuffledKeys(n)
		value := []byte("value")
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			for b.Loop() {
				b.StopTimer()
				mem := NewMemory()
				for _, k := range keys {
					if err := mem.Set(k, value); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				for _, k := range keys {
					if err := mem.Delete(k); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
