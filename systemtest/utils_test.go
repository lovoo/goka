package systemtest

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/storage"
)

// polls all pollers until all return true or fails the test when secTimeout has passed.
func pollTimed(t *testing.T, what string, secTimeout float64, pollers ...func() bool) {
	for i := 0; i < int(secTimeout/0.02); i++ {
		var ok = true
		for _, poller := range pollers {
			if !poller() {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waiting for %s timed out", what)
}

// hash the key using number of partitions using goka's default hashing mechanism
func hashKey(key string, numPartitions int) int32 {
	hasher := goka.DefaultHasher()()
	hasher.Write([]byte(key))
	hash := int32(hasher.Sum32())
	if hash < 0 {
		hash = -hash
	}
	return int32(int(hash) % numPartitions)
}

// simple map of memory-storages that can be passed as a builder into goka-Processors for testing.
// This allows for storage-inspection after during the test.
type storageTracker struct {
	sync.Mutex
	storages map[string]storage.Storage
}

func newStorageTracker() *storageTracker {
	return &storageTracker{
		storages: make(map[string]storage.Storage),
	}
}

func (st *storageTracker) Build(topic string, partition int32) (storage.Storage, error) {
	st.Lock()
	defer st.Unlock()
	key := st.key(topic, partition)
	if existing, ok := st.storages[key]; ok {
		return existing, nil
	}
	st.storages[key] = storage.NewMemory()
	return st.storages[key], nil
}

func (st *storageTracker) key(topic string, partition int32) string {
	return fmt.Sprintf("%s.%d", topic, partition)
}
