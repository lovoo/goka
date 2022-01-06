package storage

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/util"
)

type memiter struct {
	current int
	keys    []string
	storage map[string][]byte
}

func (i *memiter) exhausted() bool {
	return len(i.keys) <= i.current
}

func (i *memiter) Next() bool {
	i.current++
	if string(i.Key()) == offsetKey {
		i.current++
	}
	return !i.exhausted()
}

func (*memiter) Err() error {
	return nil
}

func (i *memiter) Key() []byte {
	if i.exhausted() {
		return nil
	}

	return []byte(i.keys[i.current])
}

func (i *memiter) Value() ([]byte, error) {
	if i.exhausted() {
		return nil, nil
	}

	return i.storage[i.keys[i.current]], nil
}

func (i *memiter) Release() {
	// mark the iterator as exhausted
	i.current = len(i.keys)
}

func (i *memiter) Seek(key []byte) bool {
	if i.exhausted() {
		return false
	}

	if i.current < 0 && !i.Next() {
		return false
	}

	for i.current < len(i.keys) {
		if strings.HasPrefix(i.keys[i.current], string(key)) {
			break
		}
		i.current++
	}
	return !i.exhausted()
}

type memory struct {
	sync.RWMutex
	keys      []string
	storage   map[string][]byte
	offset    *int64
	recovered bool
}

// NewMemory returns a new in-memory storage.
func NewMemory() Storage {
	return &memory{
		keys:      make([]string, 0),
		storage:   make(map[string][]byte),
		recovered: false,
	}
}

func (m *memory) Has(key string) (bool, error) {
	m.RLock()
	defer m.RUnlock()

	_, has := m.storage[key]
	return has, nil
}

func (m *memory) Get(key string) ([]byte, error) {
	m.RLock()
	defer m.RUnlock()

	value := m.storage[key]
	return value, nil
}

func (m *memory) Set(key string, value []byte) error {
	m.Lock()
	defer m.Unlock()
	if value == nil {
		return fmt.Errorf("cannot write nil value")
	}
	if _, exists := m.storage[key]; !exists {
		m.keys = append(m.keys, key)
		sort.Strings(m.keys)
	}
	m.storage[key] = value
	return nil
}

func (m *memory) Delete(key string) error {
	m.Lock()
	defer m.Unlock()
	delete(m.storage, key)
	for i, k := range m.keys {
		if k == key {
			m.keys = append(m.keys[:i], m.keys[i+1:]...)
			break
		}
	}
	return nil
}

// Returns an interator on a snapshot of the storage. It's safe to modify
// the storage while using this iterator, but it won't be aware of such changes.
// The iterator is not concurrency-safe, but multiple iterators can
// be used concurrently.
func (m *memory) Iterator() (Iterator, error) {
	m.RLock()
	defer m.RUnlock()
	keys := make([]string, len(m.keys))
	copy(keys, m.keys)
	storage := make(map[string][]byte, len(m.storage))
	for k, v := range m.storage {
		storage[k] = v
	}
	return &memiter{-1, keys, storage}, nil
}

// Returns an interator on a snapshot of the storage within the specified range of keys.
// It's safe to modify the storage while using this iterator, but it won't be aware of such changes.
// The iterator is not concurrency-safe, but multiple iterators can
// be used concurrently.
func (m *memory) IteratorWithRange(start, limit []byte) (Iterator, error) {
	m.RLock()
	defer m.RUnlock()
	keys := []string{} // using slice as keys has an unknown size
	if len(limit) == 0 {
		limit = util.BytesPrefix(start).Limit
	}
	storage := make(map[string][]byte)
	for _, k := range m.keys {
		if bytes.Compare([]byte(k), start) > -1 && bytes.Compare([]byte(k), limit) < 1 {
			keys = append(keys, k)
			storage[k] = m.storage[k]
		}
	}

	return &memiter{-1, keys, storage}, nil
}

func (m *memory) MarkRecovered() error {
	return nil
}

func (m *memory) Recovered() bool {
	return m.recovered
}

func (m *memory) SetOffset(offset int64) error {
	m.Lock()
	defer m.Unlock()
	m.offset = &offset
	return nil
}

func (m *memory) GetOffset(defValue int64) (int64, error) {
	m.RLock()
	defer m.RUnlock()
	if m.offset == nil {
		return defValue, nil
	}

	return *m.offset, nil
}

func (m *memory) Open() error {
	return nil
}

func (m *memory) Close() error {
	return nil
}
