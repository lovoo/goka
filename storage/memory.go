package storage

import "fmt"

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

func (m *memory) Iterator() (Iterator, error) {
	keys := make([]string, 0, len(m.storage))
	for k := range m.storage {
		keys = append(keys, k)
	}

	return &memiter{-1, keys, m.storage}, nil
}

type memory struct {
	storage   map[string][]byte
	offset    *int64
	recovered bool
}

// NewMemory returns a new in-memory storage.
func NewMemory() Storage {
	return &memory{
		storage:   make(map[string][]byte),
		recovered: false,
	}
}

func (m *memory) Has(key string) (bool, error) {
	_, has := m.storage[key]
	return has, nil
}

func (m *memory) Get(key string) ([]byte, error) {
	value, _ := m.storage[key]
	return value, nil
}

func (m *memory) Set(key string, value []byte) error {
	if value == nil {
		return fmt.Errorf("cannot write nil value")
	}
	m.storage[key] = value
	return nil
}

func (m *memory) Delete(key string) error {
	delete(m.storage, key)
	return nil
}

func (m *memory) MarkRecovered() error {
	return nil
}

func (m *memory) Recovered() bool {
	return m.recovered
}

func (m *memory) SetOffset(offset int64) error {
	m.offset = &offset
	return nil
}

func (m *memory) GetOffset(defValue int64) (int64, error) {
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
