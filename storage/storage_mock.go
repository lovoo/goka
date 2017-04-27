package storage

import "fmt"

type storageMock struct {
	storage map[string]interface{}
	offset  *int64
	c       Codec
}

// NewMock creates a mock storage for testing
func NewMock(c Codec) Storage {
	return &storageMock{
		storage: make(map[string]interface{}),
		c:       c,
	}
}

type memiter struct {
	current int
	keys    []string
	storage map[string]interface{}
}

func (i *memiter) exhausted() bool {
	return len(i.keys) <= i.current
}

func (i *memiter) Next() bool {
	i.current++
	return !i.exhausted()
}

func (i *memiter) Key() []byte {
	if i.exhausted() {
		return nil
	}

	return []byte(i.keys[i.current])
}

func (i *memiter) Value() (interface{}, error) {
	if i.exhausted() {
		return nil, nil
	}

	return i.storage[i.keys[i.current]], nil
}

func (i *memiter) Release() {
	// mark the iterator as exhausted
	i.current = len(i.keys)
}

func (sm *storageMock) Iterator() Iterator {
	keys := make([]string, 0, len(sm.storage))
	for k := range sm.storage {
		keys = append(keys, k)
	}

	return &memiter{-1, keys, sm.storage}
}

func (sm *storageMock) Has(key string) (bool, error) {
	_, has := sm.storage[key]
	return has, nil
}

func (sm *storageMock) Get(key string) (interface{}, error) {
	value, _ := sm.storage[key]
	return value, nil
}

func (sm *storageMock) SetEncoded(key string, data []byte) error {
	decoded, err := sm.c.Decode(data)
	if err != nil {
		return fmt.Errorf("Error decoding data: %v", err)
	}
	sm.storage[key] = decoded
	return nil
}

func (sm *storageMock) Set(key string, value interface{}) error {
	if value == nil {
		return fmt.Errorf("cannot write nil value")
	}
	sm.storage[key] = value
	return nil
}

func (sm *storageMock) Delete(key string) error {
	delete(sm.storage, key)
	return nil
}

func (sm *storageMock) SetOffset(offset int64) error {
	sm.offset = &offset
	return nil
}

func (sm *storageMock) GetOffset(defValue int64) (int64, error) {
	if sm.offset == nil {
		return defValue, nil
	}

	return *sm.offset, nil
}
func (sm *storageMock) Open() error {
	return nil
}
func (sm *storageMock) Sync() {
}

func (sm *storageMock) Close() error {
	return nil
}
