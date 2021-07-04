package goka

import (
	"github.com/lovoo/goka/storage"
)

// Iterator allows one to iterate over the keys of a view.
type Iterator interface {
	// Next advances the iterator to the next KV-pair. Err should be called
	// after Next returns false to check whether the iteration finished
	// from exhaustion or was aborted due to an error.
	Next() bool
	// Err returns the error that stopped the iteration if any.
	Err() error
	// Return the key of the current item
	Key() string
	// Return the value of the current item
	// This value is already decoded with the view's codec (or nil, if it's nil)
	Value() (interface{}, error)
	// Release the iterator. After release, the iterator is not usable anymore
	Release()
	// Seek moves the iterator to the begining of a key-value pair sequence that
	// is greater or equal to the given key. It returns whether at least one of
	// such key-value pairs exist. If true is returned, Key/Value must be called
	// immediately to get the first item. Calling Next immediately after a successful
	// seek will effectively skip an item in the iterator.
	Seek(key string) bool
}

type iterator struct {
	iter  storage.Iterator
	codec Codec
}

// Next advances the iterator to the next key.
func (i *iterator) Next() bool {
	return i.iter.Next()
}

// Key returns the current key.
func (i *iterator) Key() string {
	return string(i.iter.Key())
}

// Value returns the current value decoded by the codec of the storage.
func (i *iterator) Value() (interface{}, error) {
	data, err := i.iter.Value()
	if err != nil {
		return nil, err
	} else if data == nil {
		return nil, nil
	}
	return i.codec.Decode(data)
}

// Err returns the possible iteration error.
func (i *iterator) Err() error {
	return i.iter.Err()
}

// Releases releases the iterator. The iterator is not usable anymore after calling Release.
func (i *iterator) Release() {
	i.iter.Release()
}

func (i *iterator) Seek(key string) bool {
	return i.iter.Seek([]byte(key))
}
