package goka

import (
	"github.com/lovoo/goka/storage"
)

// Iterator allows one to iterate over the keys of a view.
type Iterator interface {
	Next() bool
	Key() string
	Value() (interface{}, error)
	Release()
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

// Releases releases the iterator. The iterator is not usable anymore after calling Release.
func (i *iterator) Release() {
	i.iter.Release()
}

func (i *iterator) Seek(key string) bool {
	return i.iter.Seek([]byte(key))
}
