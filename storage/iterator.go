package storage

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	ldbiter "github.com/syndtr/goleveldb/leveldb/iterator"
)

// iterator wraps an Iterator implementation and handles the value decoding and
// offset key skipping.
type iterator struct {
	iter  ldbiter.Iterator
	codec Codec
	snap  *leveldb.Snapshot
}

// Next advances the iterator to the next key.
func (i *iterator) Next() bool {
	next := i.iter.Next()
	if string(i.iter.Key()) == offsetKey {
		next = i.iter.Next()
	}

	return next
}

// Key returns the current key.
func (i *iterator) Key() []byte {
	return i.iter.Key()
}

// Value returns the current value decoded by the codec of the storage.
func (i *iterator) Value() (interface{}, error) {
	data := i.iter.Value()
	if data == nil {
		return nil, nil
	}

	val, err := i.codec.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("error decoding iterator value (key %s): %v", string(i.Key()), err)
	}

	return val, nil
}

// Releases releases the iterator and the associated snapshot. The iterator is
// not usable anymore after calling Release.
func (i *iterator) Release() {
	i.iter.Release()
	i.snap.Release()
}
