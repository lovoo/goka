package storage

import (
	"fmt"

	ldbiter "github.com/syndtr/goleveldb/leveldb/iterator"
)

// iterator wraps an Iterator implementation and handles the value decoding and
// offset key skipping.
type iterator struct {
	iter  ldbiter.Iterator
	codec Codec
}

func (i *iterator) Next() bool {
	next := i.iter.Next()
	if string(i.iter.Key()) == offsetKey {
		next = i.iter.Next()
	}

	return next
}

func (i *iterator) Key() []byte {
	return i.iter.Key()
}

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

func (i *iterator) Release() {
	i.iter.Release()
}
