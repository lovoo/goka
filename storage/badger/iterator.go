package badger

import (
	"github.com/dgraph-io/badger"
)

const (
	offsetKey = "__offset_key"
)

// iterator wraps a BadgerDB Iterator and handles offset key skipping.
type iterator struct {
	txn   *badger.Txn
	iter  *badger.Iterator
	initd bool
}

// Next advances the iterator to the next key.
func (i *iterator) Next() bool {
	if !i.initd {
		i.iter.Rewind()
		i.initd = true
	}

	if !i.iter.Valid() {
		return false
	}

	if string(i.iter.Item().Key()) == offsetKey {
		return i.Next()
	}

	return true
}

// Key returns the current key.
func (i *iterator) Key() []byte {
	return i.iter.Item().Key()
}

// Value returns the current value.
func (i *iterator) Value() ([]byte, error) {
	return i.iter.Item().Value()
}

// Releases releases the iterator and the associated transaction. The iterator is
// not usable anymore after calling Release.
func (i *iterator) Release() {
	i.iter.Close()
	i.txn.Discard()
}
