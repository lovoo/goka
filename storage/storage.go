package storage

const (
	offsetKey = "__offset"
)

// Iterator provides iteration access to the stored values.
type Iterator interface {
	// Next moves the iterator to the next key-value pair and whether such a pair
	// exists. Caller should check for possible error by calling Error after Next
	// returns false.
	Next() bool
	// Err returns the error that stopped the iteration if any.
	Err() error
	// Key returns the current key. Caller should not keep references to the
	// buffer or modify its contents.
	Key() []byte
	// Value returns the current value. Caller should not keep references to the
	// buffer or modify its contents.
	Value() ([]byte, error)
	// Release releases the iterator. After release, the iterator is not usable
	// anymore.
	Release()
	// Seek moves the iterator to the begining of a key-value pair sequence that
	// is greater or equal to the given key. It returns whether at least one of
	// such key-value pairs exist. Next must be called after seeking to access
	// the first pair.
	Seek(key []byte) bool
}

// Storage is the interface Goka expects from a storage implementation.
// Implementations of this interface must be safe for any number of concurrent
// readers with one writer.
type Storage interface {
	// Opens/Initialize the storage
	Open() error

	// Close closes the storage.
	Close() error
	// Has returns whether the given key exists in the database.
	Has(key string) (bool, error)

	// Get returns the value associated with the given key. If the key does not
	// exist, a nil will be returned.
	Get(key string) ([]byte, error)

	// Set stores a key-value pair.
	Set(key string, value []byte) error

	// Delete deletes a key-value pair from the storage.
	Delete(key string) error

	// GetOffset gets the local offset of the storage.
	GetOffset(def int64) (int64, error)

	// SetOffset sets the local offset of the storage.
	SetOffset(offset int64) error

	// MarkRecovered marks the storage as recovered. Recovery message throughput
	// can be a lot higher than during normal operation. This can be used to switch
	// to a different configuration after the recovery is done.
	MarkRecovered() error

	// Iterator returns an iterator that traverses over a snapshot of the storage.
	Iterator() (Iterator, error)

	// Iterator returns a new iterator that iterates over the key-value
	// pairs. Start and limit define a half-open range [start, limit). If either
	// is nil, the range will be unbounded on the respective side.
	IteratorWithRange(start, limit []byte) (Iterator, error)
}
