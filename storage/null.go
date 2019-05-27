package storage

// Null storage discards everything that it is given. This can be useful for
// debugging.
type Null struct {
	recovered bool
}

// NewNull returns a new Null storage.
func NewNull() Storage {
	return new(Null)
}

// MarkRecovered does nothing.
func (n *Null) MarkRecovered() error {
	return nil
}

// Recovered returns whether the storage has recovered.
func (n *Null) Recovered() bool {
	return n.recovered
}

// Has returns false as in key not found.
func (n *Null) Has(key string) (bool, error) {
	return false, nil
}

// Get returns nil values.
func (n *Null) Get(key string) ([]byte, error) {
	return nil, nil
}

// Set will do nothing and doesn't error.
func (n *Null) Set(key string, val []byte) error {
	return nil
}

// Delete does nothing and doesn't error.
func (n *Null) Delete(string) error {
	return nil
}

// GetOffset returns the default offset given to it.
func (n *Null) GetOffset(def int64) (int64, error) {
	return def, nil
}

// SetOffset does nothing and doesn't error.
func (n *Null) SetOffset(val int64) error {
	return nil
}

// Iterator returns an Iterator that is immediately exhausted.
func (n *Null) Iterator() (Iterator, error) {
	return new(NullIter), nil
}

// IteratorWithRange returns an Iterator that is immediately exhausted.
func (n *Null) IteratorWithRange(start, limit []byte) (Iterator, error) {
	return new(NullIter), nil
}

// Open does nothing and doesn't error.
func (n *Null) Open() error {
	return nil
}

// Close does nothing and doesn't error
func (n *Null) Close() error {
	return nil
}

// NullIter is an iterator which is immediately exhausted.
type NullIter struct{}

// Next returns always false.
func (ni *NullIter) Next() bool {
	return false
}

func (*NullIter) Err() error {
	return nil
}

// Key returns always nil.
func (ni *NullIter) Key() []byte {
	return nil
}

// Value returns always a nil value and no errors.
func (ni *NullIter) Value() ([]byte, error) {
	return nil, nil
}

// Release does nothing.
func (ni *NullIter) Release() {}

// Seek do nothing
func (ni *NullIter) Seek(key []byte) bool { return false }
