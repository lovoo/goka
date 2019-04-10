package storage

type multiIterator struct {
	current int
	// "current" iterators, that might be limited to iters that contain
	// some Seek'ed values
	iters []Iterator
	// original iters
	origIters []Iterator
}

// NewMultiIterator returns an iterator that iterates over the given iterators.
func NewMultiIterator(iters []Iterator) Iterator {
	if len(iters) == 0 {
		return &NullIter{}
	}

	return &multiIterator{
		current:   0,
		iters:     iters,
		origIters: iters,
	}
}

func (m *multiIterator) Next() bool {
	for ; m.current < len(m.iters); m.current++ {
		if m.iters[m.current].Next() {
			return true
		}
	}
	return false
}

func (m *multiIterator) Key() []byte {
	return m.iters[m.current].Key()
}

func (m *multiIterator) Value() ([]byte, error) {
	return m.iters[m.current].Value()
}

func (m *multiIterator) Release() {
	for i := range m.iters {
		m.iters[i].Release()
	}
	m.current = 0
	m.iters = nil
}

func (m *multiIterator) Seek(key []byte) bool {
	m.current = 0
	m.iters = nil

	// at least one iterator's seek was successful
	ok := false

	// iterate over all (original) iterators
	for _, iter := range m.origIters {
		// if it matches, apply it to the list of iters we'll be using for the next Next()
		if iter.Seek(key) {
			m.iters = append(m.iters, iter)
			ok = true
		}
	}

	return ok
}
