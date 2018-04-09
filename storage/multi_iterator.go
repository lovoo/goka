package storage

type multiIterator struct {
	current int
	iters   []Iterator
}

// NewMultiIterator returns an iterator that iterates over the given iterators.
func NewMultiIterator(iters []Iterator) Iterator {
	if len(iters) == 0 {
		return &NullIter{}
	}

	return &multiIterator{current: 0, iters: iters}
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
	m.iters = []Iterator{&NullIter{}}
}

func (m *multiIterator) Seek(key []byte) bool {
	m.current = 0
	iters := []Iterator{}
	ok := false
	for i := range m.iters {
		if m.iters[i].Seek(key) {
			iters = append(iters, m.iters[i])
			ok = true
		}
	}
	return ok
}
