package codec

type Closer interface {
	Close()
}

type nullCloser struct{}

func (n *nullCloser) Close() error { return nil }

var NoopCloser = new(nullCloser)
