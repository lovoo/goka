package codec

type nullCloser struct{}

func (n *nullCloser) Close() error { return nil }

// NoopCloser can be used for returning io.Closer interfaces, whose Close call does
// nothing.
var NoopCloser = new(nullCloser)
