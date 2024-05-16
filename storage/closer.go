package storage

type nopCloser struct{}

func (n *nopCloser) Close() error { return nil }

var noopCloser = new(nopCloser)
