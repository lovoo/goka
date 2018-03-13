package goka

import "sync"

type once struct {
	once sync.Once
	err  error
}

// Do runs only once and always return the same error.
func (o *once) Do(f func() error) error {
	o.once.Do(func() { o.err = f() })
	return o.err
}
