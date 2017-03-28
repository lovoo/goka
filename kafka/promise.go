package kafka

import "sync"

// Promise as in https://en.wikipedia.org/wiki/Futures_and_promises
type Promise struct {
	sync.Mutex
	err      error
	finished bool

	callbacks []func(err error)
}

// NewPromise creates a new Promise
func NewPromise() *Promise {
	return new(Promise)
}

// execute all callbacks conveniently
// The caller needs to lock!
func (p *Promise) executeCallbacks() {
	// already resolved
	if p.finished {
		return
	}
	for _, s := range p.callbacks {
		s(p.err)
	}
	// mark as finished
	p.finished = true
}

// Then chains a callback to the Promise
func (p *Promise) Then(s func(err error)) *Promise {
	p.Lock()
	defer p.Unlock()

	// promise already run, call the callback immediately
	if p.finished {
		s(p.err)
		// append it to the subscribers otherwise
	} else {
		p.callbacks = append(p.callbacks, s)
	}
	return p
}

// Finish finishes the promise by executing all callbacks and saving the message/error for late subscribers
func (p *Promise) Finish(err error) *Promise {
	p.Lock()
	defer p.Unlock()

	p.err = err

	p.executeCallbacks()
	return p
}
