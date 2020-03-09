package goka

import (
	"sync"

	"github.com/Shopify/sarama"
)

// Promise as in https://en.wikipedia.org/wiki/Futures_and_promises
type Promise struct {
	sync.Mutex
	err      error
	msg      *sarama.ProducerMessage
	finished bool

	callbacks []func(msg *sarama.ProducerMessage, err error)
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
		s(p.msg, p.err)
	}
	// mark as finished
	p.finished = true
}

// Then chains a callback to the Promise
func (p *Promise) Then(callback func(err error)) *Promise {
	return p.ThenWithMessage(func(_ *sarama.ProducerMessage, err error) {
		callback(err)
	})
}

// ThenWithMessage chains a callback to the Promise
func (p *Promise) ThenWithMessage(callback func(msg *sarama.ProducerMessage, err error)) *Promise {
	p.Lock()
	defer p.Unlock()

	// promise already run, call the callback immediately
	if p.finished {
		callback(p.msg, p.err)
		// append it to the subscribers otherwise
	} else {
		p.callbacks = append(p.callbacks, callback)
	}
	return p
}

// Finish finishes the promise by executing all callbacks and saving the message/error for late subscribers
func (p *Promise) Finish(msg *sarama.ProducerMessage, err error) *Promise {
	p.Lock()
	defer p.Unlock()

	p.err = err
	p.msg = msg

	p.executeCallbacks()
	return p
}
