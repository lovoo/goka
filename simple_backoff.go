package goka

import (
	"sync"
	"time"
)

// NewSimpleBackoff returns a simple backoff waiting the
// specified duration longer each iteration until reset.
func NewSimpleBackoff(step time.Duration, max time.Duration) Backoff {
	return &simpleBackoff{
		step: step,
		max:  max,
	}
}

type simpleBackoff struct {
	sync.Mutex

	current time.Duration
	step    time.Duration
	max     time.Duration
}

func (b *simpleBackoff) Reset() {
	b.Lock()
	defer b.Unlock()
	b.current = time.Duration(0)
}

func (b *simpleBackoff) Duration() time.Duration {
	b.Lock()
	defer b.Unlock()
	value := b.current

	if (b.current + b.step) <= b.max {
		b.current += b.step
	}
	return value
}
