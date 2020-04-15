package goka

import "time"

// NewSimpleBackoff returns a simple backoff waiting the
// specified duration longer each iteration until reset.
func NewSimpleBackoff(step time.Duration) Backoff {
	return &simpleBackoff{
		step: time.Second,
	}
}

type simpleBackoff struct {
	current time.Duration
	step    time.Duration
}

func (b *simpleBackoff) Reset() {
	b.current = time.Duration(0)
}

func (b *simpleBackoff) Duration() time.Duration {
	b.current += b.step
	return b.current
}
