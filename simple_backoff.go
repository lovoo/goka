package goka

import "time"

// NewSimpleBackoff returns a simple backoff waiting the
// specified duration longer each iteration until reset.
func NewSimpleBackoff(step time.Duration, max time.Duration) Backoff {
	return &simpleBackoff{
		step: step,
		max:  max,
	}
}

type simpleBackoff struct {
	current time.Duration
	step    time.Duration
	max     time.Duration
}

func (b *simpleBackoff) Reset() {
	b.current = time.Duration(0)
}

func (b *simpleBackoff) Duration() time.Duration {
	value := b.current

	if (b.current + b.step) <= b.max {
		b.current += b.step
	}
	return value
}
