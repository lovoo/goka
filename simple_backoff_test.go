package goka

import (
	"testing"
	"time"

	"github.com/lovoo/goka/internal/test"
)

func TestSimpleBackoff(t *testing.T) {
	t.Run("simple progression", func(t *testing.T) {
		backoff := NewSimpleBackoff(time.Second, 10*time.Second)
		for i := 0; i < 10; i++ {
			test.AssertEqual(t, time.Duration(i)*time.Second, backoff.Duration())
		}

		// it doesn't go higher than the max
		test.AssertEqual(t, 10*time.Second, backoff.Duration())
		test.AssertEqual(t, 10*time.Second, backoff.Duration())
	})
	t.Run("reset", func(t *testing.T) {
		backoff := NewSimpleBackoff(time.Second, 10*time.Second)

		test.AssertEqual(t, backoff.Duration(), time.Duration(0))
		backoff.Duration()
		test.AssertTrue(t, backoff.Duration() != 0)
		backoff.Reset()
		test.AssertEqual(t, backoff.Duration(), time.Duration(0))
	})
}
