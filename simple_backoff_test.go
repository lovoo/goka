package goka

import (
	"testing"
	"time"

	"github.com/lovoo/goka/internal/test"
)

func TestSimpleBackoff(t *testing.T) {
	t.Run("simple progression", func(t *testing.T) {
		backoff := NewSimpleBackoff(time.Second)
		for i := 0; i < 10; i++ {
			test.AssertEqual(t, time.Duration(i+1)*time.Second, backoff.Duration())
		}
	})
	t.Run("reset", func(t *testing.T) {
		backoff := NewSimpleBackoff(time.Second)
		for i := 0; i < 10; i++ {
			if i%5 == 0 {
				backoff.Reset()
			}
			test.AssertEqual(t, time.Duration(i%5+1)*time.Second, backoff.Duration())
		}
	})
}
