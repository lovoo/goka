package goka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSimpleBackoff(t *testing.T) {
	t.Run("simple progression", func(t *testing.T) {
		backoff := NewSimpleBackoff(time.Second, 10*time.Second)
		for i := 0; i < 10; i++ {
			require.Equal(t, time.Duration(i)*time.Second, backoff.Duration())
		}

		// it doesn't go higher than the max
		require.Equal(t, 10*time.Second, backoff.Duration())
		require.Equal(t, 10*time.Second, backoff.Duration())
	})
	t.Run("reset", func(t *testing.T) {
		backoff := NewSimpleBackoff(time.Second, 10*time.Second)

		require.Equal(t, backoff.Duration(), time.Duration(0))
		backoff.Duration()
		require.True(t, backoff.Duration() != 0)
		backoff.Reset()
		require.Equal(t, backoff.Duration(), time.Duration(0))
	})
}
