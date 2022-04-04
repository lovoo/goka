package actions

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var actionRuntime = 10 * time.Millisecond

func TestActionStart(t *testing.T) {
	var run int64
	actor := FuncActor("sleep", func(ctx context.Context, value string) error {
		select {
		case <-ctx.Done():
			log.Printf("ctx done")
			return ctx.Err()
		case <-time.After(actionRuntime):
			run++
		}
		return nil
	})

	t.Run("no-run", func(t *testing.T) {
		a := &action{
			name:  "test",
			actor: actor,
		}

		require.Equal(t, a.Name(), "test")
		require.Equal(t, a.Description(), "sleep")
		require.True(t, a.Error() == nil)
		require.False(t, a.IsRunning())
		require.Equal(t, a.StartTime(), "not started")
		require.Equal(t, a.FinishedTime(), "not finished")
	})

	t.Run("stop-only", func(t *testing.T) {
		a := &action{
			name:  "test",
			actor: actor,
		}

		a.Stop()
	})

	t.Run("start-stop", func(t *testing.T) {
		run = 0
		a := &action{
			name:  "test",
			actor: actor,
		}

		// start and check it's running
		a.Start("")
		require.True(t, a.IsRunning())
		require.NotEqual(t, a.StartTime(), "not started")
		require.Equal(t, a.FinishedTime(), "not finished")

		a.Stop()
		require.False(t, a.IsRunning())
		require.NotEqual(t, a.StartTime(), "not started")
		// it's finished
		require.NotEqual(t, a.FinishedTime(), "not finished")
		require.True(t, a.Error() != nil)
		require.Equal(t, a.Error().Error(), context.Canceled.Error())
		require.Equal(t, run, int64(0))
	})

	t.Run("start-finish", func(t *testing.T) {
		run = 0
		a := &action{
			name:  "test",
			actor: actor,
		}

		// start and check it's running
		a.Start("")
		time.Sleep(actionRuntime * 2)
		require.False(t, a.IsRunning())
		require.NotEqual(t, a.StartTime(), "not started")
		require.NotEqual(t, a.FinishedTime(), "not finished")
		require.True(t, a.Error() == nil)
		require.Equal(t, run, int64(1))
	})

	t.Run("start-restart-finish", func(t *testing.T) {
		run = 0
		a := &action{
			name:  "test",
			actor: actor,
		}

		// start + stop immediately
		a.Start("")
		a.Stop()

		// start and keep it running
		a.Start("")
		time.Sleep(actionRuntime * 2)

		a.Start("")
		time.Sleep(actionRuntime * 2)
		require.False(t, a.IsRunning())
		require.NotEqual(t, a.StartTime(), "not started")
		require.NotEqual(t, a.FinishedTime(), "not finished")
		require.True(t, a.Error() == nil)
		require.Equal(t, run, int64(2))
	})
}
