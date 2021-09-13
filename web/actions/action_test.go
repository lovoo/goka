package actions

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/lovoo/goka/internal/test"
)

var (
	actionRuntime = 10 * time.Millisecond
)

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

		test.AssertEqual(t, a.Name(), "test")
		test.AssertEqual(t, a.Description(), "sleep")
		test.AssertTrue(t, a.Error() == nil)
		test.AssertFalse(t, a.IsRunning())
		test.AssertEqual(t, a.StartTime(), "not started")
		test.AssertEqual(t, a.FinishedTime(), "not finished")
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
		test.AssertTrue(t, a.IsRunning())
		test.AssertNotEqual(t, a.StartTime(), "not started")
		test.AssertEqual(t, a.FinishedTime(), "not finished")

		a.Stop()
		test.AssertFalse(t, a.IsRunning())
		test.AssertNotEqual(t, a.StartTime(), "not started")
		// it's finished
		test.AssertNotEqual(t, a.FinishedTime(), "not finished")
		test.AssertTrue(t, a.Error() != nil)
		test.AssertEqual(t, a.Error().Error(), context.Canceled.Error())
		test.AssertEqual(t, run, int64(0))
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
		test.AssertFalse(t, a.IsRunning())
		test.AssertNotEqual(t, a.StartTime(), "not started")
		test.AssertNotEqual(t, a.FinishedTime(), "not finished")
		test.AssertTrue(t, a.Error() == nil)
		test.AssertEqual(t, run, int64(1))
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
		test.AssertFalse(t, a.IsRunning())
		test.AssertNotEqual(t, a.StartTime(), "not started")
		test.AssertNotEqual(t, a.FinishedTime(), "not finished")
		test.AssertTrue(t, a.Error() == nil)
		test.AssertEqual(t, run, int64(2))
	})

}
