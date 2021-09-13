package actions

import (
	"context"
	"sync"
	"time"
)

type action struct {
	sync.RWMutex
	ctx      context.Context
	name     string
	actor    Actor
	cancel   context.CancelFunc
	done     chan struct{}
	started  time.Time
	finished time.Time
	runErr   error
}

func (a *action) Name() string {
	return a.name
}

// IsRunning returns whether the action is currently running.
func (a *action) IsRunning() bool {
	a.RLock()
	defer a.RUnlock()

	// if there's a "done"-channel and it's not closed, the action is running
	if a.done != nil {
		select {
		case <-a.done:
			return false
		default:
			return true
		}
	}
	return false
}

func (a *action) Description() string {
	return a.actor.Description()
}

// StartTime returns a rfc3339 format of the start time or "not started" if it was not started yet
func (a *action) StartTime() string {
	a.RLock()
	defer a.RUnlock()
	if a.started.IsZero() {
		return "not started"
	}
	return a.started.Format(time.RFC3339)
}

// FinishedTime returns a rfc3339 format of the finish time or "not finished" if it was not finished yet.
func (a *action) FinishedTime() string {
	a.RLock()
	defer a.RUnlock()
	if a.finished.IsZero() {
		return "not finished"
	}
	return a.finished.Format(time.RFC3339)
}

// Error returns the error of the last invocation
func (a *action) Error() error {

	a.RLock()
	defer a.RUnlock()
	return a.runErr
}

// Start starts the action in a separate goroutine.
// If Start is called while the action is running, `Stop` will be called first.
func (a *action) Start(value string) {
	// stop the action (noop if it's not running)
	a.Stop()

	a.Lock()
	defer a.Unlock()

	a.started = time.Now()
	a.finished = time.Time{}
	ctx, cancel := context.WithCancel(context.Background())
	a.ctx, a.cancel = ctx, cancel
	done := make(chan struct{})
	a.done = done
	go func() {
		defer cancel()
		err := a.actor.RunAction(ctx, value)
		defer close(done)

		a.Lock()
		a.finished = time.Now()
		a.runErr = err
		a.Unlock()

	}()
}

// Stop stops the action. It waits for the action to be completed
func (a *action) Stop() {
	a.RLock()
	done := a.done
	if a.cancel != nil {
		a.cancel()
	}
	a.RUnlock()

	// if there was something to wait on, let's wait.
	if done != nil {
		<-done
	}
}
