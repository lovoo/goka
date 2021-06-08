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
	started  time.Time
	finished time.Time
	runErr   error
}

func (a *action) Name() string {
	return a.name
}

func (a *action) IsRunning() bool {
	a.RLock()
	defer a.RUnlock()
	return !a.started.IsZero() && a.finished.IsZero()
}

func (a *action) Description() string {
	return a.actor.Description()
}

func (a *action) StartTime() string {
	a.RLock()
	defer a.RUnlock()
	if a.started.IsZero() {
		return "not started"
	}
	return a.started.Format(time.RFC3339)
}
func (a *action) FinishedTime() string {
	a.RLock()
	defer a.RUnlock()
	if a.finished.IsZero() {
		return "not finished"
	}
	return a.finished.Format(time.RFC3339)
}

func (a *action) Error() error {

	a.RLock()
	defer a.RUnlock()
	return a.runErr
}

// Start starts the action in a separate goroutine.
// Note that stopping the action won't wait for it to be finished/shutdown,
// so starting it a second time might lead to overwriting the finish time if the first run
// stops and writes "finish" again
func (a *action) Start(value string) {
	a.Lock()
	defer a.Unlock()
	a.started = time.Now()
	a.finished = time.Time{}
	ctx, cancel := context.WithCancel(context.Background())
	a.ctx, a.cancel = ctx, cancel
	go func() {
		defer cancel()
		err := a.actor.RunAction(ctx, value)
		a.Lock()
		defer a.Unlock()
		a.finished = time.Now()
		a.runErr = err
	}()
}

func (a *action) Stop() {
	a.Lock()
	defer a.Unlock()
	if a.cancel != nil {
		a.cancel()
	}
}
