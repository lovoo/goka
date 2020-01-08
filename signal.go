package goka

import (
	"fmt"
	"sync"
)

// State types a state of the Signal
type State int

type waiter struct {
	done     chan struct{}
	state    State
	minState bool
}

// Signal allows synchronization on a state, waiting for that state and checking
// the current state
type Signal struct {
	m             sync.Mutex
	state         State
	waiters       []*waiter
	allowedStates map[State]bool
}

// NewSignal creates a new Signal based on the states
func NewSignal(states ...State) *Signal {
	s := &Signal{
		allowedStates: make(map[State]bool),
	}
	for _, state := range states {
		s.allowedStates[state] = true
	}

	return s
}

// SetState changes the state of the signal
// and notifies all goroutines waiting for the new state
func (s *Signal) SetState(state State) *Signal {
	s.m.Lock()
	defer s.m.Unlock()
	if !s.allowedStates[state] {
		panic(fmt.Errorf("trying to set illegal state %v", state))
	}

	// set the state and notify all channels waiting for it.
	s.state = state
	var newWaiters []*waiter
	for _, w := range s.waiters {
		if w.state == state || (w.minState && state >= w.state) {
			close(w.done)
			continue
		}
		newWaiters = append(newWaiters, w)
	}
	s.waiters = newWaiters

	return s
}

// IsState returns if the signal is in the requested state
func (s *Signal) IsState(state State) bool {
	return s.state == state
}

// State returns the current state
func (s *Signal) State() State {
	return s.state
}

func (s *Signal) WaitForStateMin(state State) chan struct{} {
	s.m.Lock()
	defer s.m.Unlock()

	w := &waiter{
		done:     make(chan struct{}),
		state:    state,
		minState: true,
	}

	return s.waitForWaiter(state, w)
}

// WaitForState returns a channel that closes when the signal reaches passed
// state.
func (s *Signal) WaitForState(state State) chan struct{} {
	s.m.Lock()
	defer s.m.Unlock()

	w := &waiter{
		done:  make(chan struct{}),
		state: state,
	}

	return s.waitForWaiter(state, w)
}

func (s *Signal) waitForWaiter(state State, w *waiter) chan struct{} {

	// if the signal is currently in that state (or in a higher state if minState is set)
	// then close the waiter immediately
	if curState := s.State(); state == curState || (w.minState && curState >= state) {
		close(w.done)
	} else {
		s.waiters = append(s.waiters, w)
	}

	return w.done
}
