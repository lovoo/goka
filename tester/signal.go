package tester

import (
	"fmt"
	"sync"
)

// State types a state of the Signal
type State int

// Signal allows synchronization on a state, waiting for that state and checking
// the current state
type Signal struct {
	sync.Mutex
	state         State
	waitChans     map[State][]chan struct{}
	allowedStates map[State]bool
}

// NewSignal creates a new Signal based on the states
func NewSignal(states ...State) *Signal {
	s := &Signal{
		waitChans:     make(map[State][]chan struct{}),
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
	s.Lock()
	defer s.Unlock()
	if !s.allowedStates[state] {
		panic(fmt.Errorf("trying to set illegal state %v", state))
	}

	// set the state and notify all channels waiting for it.
	s.state = state
	for _, waitChan := range s.waitChans[state] {
		close(waitChan)
	}
	delete(s.waitChans, state)

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

// WaitForState returns a channel that closes when the signal reaches passed
// state.
func (s *Signal) WaitForState(state State) chan struct{} {
	s.Lock()
	defer s.Unlock()
	cb := make(chan struct{})

	if s.IsState(state) {
		close(cb)
	} else {
		s.waitChans[state] = append(s.waitChans[state], cb)
	}

	return cb
}
