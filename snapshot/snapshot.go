package snapshot

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/db7/barrier"
)

type flusher func(key string, value interface{}, encoded bool)
type evicter func(key string, value interface{}, encoded bool)
type cloner func(key string, value interface{}, encoded bool) (interface{}, error)

type entry struct {
	value   interface{}
	encoded bool
	flush   flusher
	evict   evicter
}

type MetricsHook func(numUpdates int64, numElements int)

type entriesSnapshot struct {
	entries       map[string]entry
	updateCounter int64
}
type Snapshot struct {
	current    *entriesSnapshot
	stable     *entriesSnapshot
	m          sync.RWMutex
	b          *barrier.Barrier
	rendezvous int64 // active or inactive

	// safety flag ensuring the flush is never executed twice.
	// 1 => it is flushing
	// 0 => it is not flushing
	isFlushing int64

	MetricsHook MetricsHook
}

func newEntriesSnapshot() *entriesSnapshot {
	return &entriesSnapshot{
		entries: make(map[string]entry),
	}
}

// New create a new snapshot with a barrier for n external goroutines plus
// internal go routines.
func New(n int) *Snapshot {
	return &Snapshot{
		current: newEntriesSnapshot(),
		stable:  newEntriesSnapshot(),
		b:       barrier.New(1 + n),
	}
}

const (
	inactive = 0
	active   = 1
)

func (s *Snapshot) Has(key string) bool {
	_, cok := s.current.entries[key]
	_, sok := s.stable.entries[key]

	return cok || sok
}

func (s *Snapshot) Keys() []string {
	compacted := make(map[string]interface{})

	for _, m := range []map[string]entry{s.current.entries, s.stable.entries} {
		for k := range m {
			compacted[k] = nil
		}
	}

	keys := make([]string, 0, len(compacted))
	for k := range compacted {
		keys = append(keys, k)
	}

	return keys
}

// Set adds a new key/value pair to the Snapshot, defining how the value will
// be evicted and flushed, on appropriate times.
// if the value already exists, the evicter/flusher are being overwritten.
func (s *Snapshot) Set(key string, value interface{}, encoded bool, flush flusher, evict evicter) {
	s.m.Lock()
	defer s.m.Unlock()
	s.current.entries[key] = entry{value: value, encoded: encoded, flush: flush, evict: evict}
	s.current.updateCounter++
}

// Get returns a value from the Snapshot identified by passed key.
// It will search in the current Snapshot and returns its value directly,git
// otherwise check in the last Snapshot (stable) and return a cloned value.
func (s *Snapshot) Get(key string, clone cloner) (interface{}, bool, error) {
	s.m.RLock()
	//
	if e, has := s.current.entries[key]; has {
		s.m.RUnlock()
		return e.value, e.encoded, nil
	}
	s.m.RUnlock()

	if e, has := s.stable.entries[key]; has && e.value != nil {
		s.m.Lock()
		defer s.m.Unlock()
		value, err := clone(key, e.value, e.encoded)
		if err != nil {
			return nil, false, fmt.Errorf("Error cloning the value from stable Snapshot for key %s: %v", key, err)
		}
		s.current.entries[key] = entry{value, e.encoded, nil, nil}
		return value, e.encoded, nil
	}
	return nil, false, nil
}

// Flush swaps the current Snapshot to stable. To avoid the user-goroutines changing the states
// at the same time, it synchronizes using a barrier. After swap, it flushes all stable entries.
// This function must never be executed concurrently, otherwise it will panic.
func (s *Snapshot) Flush(flushDone func() error) error {
	if !atomic.CompareAndSwapInt64(&s.isFlushing, 0, 1) {
		panic("Detected concurrent invocation of stateManager.Flush. This must never happen!")
	}
	// disable it again.
	defer atomic.StoreInt64(&s.isFlushing, 0)

	atomic.StoreInt64(&s.rendezvous, active)
	err := s.b.Await(s.swap)
	if err != nil {
		return fmt.Errorf("Error writing the Snapshot: %v", err)
	}

	// copy the old "stable" Snapshot asynchronously
	for k, entry := range s.stable.entries {
		if entry.flush != nil {
			entry.flush(k, entry.value, entry.encoded)
		}
	}

	if s.MetricsHook != nil {
		s.MetricsHook(s.stable.updateCounter, len(s.stable.entries))
	}
	if flushDone != nil {
		return flushDone()
	}
	return nil
}

func (s *Snapshot) swap() error {
	s.stable = s.current
	s.current = newEntriesSnapshot()
	for key, elem := range s.stable.entries {
		if elem.evict == nil {
			continue
		}
		elem.evict(key, elem.value, elem.encoded)
	}

	atomic.StoreInt64(&s.rendezvous, inactive)
	return nil
}

// Check is called by all clients
func (s *Snapshot) Check() error {
	if atomic.LoadInt64(&s.rendezvous) == active {
		err := s.b.Await(s.swap)
		if err != nil {
			return fmt.Errorf("Error writing the Snapshot: %v", err)
		}
	}
	return nil
}

// Cancel the Snapshot from Flushing/Checking of the Snapshot. Mostly for shutdown.
func (s *Snapshot) Cancel() {
	s.b.Abort()
}
