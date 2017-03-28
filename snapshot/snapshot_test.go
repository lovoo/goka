package snapshot

import (
	"encoding/json"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestKeys(t *testing.T) {
	s := New(1)

	kv := []string{
		"key-1",
		"key-2",
	}
	for _, k := range kv {
		s.Set(k, nil, false, nil, nil)
	}

	ensure.SameElements(t, s.Keys(), kv)
}

func TestSnap(t *testing.T) {
	s := New(1)
	var flushed int
	var evicted int
	s.Set("test", 3, false, func(key string, value interface{}, encoded bool) {
		flushed = value.(int)
	},
		func(key string, value interface{}, encoded bool) {
			evicted = value.(int)
		})

	// the check needs to be executed repeatedly until it finally blocks,
	// unless the check might be earlier than the Flush which takes some time to flush
	go func() {
		for {
			s.Check()
		}
	}()

	// flush runs synchronously
	s.Flush(func() error { return nil })

	ensure.True(t, flushed == 3 && evicted == 3)
}

func TestSnap_get_cloned(t *testing.T) {
	var (
		err   error
		value interface{}
	)
	s := New(1)

	initialValue := 42

	// cloning function that simply copies an int value to a new value
	clone := func(key string, value interface{}, encoded bool) (interface{}, error) {
		cloned := *value.(*int)
		return &cloned, nil
	}

	// try to retrieve it, it doesn't exist
	value, _, err = s.Get("key", clone)
	ensure.Nil(t, err)
	ensure.True(t, value == nil)

	ensure.False(t, s.Has("key"))

	// set it, using dummy evict/flush methods
	s.Set("key", &initialValue, false, func(key string, value interface{}, encoded bool) {}, func(key string, value interface{}, encoded bool) {})

	ensure.True(t, s.Has("key"))

	// trying to get it again will retrieve it (not cloned - it's in the current snapshot)
	value, _, err = s.Get("key", clone)
	ensure.Nil(t, err)
	ensure.True(t, initialValue == *value.(*int))
	// changing it will change also the snapshot-value
	initialValue = 43
	ensure.True(t, initialValue == *value.(*int))

	// now flush and evict everything, so cloning the values is necessary
	go func() {
		for {
			s.Check()
		}
	}()
	s.Flush(func() error { return nil })

	// now checking using the cloning
	value, _, err = s.Get("key", clone)
	ensure.Nil(t, err)
	ensure.True(t, initialValue == *value.(*int))
	// changing it will change also the snapshot-value
	initialValue = 44
	// the snapshotted-value is still 43
	ensure.True(t, 43 == *value.(*int))
}

// Checks that a concurrent call to flush causes a panic.

func TestSnap_doubleFlush(t *testing.T) {
	s := New(0)

	// We simulate a very long running flush on one of the keys (the only one) to make sure
	// the flush takes considerate time
	wait := make(chan bool)
	done := make(chan bool)
	waiting := func(key string, value interface{}, encoded bool) {
		close(wait)
		<-done
	}
	s.Set("key", 42, false, waiting, waiting)

	// catch the panic, and fail if there was no panic
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic, but nothing happened")
			t.FailNow()
		}
	}()

	// start the first flush, now we have 100ms time to start the second
	go s.Flush(func() error { return nil })
	// to avoid having the panic in the go-routine (which will then not be recovered here),
	// let's sleep a bit
	<-wait
	// start flush a second time, this time it must panic (to be recovered)
	s.Flush(func() error { return nil })
	close(done)
}

func TestSnap_setEncoded(t *testing.T) {
	s := New(0)

	value := struct {
		value int
	}{
		value: 42,
	}

	valueEncoded, err := json.Marshal(value)
	ensure.Nil(t, err)

	s.Set("key", valueEncoded, true, nil, nil)

	returnedValue, isEncoded, err := s.Get("key", func(key string, value interface{}, encoded bool) (interface{}, error) { return value, nil })
	ensure.True(t, isEncoded)
	ensure.NotNil(t, returnedValue)
	ensure.Nil(t, err)
}
