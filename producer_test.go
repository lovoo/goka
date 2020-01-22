// +build goka

package goka

import (
	"log"
	"sync/atomic"
	"testing"
	"time"
)

// This tests how a producer behaves when being shutdown to make sure,
// no promises stay unfinished.
// To run the test, get a local kafka-container running (e.g. go to
// examples-directory and do `make restart`), then run the tests with
// `go test -v github.com/lovoo/goka/kafka/ -tags=kafka`
func TestProducerError(t *testing.T) {
	cfg := DefaultConfig()
	p, err := NewProducer([]string{"localhost:9092"}, cfg)

	if err != nil {
		t.Fatalf("error creating producer: %v", err)
	}

	var (
		promises     []*Promise
		donePromises int64
		emitted      int64
		done         = make(chan bool)
	)

	go func() {
		defer func() {
			recover()
		}()
		defer close(done)
		for {
			promise := p.Emit("test", "test", []byte{})
			promise.Then(func(err error) {
				atomic.AddInt64(&donePromises, 1)
				if err != nil {
					log.Printf("error producing message: %v", err)
				}
			})
			promises = append(promises, promise)
			emitted++
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// let it run for 1 second
	time.Sleep(1000 * time.Millisecond)

	// close the producer
	err = p.Close()
	if err != nil {
		log.Printf("Error closing producer: %v", err)
	}
	// wait for the promises to be
	<-done

	if len(promises) != int(emitted) {
		t.Errorf("Promises/Emits do not match: promises: %d, emitted. %d", len(promises), emitted)
	}
	if len(promises) != int(donePromises) {
		t.Errorf("Promises/Done promises do not match: promises: %d, done. %d", len(promises), donePromises)
	}
}
