package goka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/facebookgo/ensure"
)

type mockRunnable struct {
	ch chan error
}

func newMockRunnable() *mockRunnable {
	return &mockRunnable{
		ch: make(chan error),
	}
}

func (r *mockRunnable) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-r.ch:
		return err
	}
}

func TestRunset_Start(t *testing.T) {
	p := newMockRunnable()
	rs := Start(p)
	rs.Stop()
	err := doTimed(t, func() {
		err := rs.Wait()
		ensure.Nil(t, err)
	})
	ensure.Nil(t, err)

	// should not return
	p = newMockRunnable()
	rs = Start(p)
	err = doTimed(nil, func() {
		_ = rs.Wait()
	})
	ensure.NotNil(t, err)
	rs.Stop()

	// should return if terminates without error
	p = newMockRunnable()
	rs = Start(p)
	close(p.ch)
	err = doTimed(nil, func() {
		err := rs.Wait()
		ensure.Nil(t, err)
	})
	ensure.Nil(t, err)

	// should return if terminates with error
	p = newMockRunnable()
	rs = Start(p)
	p.ch <- errors.New("some error")
	err = doTimed(nil, func() {
		err := rs.Wait()
		ensure.NotNil(t, err)
	})
	ensure.Nil(t, err)

	// should return
	p = newMockRunnable()
	rs = Start(p)
	p.ch <- errors.New("some error")
	err = doTimed(nil, func() {
		<-rs.Done()
	})
	ensure.Nil(t, err)

}

// Example shows how to control the lifecycle of runnables (processors or
// views) using Runsets.
func ExampleRunset() {
	var (
		brokers        = []string{"127.0.0.1:9092"}
		group   Group  = "group"
		topic   Stream = "topic"
	)

	f := func(ctx Context, m interface{}) {
		fmt.Printf("Hello world: %v", m)
	}

	p, err := NewProcessor(brokers, DefineGroup(group, Input(topic, rawCodec, f)))
	if err != nil {
		log.Fatalln(err)
	}

	// start processor creating a Runset.
	rs := Start(p)

	// wait for bad things to happen
	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-rs.Done():
	case <-wait: // wait for SIGINT/SIGTERM
		rs.Stop() // gracefully stop processor
	}
	if err := rs.Wait(); err != nil {
		log.Fatalln(err)
	}
}
