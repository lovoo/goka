package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

func main() {

	view, err := goka.NewView(
		// connect to example kafka cluster
		[]string{"localhost:9092"},
		// name does not matter, table will be empty
		"restartable-view-test-table",
		// codec doesn't matter, the table will be empty
		new(codec.String),
		// start the view autoconnecting
		goka.WithViewAutoReconnect(),
	)
	if err != nil {
		log.Fatalf("Cannot create view: %v", err)
	}
	// context we'll use to run the view and the state change observer
	ctx, cancel := context.WithCancel(context.Background())

	// channel used to wait for the view to finish
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := view.Run(ctx)
		if err != nil {
			log.Printf("View finished with error: %v", err)
		}
	}()

	// Get a state change observer and
	go func() {
		obs := view.ObserveStateChanges()
		defer obs.Stop()
		for {
			select {
			case state, ok := <-obs.C():
				if !ok {
					return
				}
				log.Printf("View is in state: %v", goka.ViewState(state))
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		waiter := make(chan os.Signal, 1)
		signal.Notify(waiter, syscall.SIGINT, syscall.SIGTERM)
		<-waiter
		cancel()
	}()

	<-done
}
