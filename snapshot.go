package goka

import (
	"context"

	"github.com/lovoo/goka/logger"
)

type SnapshotProvider struct {
	processorState *Signal
	tableIterator  Iterator
	log            logger.Logger
}

func NewSnapshotProvider(it Iterator, stateChangeChan *Signal, log logger.Logger) *SnapshotProvider {
	return &SnapshotProvider{
		log:            log,
		processorState: stateChangeChan,
		tableIterator:  it,
	}
}

func (sp *SnapshotProvider) Items(ctx context.Context) <-chan *Item {
	// make a channel to forward all the table items to the receiver
	c := make(chan *Item)

	// fill the channel in a separate go routine
	go func() {
		// close the items-channel in the end
		defer close(c)
		defer sp.tableIterator.Release()

		// observe the processor state to check if the processor is still running
		stateObserver := sp.processorState.ObserveStateChange()

		// close the stateObserver once the iteration is finished
		defer func() {
			if _, ok := <-stateObserver.closed; ok {
				stateObserver.Stop()
			}
		}()

		for {
			// read the next table item
			item, ok, err := sp.getNextItem()
			if err != nil {
				sp.log.Printf("failed to get next table value %v", err)
				return
			}
			if !ok {
				return
			}
			// Wait until one of the following channel operations can be executed:
			select {
			// stop, if the caller closes the context
			case <-ctx.Done():
				return
			// stop, if the processor state changes
			case <-stateObserver.C():
				return
			// push the item to the channel if the caller is ready to read the next item
			case c <- item:
			}
		}
	}()

	return c
}

func (sp *SnapshotProvider) getNextItem() (*Item, bool, error) {
	if !sp.tableIterator.Next() {
		return nil, false, nil
	}
	value, err := sp.tableIterator.Value()
	if err != nil {
		return nil, false, err
	}
	return &Item{
		key:   sp.tableIterator.Key(),
		value: value,
	}, true, nil
}

type Item struct {
	key   string
	value interface{}
}
