package goka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
)

// ViewState represents the state of the view
type ViewState int

const (
	// ViewStateIdle  - the view is not started yet
	ViewStateIdle ViewState = iota
	// ViewStateInitializing - the view (i.e. at least one partition) is initializing
	ViewStateInitializing
	// ViewStateConnecting - the view (i.e. at least one partition) is (re-)connecting
	ViewStateConnecting
	// ViewStateCatchUp - the view (i.e. at least one partition) is still catching up
	ViewStateCatchUp
	// ViewStateRunning - the view (i.e. all partitions) has caught up and is running
	ViewStateRunning
)

func newViewSignal() *Signal {
	return NewSignal(State(ViewStateIdle),
		State(ViewStateInitializing),
		State(ViewStateConnecting),
		State(ViewStateCatchUp),
		State(ViewStateRunning)).SetState(State(ViewStateIdle))
}

// Getter functions return a value for a key or an error. If no value exists for the key, nil is returned without errors.
type Getter func(string) (interface{}, error)

// View is a materialized (i.e. persistent) cache of a group table.
type View struct {
	brokers    []string
	topic      string
	opts       *voptions
	log        logger
	partitions []*PartitionTable
	consumer   sarama.Consumer
	tmgr       TopicManager
	state      *Signal
}

// NewView creates a new View object from a group.
func NewView(brokers []string, topic Table, codec Codec, options ...ViewOption) (*View, error) {
	options = append(
		// default options comes first
		[]ViewOption{
			WithViewClientID(fmt.Sprintf("goka-view-%s", topic)),
			WithViewCallback(DefaultUpdate),
			WithViewStorageBuilder(storage.DefaultBuilder(DefaultViewStoragePath())),
		},

		// then the user passed options
		options...,
	)

	opts := new(voptions)
	err := opts.applyOptions(topic, codec, options...)
	if err != nil {
		return nil, fmt.Errorf("Error applying user-defined options: %v", err)
	}

	consumer, err := opts.builders.consumerSarama(brokers, opts.clientID)
	if err != nil {
		return nil, fmt.Errorf("Error creating sarama consumer for brokers %+v: %v", brokers, err)
	}
	opts.tableCodec = codec

	tmgr, err := opts.builders.topicmgr(brokers)
	if err != nil {
		return nil, fmt.Errorf("Error creating topic manager: %v", err)
	}

	v := &View{
		brokers:  brokers,
		topic:    string(topic),
		opts:     opts,
		log:      opts.log.Prefix(fmt.Sprintf("View %s", topic)),
		consumer: consumer,
		tmgr:     tmgr,
		state:    newViewSignal(),
	}

	if err = v.createPartitions(brokers); err != nil {
		return nil, err
	}

	return v, err
}

// WaitRunning returns a channel that will be closed when the view enters the running state
func (v *View) WaitRunning() <-chan struct{} {
	return v.state.WaitForState(State(ViewStateRunning))
}

func (v *View) createPartitions(brokers []string) (rerr error) {
	tm, err := v.opts.builders.topicmgr(brokers)
	if err != nil {
		return fmt.Errorf("Error creating topic manager: %v", err)
	}
	defer func() {
		e := tm.Close()
		if e != nil && rerr == nil {
			rerr = fmt.Errorf("Error closing topic manager: %v", e)
		}
	}()

	partitions, err := tm.Partitions(v.topic)
	if err != nil {
		return fmt.Errorf("Error getting partitions for topic %s: %v", v.topic, err)
	}

	// check assumption that partitions are gap-less
	for i, p := range partitions {
		if i != int(p) {
			return fmt.Errorf("Partition numbers are not sequential for topic %s", v.topic)
		}
	}

	for partID, p := range partitions {
		backoff, err := v.opts.builders.backoff()
		if err != nil {
			return fmt.Errorf("Error creating backoff: %v", err)
		}
		v.partitions = append(v.partitions, newPartitionTable(v.topic,
			p,
			v.consumer,
			v.tmgr,
			v.opts.updateCallback,
			v.opts.builders.storage,
			v.log.Prefix(fmt.Sprintf("PartTable-%d", partID)),
			backoff,
			v.opts.backoffResetTime,
		))
	}

	return nil
}

func (v *View) runStateMerger(ctx context.Context) {

	var (
		states = make(map[int]PartitionStatus)
		m      sync.Mutex
	)

	// internal callback that will be called when the state of any
	// partition changes.
	// Then the "lowest" state of all partitions will be selected and
	// translated into the respective ViewState
	updateViewState := func(idx int, state State) {
		m.Lock()
		defer m.Unlock()
		states[idx] = PartitionStatus(state)

		var lowestState = PartitionStatus(-1)

		for _, partitionState := range states {
			if lowestState == -1 || partitionState < lowestState {
				lowestState = partitionState
			}
		}
		var newState = ViewState(-1)
		switch lowestState {
		case PartitionStopped:
			newState = ViewStateIdle
		case PartitionInitializing:
			newState = ViewStateInitializing
		case PartitionConnecting:
			newState = ViewStateConnecting
		case PartitionRecovering:
			newState = ViewStateCatchUp
		case PartitionPreparing:
			newState = ViewStateCatchUp
		case PartitionRunning:
			newState = ViewStateRunning
		default:
			v.log.Printf("State merger received unknown partition state: %v", lowestState)
		}

		if newState != -1 {
			v.state.SetState(State(newState))
		}
	}

	// get a state change observer for all partitions
	for idx, partition := range v.partitions {
		idx := idx
		partition := partition

		observer := partition.observeStateChanges()
		// create a goroutine that updates the view state based all partition states
		go func() {
			for {
				select {
				case newState, ok := <-observer.C():
					if !ok {
						return
					}
					// something has changed, so update the state
					updateViewState(idx, newState)
				case <-ctx.Done():
					observer.Stop()
					return
				}
			}
		}()
	}
}

// Run starts consuming the view's topic and saving updates in the local persistent cache.
//
// The view will shutdown in case of errors or when the context is closed.
// It can be initialized with autoreconnect
//  view := NewView(..., WithViewAutoReconnect())
// which makes the view internally reconnect in case of errors.
// Then it will only stop by canceling the context (see example).
func (v *View) Run(ctx context.Context) (rerr error) {
	v.log.Debugf("starting")
	defer v.log.Debugf("stopped")

	// update the view state asynchronously by observing
	// the partition's state and translating that to the view
	v.runStateMerger(ctx)
	defer v.state.SetState(State(ViewStateIdle))

	// close the view after running
	defer func() {
		rerr = multierror.Append(
			rerr,
			v.close(),
		).ErrorOrNil()
	}()

	recoverErrg, recoverCtx := multierr.NewErrGroup(ctx)

	for _, partition := range v.partitions {
		partition := partition
		go partition.RunStatsLoop(ctx)
		recoverErrg.Go(func() error {
			return partition.SetupAndRecover(recoverCtx, v.opts.autoreconnect)
		})
	}

	err := recoverErrg.Wait().ErrorOrNil()
	if err != nil {
		rerr = fmt.Errorf("Error recovering partitions for view %s: %v", v.Topic(), err)
		return
	}

	select {
	case <-ctx.Done():
		return nil
	default:
	}

	catchupErrg, catchupCtx := multierr.NewErrGroup(ctx)

	for _, partition := range v.partitions {
		partition := partition
		catchupErrg.Go(func() error {
			return partition.CatchupForever(catchupCtx, v.opts.autoreconnect)
		})
	}

	err = catchupErrg.Wait().ErrorOrNil()
	if err != nil {
		rerr = fmt.Errorf("Error catching up partitions for view %s: %v", v.Topic(), err)
	}
	return
}

// close closes all storage partitions
func (v *View) close() error {
	errg, _ := multierr.NewErrGroup(context.Background())
	for _, p := range v.partitions {
		p := p
		errg.Go(func() error {
			return p.Close()
		})
	}
	v.partitions = nil
	return errg.Wait().ErrorOrNil()
}

func (v *View) hash(key string) (int32, error) {
	// create a new hasher every time. Alternative would be to store the hash in
	// view and every time reset the hasher (ie, hasher.Reset()). But that would
	// also require us to protect the access of the hasher with a mutex.
	hasher := v.opts.hasher()

	_, err := hasher.Write([]byte(key))
	if err != nil {
		return -1, err
	}
	hash := int32(hasher.Sum32())
	if hash < 0 {
		hash = -hash
	}
	if len(v.partitions) == 0 {
		return 0, errors.New("no partitions found")
	}
	return hash % int32(len(v.partitions)), nil
}

func (v *View) find(key string) (*PartitionTable, error) {
	h, err := v.hash(key)
	if err != nil {
		return nil, err
	}
	return v.partitions[h], nil
}

// Topic returns  the view's topic
func (v *View) Topic() string {
	return v.topic
}

// Get returns the value for the key in the view, if exists. Nil if it doesn't.
// Get can be called by multiple goroutines concurrently.
// Get can only be called after Recovered returns true.
func (v *View) Get(key string) (interface{}, error) {

	if v.state.IsState(State(ViewStateIdle)) || v.state.IsState(State(ViewStateInitializing)) {
		return nil, fmt.Errorf("View is either not running, not correctly initialized or stopped again. It's not safe to retrieve values")
	}

	// find partition where key is located
	partTable, err := v.find(key)
	if err != nil {
		return nil, err
	}

	// get key and return
	data, err := partTable.Get(key)
	if err != nil {
		return nil, fmt.Errorf("error getting value (key %s): %v", key, err)
	} else if data == nil {
		return nil, nil
	}

	// decode value
	value, err := v.opts.tableCodec.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("error decoding value (key %s): %v", key, err)
	}

	// if the key does not exist the return value is nil
	return value, nil
}

// Has checks whether a value for passed key exists in the view.
func (v *View) Has(key string) (bool, error) {
	// find partition where key is located
	partTable, err := v.find(key)
	if err != nil {
		return false, err
	}

	return partTable.Has(key)
}

// Iterator returns an iterator that iterates over the state of the View.
func (v *View) Iterator() (Iterator, error) {
	iters := make([]storage.Iterator, 0, len(v.partitions))
	for i := range v.partitions {
		iter, err := v.partitions[i].Iterator()
		if err != nil {
			// release already opened iterators
			for i := range iters {
				iters[i].Release()
			}

			return nil, fmt.Errorf("error opening partition iterator: %v", err)
		}

		iters = append(iters, iter)
	}

	return &iterator{
		iter:  storage.NewMultiIterator(iters),
		codec: v.opts.tableCodec,
	}, nil
}

// IteratorWithRange returns an iterator that iterates over the state of the View. This iterator is build using the range.
func (v *View) IteratorWithRange(start, limit string) (Iterator, error) {
	iters := make([]storage.Iterator, 0, len(v.partitions))
	for i := range v.partitions {
		iter, err := v.partitions[i].IteratorWithRange([]byte(start), []byte(limit))
		if err != nil {
			// release already opened iterators
			for i := range iters {
				iters[i].Release()
			}

			return nil, fmt.Errorf("error opening partition iterator: %v", err)
		}

		iters = append(iters, iter)
	}

	return &iterator{
		iter:  storage.NewMultiIterator(iters),
		codec: v.opts.tableCodec,
	}, nil
}

// Evict removes the given key only from the local cache. In order to delete a
// key from Kafka and other Views, context.Delete should be used on a Processor.
func (v *View) Evict(key string) error {
	s, err := v.find(key)
	if err != nil {
		return err
	}

	return s.Delete(key)
}

// Recovered returns true when the view has caught up with events from kafka.
func (v *View) Recovered() bool {
	// no partitions --> never recover
	// Otherwise we might mask errors of initializing the view
	if len(v.partitions) == 0 {
		return false
	}

	for _, p := range v.partitions {
		if !p.IsRecovered() {
			return false
		}
	}

	return true
}

// CurrentState returns the current ViewState of the view
// This is useful for polling e.g. when implementing health checks or metrics
func (v *View) CurrentState() ViewState {
	return ViewState(v.state.State())
}

// ObserveStateChanges returns a StateChangeObserver that allows to handle state changes of the view
// by reading from a channel.
// It is crucial to continuously read from that channel, otherwise the View might deadlock upon
// state changes.
// If the observer is not needed, the caller must call observer.Stop()
//
// Example
//
//  view := goka.NewView(...)
//  go view.Run(ctx)
//
//  go func(){
//    obs := view.ObserveStateChanges()
//    defer obs.Stop()
//    for {
//      select{
//        case state, ok := <-obs.C:
//          // handle state (or closed channel)
//        case <-ctx.Done():
//      }
//    }
//  }()
func (v *View) ObserveStateChanges() *StateChangeObserver {
	return v.state.ObserveStateChange()
}

// Stats returns a set of performance metrics of the view.
func (v *View) Stats(ctx context.Context) *ViewStats {
	return v.statsWithContext(ctx)
}

func (v *View) statsWithContext(ctx context.Context) *ViewStats {
	var (
		m     sync.Mutex
		stats = newViewStats()
	)
	errg, ctx := multierr.NewErrGroup(ctx)

	for _, partTable := range v.partitions {
		partTable := partTable

		errg.Go(func() error {
			tableStats := partTable.fetchStats(ctx)
			m.Lock()
			defer m.Unlock()

			if tableStats != nil {
				stats.Partitions[partTable.partition] = tableStats
			}
			return nil
		})
	}

	err := errg.Wait().ErrorOrNil()
	if err != nil {
		v.log.Printf("Error retrieving stats: %v", err)
	}
	return stats
}
