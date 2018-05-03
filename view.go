package goka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
)

// Getter functions return a value for a key or an error. If no value exists for the key, nil is returned without errors.
type Getter func(string) (interface{}, error)

// View is a materialized (i.e. persistent) cache of a group table.
type View struct {
	brokers    []string
	topic      string
	opts       *voptions
	partitions []*partition
	consumer   kafka.Consumer
	terminated bool
}

// NewView creates a new View object from a group.
func NewView(brokers []string, topic Table, codec Codec, options ...ViewOption) (*View, error) {
	options = append(
		// default options comes first
		[]ViewOption{
			WithViewLogger(logger.Default()),
			WithViewCallback(DefaultUpdate),
			WithViewPartitionChannelSize(defaultPartitionChannelSize),
			WithViewStorageBuilder(storage.DefaultBuilder(DefaultViewStoragePath())),
		},

		// then the user passed options
		options...,
	)

	// figure out how many partitions the group has
	opts := new(voptions)
	err := opts.applyOptions(topic, options...)
	if err != nil {
		return nil, fmt.Errorf("Error applying user-defined options: %v", err)
	}

	opts.tableCodec = codec

	v := &View{
		brokers: brokers,
		topic:   string(topic),
		opts:    opts,
	}

	if err = v.createPartitions(brokers); err != nil {
		return nil, err
	}

	return v, err
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

	v.opts.log.Printf("Table %s has %d partitions", v.topic, len(partitions))
	for _, p := range partitions {
		st, err := v.opts.builders.storage(v.topic, p)
		if err != nil {
			// TODO(diogo): gracefully terminate all partitions
			return fmt.Errorf("Error creating local storage for partition %d: %v", p, err)
		}

		po := newPartition(v.opts.log, v.topic, nil,
			&storageProxy{Storage: st, partition: p, update: v.opts.updateCallback},
			&proxy{p, nil},
			v.opts.partitionChannelSize,
		)
		v.partitions = append(v.partitions, po)
	}

	return nil
}

// reinit (re)initializes the view and its partitions to connect to Kafka
func (v *View) reinit() error {
	if v.terminated {
		return fmt.Errorf("view: cannot reinitialize terminated view")
	}

	consumer, err := v.opts.builders.consumer(v.brokers, "goka-view", v.opts.clientID)
	if err != nil {
		return fmt.Errorf("view: cannot create Kafka consumer: %v", err)
	}
	v.consumer = consumer

	for i, p := range v.partitions {
		p.reinit(&proxy{int32(i), v.consumer})
	}
	return nil
}

// Run starts consuming the view's topic.
func (v *View) Run(ctx context.Context) error {
	v.opts.log.Printf("view: starting")
	defer v.opts.log.Printf("view: stopped")

	if err := v.reinit(); err != nil {
		return err
	}

	errg, ctx := multierr.NewErrGroup(ctx)
	errg.Go(func() error { return v.run(ctx) })

	for id, p := range v.partitions {
		pid, par := int32(id), p
		errg.Go(func() error {
			v.opts.log.Printf("view: partition %d started", pid)
			defer v.opts.log.Printf("view: partition %d stopped", pid)
			if err := par.st.Open(); err != nil {
				return fmt.Errorf("view: error opening storage partition %d: %v", pid, err)
			}
			if err := par.startCatchup(ctx); err != nil {
				return fmt.Errorf("view: error running partition %d: %v", pid, err)
			}
			return nil
		})
	}

	// wait for partition goroutines and shutdown
	errs := errg.Wait()

	log.Println("view: closing consumer")
	if err := v.consumer.Close(); err != nil {
		_ = errs.Collect(fmt.Errorf("view: failed closing consumer: %v", err))
	}

	if !v.opts.restartable {
		v.terminated = true
		errs = errs.Merge(v.close())
	}

	return errs.NilOrError()
}

// close closes all storage partitions
func (v *View) close() *multierr.Errors {
	errs := new(multierr.Errors)
	for _, p := range v.partitions {
		_ = errs.Collect(p.st.Close())
	}
	v.partitions = nil
	return errs
}

// Terminate closes storage partitions. It must be called only if the view is
// restartable (see WithViewRestartable() option). Once Terminate() is called,
// the view cannot be restarted anymore.
func (v *View) Terminate() error {
	if !v.opts.restartable {
		return nil
	}
	v.opts.log.Printf("View: closing")

	// do not allow any reinitialization
	if v.terminated {
		return nil
	}
	v.terminated = true

	if v.opts.restartable {
		return v.close().NilOrError()
	}
	return nil
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

func (v *View) find(key string) (storage.Storage, error) {
	h, err := v.hash(key)
	if err != nil {
		return nil, err
	}
	return v.partitions[h].st, nil
}

// Topic returns  the view's topic
func (v *View) Topic() string {
	return v.topic
}

// Get returns the value for the key in the view, if exists. Nil if it doesn't.
// Get can be called by multiple goroutines concurrently.
// Get can only be called after Recovered returns true.
func (v *View) Get(key string) (interface{}, error) {
	// find partition where key is located
	s, err := v.find(key)
	if err != nil {
		return nil, err
	}

	// get key and return
	data, err := s.Get(key)
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
	s, err := v.find(key)
	if err != nil {
		return false, err
	}

	return s.Has(key)
}

// Iterator returns an iterator that iterates over the state of the View.
func (v *View) Iterator() (Iterator, error) {
	iters := make([]storage.Iterator, 0, len(v.partitions))
	for i := range v.partitions {
		iter, err := v.partitions[i].st.Iterator()
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
		iter, err := v.partitions[i].st.IteratorWithRange([]byte(start), []byte(limit))
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

func (v *View) run(ctx context.Context) error {
	for {
		select {
		case ev := <-v.consumer.Events():
			switch ev := ev.(type) {
			case *kafka.Message:
				partition := v.partitions[int(ev.Partition)]
				select {
				case partition.ch <- ev:
				case <-ctx.Done():
					return nil
				}
			case *kafka.BOF:
				partition := v.partitions[int(ev.Partition)]
				select {
				case partition.ch <- ev:
				case <-ctx.Done():
					return nil
				}
			case *kafka.EOF:
				partition := v.partitions[int(ev.Partition)]
				select {
				case partition.ch <- ev:
				case <-ctx.Done():
					return nil
				}
			case *kafka.Error:
				return fmt.Errorf("view: error from kafka consumer: %v", ev)
			default:
				return fmt.Errorf("view: cannot handle %T = %v", ev, ev)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// Recovered returns true when the view has caught up with events from kafka.
func (v *View) Recovered() bool {
	for _, p := range v.partitions {
		if !p.recovered() {
			return false
		}
	}

	return true
}

// Stats returns a set of performance metrics of the view.
func (v *View) Stats() *ViewStats {
	return v.statsWithContext(context.Background())
}

func (v *View) statsWithContext(ctx context.Context) *ViewStats {
	var (
		m     sync.Mutex
		wg    sync.WaitGroup
		stats = newViewStats()
	)

	wg.Add(len(v.partitions))
	for i, p := range v.partitions {
		go func(pid int32, par *partition) {
			s := par.fetchStats(ctx)
			m.Lock()
			stats.Partitions[pid] = s
			m.Unlock()
			wg.Done()
		}(int32(i), p)
	}
	wg.Wait()
	return stats
}
