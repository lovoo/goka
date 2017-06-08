package goka

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/storage"

	"github.com/rcrowley/go-metrics"
)

const (
	defaultReaderStoragePath = "/tmp/goka_view"
)

// Getter functions return a value for a key or an error. If no value exists for the key, nil is returned without errors.
type Getter func(string) (interface{}, error)

// View is a materialized (i.e. persistent) cache of a group table.
type View struct {
	topic      string
	opts       *voptions
	partitions []*partition
	consumer   kafka.Consumer
	done       chan bool
	dead       chan bool

	errors   Errors
	stopOnce sync.Once
}

// NewView creates a new View object from a group.
func NewView(brokers []string, topic Table, codec Codec, options ...ViewOption) (*View, error) {

	options = append(
		// default options comes first
		[]ViewOption{
			WithViewLogger(logger.Default()),
			WithViewCallback(DefaultUpdate),
			WithViewStoragePath(defaultReaderStoragePath),
			WithViewPartitionChannelSize(defaultPartitionChannelSize),
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

	consumer, err := opts.builders.consumer(brokers, "goka-view", opts.kafkaRegistry)
	if err != nil {
		return nil, fmt.Errorf("view: cannot create Kafka consumer: %v", err)
	}

	v := &View{
		topic:    string(topic),
		opts:     opts,
		consumer: consumer,
		done:     make(chan bool),
		dead:     make(chan bool),
	}

	if err = v.createPartitions(brokers); err != nil {
		return nil, err
	}

	return v, err
}

// Registry returns the go-metrics registry used by the view.
func (v *View) Registry() metrics.Registry {
	return v.opts.registry
}

func (v *View) createPartitions(brokers []string) (err error) {
	tm, err := v.opts.builders.topicmgr(brokers)
	if err != nil {
		return fmt.Errorf("Error creating topic manager: %v", err)
	}
	defer func() {
		e := tm.Close()
		if e != nil && err == nil {
			err = fmt.Errorf("Error closing topic manager: %v", e)
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
		reg := metrics.NewPrefixedChildRegistry(v.opts.gokaRegistry,
			fmt.Sprintf("%s.%d.", v.topic, p))

		st, err := v.opts.builders.storage(v.topic, p, v.opts.tableCodec, reg)
		if err != nil {
			// TODO(diogo): gracefully terminate all partitions
			return fmt.Errorf("Error creating local storage for partition %d: %v", p, err)
		}

		po := newPartition(v.opts.log, v.topic, nil,
			&storageProxy{Storage: st, partition: p, update: v.opts.updateCallback},
			&proxy{p, v.consumer},
			reg,
			v.opts.partitionChannelSize,
		)
		v.partitions = append(v.partitions, po)
	}

	return nil
}

// Start starts consuming the view's topic.
func (v *View) Start() error {
	go v.run()

	var wg sync.WaitGroup
	wg.Add(len(v.partitions))
	for id, p := range v.partitions {
		go func(id int32, p *partition) {
			defer wg.Done()
			err := p.startCatchup()
			if err != nil {
				v.fail(fmt.Errorf("view: error opening partition %d: %v", id, err))
				return
			}
		}(int32(id), p)
	}
	wg.Wait()

	<-v.dead
	if v.errors.hasErrors() {
		return &v.errors
	}
	return nil
}

func (v *View) fail(err error) {
	v.errors.collect(err)
	go v.stop()
}

func (v *View) stop() {
	v.stopOnce.Do(func() {
		defer close(v.dead)
		// stop consumer
		if err := v.consumer.Close(); err != nil {
			v.errors.collect(fmt.Errorf("failed to close consumer on stopping the view: %v", err))
		}
		<-v.done

		var wg sync.WaitGroup
		for _, par := range v.partitions {
			wg.Add(1)
			go func(p *partition) {
				p.stop()
				wg.Done()
			}(par)
		}
		wg.Wait()
	})
}

// Stop stops the view, frees any resources + connections to kafka
func (v *View) Stop() {
	v.opts.log.Printf("View: stopping")
	v.stop()
	v.opts.log.Printf("View: shutdown complete")
}

func (v *View) hash(key string) (int32, error) {
	// create a new hasher every time. Alternative would be to store the hash in
	// view and every time reset the hasher (ie, hasher.Reset()). But that would
	// also require us to protect the access of the hasher with a mutex.
	hasher := fnv.New32a()

	_, err := hasher.Write([]byte(key))
	if err != nil {
		return -1, err
	}
	hash := int32(hasher.Sum32())
	if hash < 0 {
		hash = -hash
	}
	if len(v.partitions) == 0 {
		return 0, errors.New("No partitions found.")
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
func (v *View) Get(key string) (interface{}, error) {
	// find partition where key is located
	s, err := v.find(key)
	if err != nil {
		return nil, err
	}

	// get key and return
	val, err := s.Get(key)
	if err != nil {
		return nil, err
	}

	// if the key does not exist the return value is nil
	return val, nil
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

func (v *View) run() {
	defer close(v.done)
	v.opts.log.Printf("View: started")
	defer v.opts.log.Printf("View: stopped")

	for ev := range v.consumer.Events() {
		switch ev := ev.(type) {
		case *kafka.Message:
			partition := v.partitions[int(ev.Partition)]
			partition.ch <- ev
		case *kafka.BOF:
			partition := v.partitions[int(ev.Partition)]
			partition.ch <- ev
		case *kafka.EOF:
			partition := v.partitions[int(ev.Partition)]
			partition.ch <- ev
		case *kafka.Error:
			v.fail(fmt.Errorf("view: error from kafka consumer: %v", ev))
			return
		default:
			v.fail(fmt.Errorf("view: cannot handle %T = %v", ev, ev))
			return
		}
	}
}

// Ready returns true when the view has caught up with events from kafka.
func (v *View) Ready() bool {
	for _, p := range v.partitions {
		if !p.ready() {
			return false
		}
	}

	return true
}
