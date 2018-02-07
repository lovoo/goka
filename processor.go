package goka

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
)

// Processor is a set of stateful callback functions that, on the arrival of
// messages, modify the content of a table (the group table) and emit messages into other
// topics. Messages as well as rows in the group table are key-value pairs.
// A group is composed by multiple processor instances.
type Processor struct {
	opts *poptions

	partitions     map[int32]*partition
	partitionViews map[int32]map[string]*partition
	partitionCount int
	views          map[string]*View

	graph *GroupGraph
	m     sync.RWMutex

	consumer kafka.Consumer
	producer kafka.Producer
	dying    chan bool // stop() goroutine asks start() goroutine to exit
	done     chan bool // start() goroutine is ready to exit
	dead     chan bool // stop() goroutine exits
	errors   multierr.Errors
	stopOnce sync.Once
}

// message to be consumed
type message struct {
	Key       string
	Data      []byte
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
}

// ProcessCallback function is called for every message received by the
// processor.
type ProcessCallback func(ctx Context, msg interface{})

// NewProcessor creates a processor instance in a group given the address of
// Kafka brokers, the consumer group name, a list of subscriptions (topics,
// codecs, and callbacks), and series of options.
func NewProcessor(brokers []string, gg *GroupGraph, options ...ProcessorOption) (*Processor, error) {
	options = append(
		// default options comes first
		[]ProcessorOption{
			WithLogger(logger.Default()),
			WithUpdateCallback(DefaultUpdate),
			WithPartitionChannelSize(defaultPartitionChannelSize),
			WithStorageBuilder(DefaultStorageBuilder(DefaultProcessorStoragePath(gg.Group()))),
		},

		// user-defined options (may overwrite default ones)
		options...,
	)

	if err := gg.Validate(); err != nil {
		return nil, err
	}

	opts := new(poptions)
	err := opts.applyOptions(string(gg.Group()), options...)
	if err != nil {
		return nil, fmt.Errorf(errApplyOptions, err)
	}

	npar, err := prepareTopics(brokers, gg, opts)
	if err != nil {
		return nil, err
	}

	// create kafka consumer
	consumer, err := opts.builders.consumer(brokers, string(gg.Group()), opts.clientID)
	if err != nil {
		return nil, fmt.Errorf(errBuildConsumer, err)
	}

	// create kafka producer
	producer, err := opts.builders.producer(brokers)
	if err != nil {
		return nil, fmt.Errorf(errBuildProducer, err)
	}

	// create views
	views := make(map[string]*View)
	for _, t := range gg.LookupTables() {
		view, err := NewView(brokers, Table(t.Topic()), t.Codec(),
			WithViewLogger(opts.log),
			WithViewHasher(opts.hasher),
			WithViewPartitionChannelSize(opts.partitionChannelSize),
			WithViewClientID(opts.clientID),
			WithViewStorageBuilder(opts.builders.storage),
			WithViewConsumerBuilder(opts.builders.consumer),
		)
		if err != nil {
			return nil, fmt.Errorf("error creating view: %v", err)
		}
		views[t.Topic()] = view
	}

	// combine things together
	processor := &Processor{

		opts: opts,

		partitions:     make(map[int32]*partition),
		partitionViews: make(map[int32]map[string]*partition),
		partitionCount: npar,
		views:          views,

		graph: gg,

		consumer: consumer,
		producer: producer,
		done:     make(chan bool),
		dying:    make(chan bool),
		dead:     make(chan bool),
	}

	return processor, nil
}

func prepareTopics(brokers []string, gg *GroupGraph, opts *poptions) (npar int, err error) {
	// create topic manager
	tm, err := opts.builders.topicmgr(brokers)
	if err != nil {
		return 0, fmt.Errorf("Error creating topic manager: %v", err)
	}
	defer func() {
		e := tm.Close()
		if e != nil && err == nil {
			err = fmt.Errorf("Error closing topic manager: %v", e)
		}
	}()

	// check co-partitioned (external) topics have the same number of partitions
	npar, err = copartitioned(tm, gg.inputs().Topics())
	if err != nil {
		return 0, err
	}

	// TODO(diogo): add output topics
	if ls := gg.LoopStream(); ls != nil {
		ensureStreams := []string{ls.Topic()}
		for _, t := range ensureStreams {
			if err = tm.EnsureStreamExists(t, npar); err != nil {
				return
			}
		}
	}

	if gt := gg.GroupTable(); gt != nil {
		if err = tm.EnsureTableExists(gt.Topic(), npar); err != nil {
			return
		}
	}

	return
}

// returns the number of partitions the topics have, and an error if topics are
// not copartitionea.
func copartitioned(tm kafka.TopicManager, topics []string) (int, error) {
	var npar int
	for _, topic := range topics {
		partitions, err := tm.Partitions(topic)
		if err != nil {
			return 0, fmt.Errorf("Error fetching partitions for topic %s: %v", topic, err)
		}

		// check assumption that partitions are gap-less
		for i, p := range partitions {
			if i != int(p) {
				return 0, fmt.Errorf("Topic %s has partition gap: %v", topic, partitions)
			}
		}

		if npar == 0 {
			npar = len(partitions)
		}
		if len(partitions) != npar {
			return 0, fmt.Errorf("Topic %s does not have %d partitions", topic, npar)
		}
	}
	return npar, nil
}

// isStateless returns whether the processor is a stateless one.
func (g *Processor) isStateless() bool {
	return g.graph.GroupTable() == nil
}

///////////////////////////////////////////////////////////////////////////////
// value getter
///////////////////////////////////////////////////////////////////////////////

// Get returns a read-only copy of a value from the group table if the
// respective partition is owned by the processor instace.
// Get can be called by multiple goroutines concurrently.
// Get can be only used with stateful processors (ie, when group table is
// enabled) and after Recovered returns true.
func (g *Processor) Get(key string) (interface{}, error) {
	if g.isStateless() {
		return nil, fmt.Errorf("can't get a value from stateless processor")
	}

	// find partition where key is located
	s, err := g.find(key)
	if err != nil {
		return nil, err
	}

	// get key and return
	val, err := s.Get(key)
	if err != nil {
		return nil, fmt.Errorf("error getting %s: %v", key, err)
	} else if val == nil {
		// if the key does not exist the return value is nil
		return nil, nil
	}

	// since we don't know what the codec does, make copy of the object
	data := make([]byte, len(val))
	copy(data, val)
	value, err := g.graph.GroupTable().Codec().Decode(data)
	if err != nil {
		return nil, fmt.Errorf("error decoding %s: %v", key, err)
	}
	return value, nil
}

func (g *Processor) find(key string) (storage.Storage, error) {
	p, err := g.hash(key)
	if err != nil {
		return nil, err
	}

	if _, ok := g.partitions[p]; !ok {
		return nil, fmt.Errorf("this processor does not contain partition %v", p)
	}

	return g.partitions[p].st, nil
}

func (g *Processor) hash(key string) (int32, error) {
	// create a new hasher every time. Alternative would be to store the hash in
	// view and every time reset the hasher (ie, hasher.Reset()). But that would
	// also require us to protect the access of the hasher with a mutex.
	hasher := g.opts.hasher()

	_, err := hasher.Write([]byte(key))
	if err != nil {
		return -1, err
	}
	hash := int32(hasher.Sum32())
	if hash < 0 {
		hash = -hash
	}
	if g.partitionCount == 0 {
		return 0, errors.New("can't hash with 0 partitions")
	}
	return hash % int32(g.partitionCount), nil
}

///////////////////////////////////////////////////////////////////////////////
// start/stop
///////////////////////////////////////////////////////////////////////////////

// Start starts receiving messages from Kafka for the subscribed topics. For each
// partition, a recovery will be attempted.
func (g *Processor) Start() error {
	// start all views
	for t, v := range g.views {
		go func(t string, v *View) {
			err := v.Start()
			if err != nil {
				g.fail(fmt.Errorf("error in view %s: %v", t, err))
			}
		}(t, v)
	}

	topics := make(map[string]int64)
	for _, e := range g.graph.InputStreams() {
		topics[e.Topic()] = -1
	}
	if lt := g.graph.LoopStream(); lt != nil {
		topics[lt.Topic()] = -1
	}
	if err := g.consumer.Subscribe(topics); err != nil {
		g.errors.Collect(fmt.Errorf("error subscribing topics: %v", err))
		close(g.done)
		return &g.errors
	}
	g.run()

	close(g.done)
	<-g.dead // wait for stop goroutine to return from stop() call
	if g.errors.HasErrors() {
		return &g.errors
	}
	return nil
}

func (g *Processor) pushToPartition(part int32, ev kafka.Event) error {
	p, ok := g.partitions[part]
	if !ok {
		return fmt.Errorf("dropping message, no partition yet: %v", ev)
	}
	select {
	case p.ch <- ev:
	case <-g.dying:
	}
	return nil
}

func (g *Processor) pushToPartitionView(topic string, part int32, ev kafka.Event) error {
	views, ok := g.partitionViews[part]
	if !ok {
		return fmt.Errorf("dropping message, no partition yet: %v", ev)
	}
	p, ok := views[topic]
	if !ok {
		return fmt.Errorf("dropping message, no view yet: %v", ev)
	}
	select {
	case p.ch <- ev:
	case <-g.dying:
	}
	return nil
}

func (g *Processor) run() {
	g.opts.log.Printf("Processor: started")
	defer g.opts.log.Printf("Processor: stopped")
	var failed bool

	for ev := range g.consumer.Events() {
		if _, ok := ev.(*kafka.Error); failed && !ok {
			// if we failed and message is not a further error, drop message
			continue
		}

		switch ev := ev.(type) {
		case *kafka.Assignment:
			err := g.rebalance(*ev)
			if err != nil {
				g.fail(fmt.Errorf("error on rebalance: %v", err))
				failed = true
			}

		case *kafka.Message:
			var err error
			if g.graph.joint(ev.Topic) {
				err = g.pushToPartitionView(ev.Topic, ev.Partition, ev)
			} else {
				err = g.pushToPartition(ev.Partition, ev)
			}
			if err != nil {
				g.fail(fmt.Errorf("error consuming message: %v", err))
				failed = true
			}

		case *kafka.BOF:
			var err error
			if g.graph.joint(ev.Topic) {
				err = g.pushToPartitionView(ev.Topic, ev.Partition, ev)
			} else {
				err = g.pushToPartition(ev.Partition, ev)
			}
			if err != nil {
				g.fail(fmt.Errorf("error consuming BOF: %v", err))
				failed = true
			}

		case *kafka.EOF:
			var err error
			if g.graph.joint(ev.Topic) {
				err = g.pushToPartitionView(ev.Topic, ev.Partition, ev)
			} else {
				err = g.pushToPartition(ev.Partition, ev)
			}
			if err != nil {
				g.fail(fmt.Errorf("error consuming EOF: %v", err))
				failed = true
			}

		case *kafka.NOP:
			if g.graph.joint(ev.Topic) {
				_ = g.pushToPartitionView(ev.Topic, ev.Partition, ev)
			} else {
				_ = g.pushToPartition(ev.Partition, ev)
			}

		case *kafka.Error:
			g.fail(ev.Err)
			failed = true

		default:
			g.fail(fmt.Errorf("processor: cannot handle %T = %v", ev, ev))
			failed = true
		}
	}
}

func (g *Processor) fail(err error) {
	g.opts.log.Printf("failing: %v", err)
	g.errors.Collect(err)
	go g.stop()
}

func (g *Processor) stop() {
	g.stopOnce.Do(func() {
		close(g.dying) // stops any blocking call
		err := g.consumer.Close()

		g.opts.log.Printf("Processor: wait for main goroutine")
		<-g.done
		if err != nil {
			g.errors.Collect(fmt.Errorf("Failed to close consumer: %v", err))
		}

		// remove all partitions first
		g.opts.log.Printf("Processor: removing partitions")
		for partition := range g.partitions {
			g.removePartition(partition)
		}

		// stop all views
		for _, v := range g.views {
			v.Stop()
		}

		// close connection to Kafka
		if err := g.producer.Close(); err != nil {
			g.errors.Collect(fmt.Errorf("Failed to close producer: %v", err))
		}

		// release Start() goroutine
		close(g.dead)
	})
}

// Stop gracefully stops the consumer
func (g *Processor) Stop() {
	g.opts.log.Printf("Processor: stopping")
	g.stop()
	g.opts.log.Printf("Processor: shutdown complete")
}

///////////////////////////////////////////////////////////////////////////////
// partition management (rebalance)
///////////////////////////////////////////////////////////////////////////////

func (g *Processor) newJoinStorage(topic string, id int32, update UpdateCallback) (*storageProxy, error) {
	st, err := g.opts.builders.storage(topic, id)
	if err != nil {
		return nil, err
	}
	return &storageProxy{
		Storage:   st,
		partition: id,
		update:    update,
	}, nil
}

func (g *Processor) newStorage(topic string, id int32, update UpdateCallback) (*storageProxy, error) {
	if g.isStateless() {
		return &storageProxy{
			Storage:   storage.NewMemory(),
			partition: id,
			stateless: true,
		}, nil
	}

	st, err := g.opts.builders.storage(topic, id)
	if err != nil {
		return nil, err
	}
	return &storageProxy{
		Storage:   st,
		partition: id,
		update:    update,
	}, nil
}

func (g *Processor) createPartitionViews(id int32) error {
	g.m.Lock()
	defer g.m.Unlock()

	if _, has := g.partitionViews[id]; !has {
		g.partitionViews[id] = make(map[string]*partition)
	}

	for _, t := range g.graph.JointTables() {
		if _, has := g.partitions[id]; has {
			continue
		}
		st, err := g.newJoinStorage(t.Topic(), id, DefaultUpdate)
		if err != nil {
			return fmt.Errorf("processor: error creating storage: %v", err)
		}
		p := newPartition(
			g.opts.log,
			t.Topic(),
			nil, st, &proxy{id, g.consumer},
			g.opts.partitionChannelSize,
		)
		g.partitionViews[id][t.Topic()] = p

		go func(par *partition, pid int32) {
			defer func() {
				if err := recover(); err != nil {
					g.opts.log.Printf("partition view %s/%d: panic", par.topic, pid)
					g.fail(fmt.Errorf("panic partition view %s/%d: %v\nstack:%v",
						par.topic, pid, err, string(debug.Stack())))
				}
			}()

			err := par.startCatchup()
			if err != nil {
				g.fail(fmt.Errorf("error in partition view %s/%d: %v", par.topic, pid, err))
			}
			g.opts.log.Printf("partition view %s/%d: exit", par.topic, pid)
		}(p, id)
	}
	return nil
}

func (g *Processor) createPartition(id int32) error {
	if _, has := g.partitions[id]; has {
		return nil
	}
	// TODO(diogo) what name to use for stateless processors?
	var groupTable string
	if gt := g.graph.GroupTable(); gt != nil {
		groupTable = gt.Topic()
	}
	st, err := g.newStorage(groupTable, id, g.opts.updateCallback)
	if err != nil {
		return fmt.Errorf("processor: error creating storage: %v", err)
	}

	// collect dependencies
	var wait []func() bool
	if pviews, has := g.partitionViews[id]; has {
		for _, p := range pviews {
			wait = append(wait, p.recovered)
		}
	}
	for _, v := range g.views {
		wait = append(wait, v.Recovered)
	}

	g.partitions[id] = newPartition(
		g.opts.log,
		groupTable,
		g.process, st, &delayProxy{partition: id, consumer: g.consumer, wait: wait},
		g.opts.partitionChannelSize,
	)

	go func(par *partition) {
		defer func() {
			if err := recover(); err != nil {
				g.opts.log.Printf("partition %s/%d: panic", par.topic, id)
				g.fail(fmt.Errorf("panic partition %s/%d: %v\nstack:%v",
					par.topic, id, err, string(debug.Stack())))
			}
		}()
		err := par.start()
		if err != nil {
			g.fail(fmt.Errorf("error in partition %d: %v", id, err))
		}
		g.opts.log.Printf("partition %s/%d: exit", par.topic, id)
	}(g.partitions[id])

	return nil
}

func (g *Processor) rebalance(partitions kafka.Assignment) error {
	g.opts.log.Printf("Processor: rebalancing: %+v", partitions)

	for id := range partitions {
		// create partition views
		if err := g.createPartitionViews(id); err != nil {
			return err
		}
		// create partition processor
		if err := g.createPartition(id); err != nil {
			return err
		}
	}

	for partition := range g.partitions {
		if _, has := partitions[partition]; !has {
			g.removePartition(partition)
		}
	}
	return nil
}

func (g *Processor) removePartition(partition int32) {
	g.opts.log.Printf("Removing partition %d", partition)

	// remove partition processor
	g.partitions[partition].stop()
	delete(g.partitions, partition)

	// remove partition views
	pv, has := g.partitionViews[partition]
	if !has {
		return
	}

	for _, p := range pv {
		p.stop()
	}
	delete(g.partitionViews, partition)
}

///////////////////////////////////////////////////////////////////////////////
// context builder
///////////////////////////////////////////////////////////////////////////////

func (g *Processor) process(msg *message, st storage.Storage, wg *sync.WaitGroup, pstats *PartitionStats) (int, error) {
	g.m.RLock()
	views := g.partitionViews[msg.Partition]
	g.m.RUnlock()

	ctx := &context{
		graph: g.graph,

		pstats: pstats,
		pviews: views,
		views:  g.views,
		wg:     wg,
		msg:    msg,
		failer: g.fail,
		emitter: func(topic string, key string, value []byte) *kafka.Promise {
			return g.producer.Emit(topic, key, value).Then(func(err error) {
				if err != nil {
					g.fail(err)
				}
			})
		},
	}
	ctx.commit = func() {
		// write group table offset to local storage
		if ctx.counters.stores > 0 {
			if offset, err := ctx.storage.GetOffset(0); err != nil {
				ctx.failer(fmt.Errorf("error getting storage offset for %s/%d: %v",
					g.graph.GroupTable().Topic(), msg.Partition, err))
				return
			} else if err = ctx.storage.SetOffset(offset + int64(ctx.counters.stores)); err != nil {
				ctx.failer(fmt.Errorf("error writing storage offset for %s/%d: %v",
					g.graph.GroupTable().Topic(), msg.Partition, err))
				return
			}
		}

		// mark upstream offset
		if err := g.consumer.Commit(msg.Topic, msg.Partition, msg.Offset); err != nil {
			g.fail(fmt.Errorf("error committing offsets of %s/%d: %v",
				g.graph.GroupTable().Topic(), msg.Partition, err))
		}
	}

	// use the storage if the processor is not stateless. Ignore otherwise
	if !g.isStateless() {
		ctx.storage = st
	}

	var (
		m   interface{}
		err error
	)

	// decide whether to decode or ignore message
	switch {
	case msg.Data == nil && g.opts.nilHandling == NilIgnore:
		// drop nil messages
		return 0, nil
	case msg.Data == nil && g.opts.nilHandling == NilProcess:
		// process nil messages without decoding them
		m = nil
	default:
		// get stream subcription
		codec := g.graph.codec(msg.Topic)
		if codec == nil {
			return 0, fmt.Errorf("cannot handle topic %s", msg.Topic)
		}

		// decode message
		m, err = codec.Decode(msg.Data)
		if err != nil {
			return 0, fmt.Errorf("error decoding message for key %s from %s/%d: %v", msg.Key, msg.Topic, msg.Partition, err)
		}
	}

	cb := g.graph.callback(msg.Topic)
	if cb == nil {
		return 0, fmt.Errorf("error processing message for key %s from %s/%d: %v", msg.Key, msg.Topic, msg.Partition, err)
	}

	// start context and call ProcessorCallback
	ctx.start()
	defer ctx.finish() // execute even in case of panic
	cb(ctx, m)

	return ctx.counters.stores, nil
}

// Recovered returns true when the processor has caught up with events from kafka.
func (g *Processor) Recovered() bool {
	for _, v := range g.views {
		if !v.Recovered() {
			return false
		}
	}

	for _, part := range g.partitionViews {
		for _, topicPart := range part {
			if !topicPart.recovered() {
				return false
			}
		}
	}

	for _, p := range g.partitions {
		if !p.recovered() {
			return false
		}
	}

	return true
}

func (g *Processor) Stats() *ProcessorStats {
	var (
		m     sync.Mutex
		wg    sync.WaitGroup
		stats = newProcessorStats(len(g.partitions))
	)

	for i, p := range g.partitions {
		wg.Add(1)
		go func(pid int32, par *partition) {
			s := par.fetchStats()
			m.Lock()
			stats.Group[pid] = s
			m.Unlock()
			wg.Done()
		}(int32(i), p)
	}
	for i, p := range g.partitionViews {
		if _, ok := stats.Joined[i]; !ok {
			stats.Joined[i] = make(map[string]*PartitionStats)
		}
		for t, tp := range p {
			wg.Add(1)
			go func(pid int32, topic string, par *partition) {
				s := par.fetchStats()
				m.Lock()
				stats.Joined[pid][topic] = s
				m.Unlock()
				wg.Done()
			}(int32(i), t, tp)
		}
	}
	for t, v := range g.views {
		wg.Add(1)
		go func(topic string, vi *View) {
			s := vi.Stats()
			m.Lock()
			stats.Lookup[t] = s
			m.Unlock()
			wg.Done()
		}(t, v)
	}

	wg.Wait()
	return stats
}

func (g *Processor) Graph() *GroupGraph {
	return g.graph
}
