package goka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
)

const (
	// ProcStateIdle indicates an idling partition processor (not started yet)
	ProcStateIdle State = iota
	// ProcStateStarting indicates a starting partition processor, i.e. before rebalance
	ProcStateStarting
	// ProcStateSetup indicates a partition processor during setup of a rebalance round
	ProcStateSetup
	// ProcStateRunning indicates a running partition processor
	ProcStateRunning
	// ProcStateStopping indicates a stopping partition processor
	ProcStateStopping
)

// ProcessCallback function is called for every message received by the
// processor.
type ProcessCallback func(ctx Context, msg interface{})

// Processor is a set of stateful callback functions that, on the arrival of
// messages, modify the content of a table (the group table) and emit messages into other
// topics. Messages as well as rows in the group table are key-value pairs.
// A group is composed by multiple processor instances.
type Processor struct {
	opts    *poptions
	log     logger.Logger
	brokers []string

	rebalanceCallback RebalanceCallback

	// Partition processors
	partitions map[int32]*PartitionProcessor
	// lookup tables
	lookupTables map[string]*View

	partitionCount int

	graph *GroupGraph

	saramaConsumer sarama.Consumer
	producer       Producer
	tmgr           TopicManager

	state *Signal

	ctx    context.Context
	cancel context.CancelFunc
}

// NewProcessor creates a processor instance in a group given the address of
// Kafka brokers, the consumer group name, a list of subscriptions (topics,
// codecs, and callbacks), and series of options.
func NewProcessor(brokers []string, gg *GroupGraph, options ...ProcessorOption) (*Processor, error) {
	options = append(
		// default options comes first
		[]ProcessorOption{
			WithClientID(fmt.Sprintf("goka-processor-%s", gg.Group())),
			WithLogger(logger.Default()),
			WithUpdateCallback(DefaultUpdate),
			WithPartitionChannelSize(defaultPartitionChannelSize),
			WithStorageBuilder(storage.DefaultBuilder(DefaultProcessorStoragePath(gg.Group()))),
			WithRebalanceCallback(DefaultRebalance),
		},

		// user-defined options (may overwrite default ones)
		options...,
	)

	if err := gg.Validate(); err != nil {
		return nil, err
	}

	opts := new(poptions)
	err := opts.applyOptions(gg, options...)
	if err != nil {
		return nil, fmt.Errorf(errApplyOptions, err)
	}

	npar, err := prepareTopics(brokers, gg, opts)
	if err != nil {
		return nil, err
	}

	// create views
	lookupTables := make(map[string]*View)
	for _, t := range gg.LookupTables() {
		view, err := NewView(brokers, Table(t.Topic()), t.Codec(),
			WithViewLogger(opts.log),
			WithViewHasher(opts.hasher),
			WithViewClientID(opts.clientID),
			WithViewTopicManagerBuilder(opts.builders.topicmgr),
			WithViewStorageBuilder(opts.builders.storage),
			WithViewConsumerSaramaBuilder(opts.builders.consumerSarama),
		)
		if err != nil {
			return nil, fmt.Errorf("error creating view: %v", err)
		}
		lookupTables[t.Topic()] = view
	}

	// combine things together
	processor := &Processor{
		opts:    opts,
		log:     opts.log.Prefix(fmt.Sprintf("Processor %s", gg.Group())),
		brokers: brokers,

		rebalanceCallback: opts.rebalanceCallback,

		partitions:     make(map[int32]*PartitionProcessor),
		partitionCount: npar,
		lookupTables:   lookupTables,

		graph: gg,

		state: NewSignal(ProcStateIdle, ProcStateStarting, ProcStateSetup, ProcStateRunning, ProcStateStopping).SetState(ProcStateIdle),
	}

	return processor, nil
}

// Graph returns the group graph of the processor.
func (g *Processor) Graph() *GroupGraph {
	return g.graph
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

	return g.partitions[p].table.st, nil
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

// Run starts the processor using passed context.
// The processor stops in case of errors or if the context is cancelled
func (g *Processor) Run(ctx context.Context) (rerr error) {
	g.log.Debugf("starting")
	defer g.log.Debugf("stopped")

	// create errorgroup
	ctx, g.cancel = context.WithCancel(ctx)
	errg, ctx := multierr.NewErrGroup(ctx)
	g.ctx = ctx
	defer g.cancel()

	// set a starting state. From this point on we know that there's a cancel and a valid context set
	// in the processor which we can use for waiting
	g.state.SetState(ProcStateStarting)

	// collect all errors before leaving
	errors := new(multierr.Errors)
	defer func() {
		_ = errors.Collect(rerr)
		rerr = errors.NilOrError()
	}()

	// create kafka consumer
	consumerGroup, err := g.opts.builders.consumerGroup(g.brokers, string(g.graph.Group()), g.opts.clientID)
	if err != nil {
		return fmt.Errorf(errBuildConsumer, err)
	}

	go func() {
		for err := range consumerGroup.Errors() {
			g.log.Printf("Error executing group consumer: %v", err)
		}
	}()

	g.saramaConsumer, err = g.opts.builders.consumerSarama(g.brokers, g.opts.clientID)
	if err != nil {
		return fmt.Errorf("Error creating consumer for brokers [%s]: %v", strings.Join(g.brokers, ","), err)
	}

	g.tmgr, err = g.opts.builders.topicmgr(g.brokers)
	if err != nil {
		return fmt.Errorf("Error creating topic manager for brokers [%s]: %v", strings.Join(g.brokers, ","), err)
	}

	// create kafka producer
	g.log.Debugf("creating producer")
	producer, err := g.opts.builders.producer(g.brokers, g.opts.clientID, g.opts.hasher)
	if err != nil {
		return fmt.Errorf(errBuildProducer, err)
	}
	g.producer = producer
	defer func() {
		g.log.Debugf("closing producer")
		defer g.log.Debugf("producer ... closed")
		if err := g.producer.Close(); err != nil {
			errors.Collect(fmt.Errorf("error closing producer: %v", err))
		}
	}()

	// start all lookup tables
	for topic, view := range g.lookupTables {
		g.log.Debugf("Starting lookup table for %s", topic)
		// make local copies
		topic, view := topic, view
		errg.Go(func() error {
			if err := view.Run(ctx); err != nil {
				return fmt.Errorf("error running lookup table %s: %v", topic, err)
			}
			return nil
		})
	}

	g.waitForLookupTables()

	// run the main rebalance-consume-loop
	errg.Go(func() error {
		return g.rebalanceLoop(ctx, consumerGroup)
	})

	return errg.Wait().NilOrError()
}

func (g *Processor) rebalanceLoop(ctx context.Context, consumerGroup sarama.ConsumerGroup) (rerr error) {
	var topics []string
	for _, e := range g.graph.InputStreams() {
		topics = append(topics, e.Topic())
	}
	if g.graph.LoopStream() != nil {
		topics = append(topics, g.graph.LoopStream().Topic())
	}

	var errs = new(multierr.Errors)

	defer func() {
		errs.Collect(rerr)
		rerr = errs.NilOrError()
	}()

	defer func() {
		g.log.Debugf("closing consumer group")
		defer g.log.Debugf("closing consumer group ... done")
		errs.Collect(consumerGroup.Close())
	}()

	for {
		var (
			consumeErr = make(chan error)
		)
		go func() {
			g.log.Debugf("consuming from consumer loop")
			defer g.log.Debugf("consuming from consumer loop done")
			defer close(consumeErr)
			err := consumerGroup.Consume(ctx, topics, g)
			if err != nil {
				consumeErr <- err
			}
		}()
		select {
		case err := <-consumeErr:
			g.log.Debugf("Consumer group loop done, will stop here")

			if err != nil {
				errs.Collect(err)
				return
			}
		case <-ctx.Done():
			g.log.Debugf("context closed, waiting for processor to finish up")
			err := <-consumeErr
			errs.Collect(err)
			g.log.Debugf("context closed, waiting for processor to finish up")
			return
		}
		// let's wait some time before we retry to consume
		<-time.After(5 * time.Second)
	}
}

func (g *Processor) waitForLookupTables() {

	// no tables to wait for
	if len(g.lookupTables) == 0 {
		return
	}
	multiWait := multierr.NewMultiWait(g.ctx, len(g.lookupTables))

	// init the multiwait with all
	for _, view := range g.lookupTables {
		multiWait.Add(view.WaitRunning())
	}

	var (
		start     = time.Now()
		logTicker = time.NewTicker(1 * time.Minute)
	)

	defer logTicker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			g.log.Debugf("Stopping to wait for views to get up, context closed")
			return
		case <-multiWait.Done():
			g.log.Debugf("View catchup finished")
			return
		case <-logTicker.C:
			var tablesWaiting []string
			for topic, view := range g.lookupTables {
				if !view.Recovered() {
					tablesWaiting = append(tablesWaiting, topic)
				}
			}
			g.log.Printf("Waiting for views [%s] to catchup since %.2f minutes",
				strings.Join(tablesWaiting, ", "),
				time.Since(start).Minutes())
		}
	}
}

// Recovered returns whether the processor is running, i.e. if the processor
// has recovered all lookups/joins/tables and is running
func (g *Processor) Recovered() bool {
	return g.state.IsState(ProcStateRunning)
}

func (g *Processor) assignmentFromSession(session sarama.ConsumerGroupSession) (Assignment, error) {
	var (
		assignment Assignment
	)

	// get the partitions this processor is assigned to.
	// We use that loop to verify copartitioning and fail otherwise
	for _, claim := range session.Claims() {

		// for first claim, generate the assignment
		if assignment == nil {
			assignment = Assignment{}
			for _, part := range claim {
				assignment[part] = sarama.OffsetNewest
			}
		} else {
			// for all others, verify the assignment is the same

			// first check length
			if len(claim) != len(assignment) {
				return nil, fmt.Errorf("session claims are not copartitioned: %#v", session.Claims())
			}

			// then check the partitions are exactly the same
			for _, part := range claim {
				if _, exists := assignment[part]; !exists {
					return nil, fmt.Errorf("session claims are not copartitioned: %#v", session.Claims())
				}
			}
		}
	}

	return assignment, nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (g *Processor) Setup(session sarama.ConsumerGroupSession) error {
	g.state.SetState(ProcStateSetup)
	defer g.state.SetState(ProcStateRunning)
	g.log.Debugf("setup generation %d, claims=%#v", session.GenerationID(), session.Claims())
	defer g.log.Debugf("setup generation %d ... done", session.GenerationID())

	assignment, err := g.assignmentFromSession(session)
	if err != nil {
		return fmt.Errorf("Error verifying assignment from session: %v", err)
	}

	if g.rebalanceCallback != nil {
		g.rebalanceCallback(assignment)
	}

	// no partitions configured, we cannot setup anything
	if len(assignment) == 0 {
		g.log.Printf("No partitions assigned. Claims were: %#v. Will probably sleep this generation", session.Claims())
		return nil
	}
	// create partition views for all partitions
	for partition := range assignment {
		// create partition processor for our partition
		err := g.createPartitionProcessor(session.Context(), partition, session)
		if err != nil {
			return fmt.Errorf("Error creating partition processor for %s/%d: %v", g.Graph().Group(), partition, err)
		}
	}

	// setup all processors
	errg, _ := multierr.NewErrGroup(session.Context())
	for _, partition := range g.partitions {
		pproc := partition
		errg.Go(func() error {
			return pproc.Setup(session.Context())
		})
	}

	return errg.Wait().NilOrError()
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (g *Processor) Cleanup(session sarama.ConsumerGroupSession) error {
	g.log.Debugf("Cleaning up for %d", session.GenerationID())
	defer g.log.Debugf("Cleaning up for %d ... done", session.GenerationID())

	g.state.SetState(ProcStateStopping)
	defer g.state.SetState(ProcStateIdle)
	errg, _ := multierr.NewErrGroup(session.Context())
	for part, partition := range g.partitions {
		partID, pproc := part, partition
		errg.Go(func() error {
			err := pproc.Stop()
			if err != nil {
				return fmt.Errorf("error stopping partition processor %d: %v", partID, err)
			}
			return nil
		})
	}
	err := errg.Wait().NilOrError()
	g.partitions = make(map[int32]*PartitionProcessor)
	return err
}

// WaitForReady waits until the processor is ready to consume messages (or is actually consuming messages)
// i.e., it is done catching up all partition tables, joins and lookup tables
func (g *Processor) WaitForReady() {
	// wait for the processor to be started
	<-g.state.WaitForStateMin(ProcStateStarting)

	// wait that the processor is actually running
	select {
	case <-g.state.WaitForState(ProcStateRunning):
	case <-g.ctx.Done():
	}

	// wait for all partitionprocessors to be running
	for _, part := range g.partitions {
		select {
		case <-part.state.WaitForState(PPStateRunning):
		case <-g.ctx.Done():
		}
	}
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (g *Processor) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	g.log.Debugf("ConsumeClaim for topic/partition %s/%d, initialOffset=%d", claim.Topic(), claim.Partition(), claim.InitialOffset())
	defer g.log.Debugf("ConsumeClaim done for topic/partition %s/%d", claim.Topic(), claim.Partition())
	part, has := g.partitions[claim.Partition()]
	if !has {
		return fmt.Errorf("No partition (%d) to handle input in topic %s", claim.Partition(), claim.Topic())
	}

	messages := claim.Messages()
	errors := part.Errors()

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				return nil
			}

			select {
			case part.input <- msg:
			case err := <-errors:
				return err
			}

		case err := <-errors:
			return err
		}
	}
}

// Stats returns the aggregated stats for the processor including all partitions, tables, lookups and joins
func (g *Processor) Stats() *ProcessorStats {
	return g.StatsWithContext(context.Background())
}

// StatsWithContext returns stats for the processor, see #Processor.Stats()
func (g *Processor) StatsWithContext(ctx context.Context) *ProcessorStats {
	var (
		m     sync.Mutex
		stats = newProcessorStats(len(g.partitions))
	)

	errg, ctx := multierr.NewErrGroup(ctx)

	// get partition-processor stats
	for partID, proc := range g.partitions {
		partID, proc := partID, proc
		errg.Go(func() error {
			partStats := proc.fetchStats(ctx)

			m.Lock()
			defer m.Unlock()
			stats.Group[partID] = partStats
			return nil
		})
	}

	for topic, view := range g.lookupTables {
		topic, view := topic, view
		errg.Go(func() error {
			m.Lock()
			viewStats := view.Stats(ctx)
			defer m.Unlock()
			stats.Lookup[topic] = viewStats
			return nil
		})
	}

	err := errg.Wait().NilOrError()
	if err != nil {
		g.log.Printf("Error retrieving stats: %v", err)
	}
	return stats
}

// creates the partition that is responsible for the group processor's table
func (g *Processor) createPartitionProcessor(ctx context.Context, partition int32, session sarama.ConsumerGroupSession) error {

	g.log.Debugf("Creating partition processor for partition %d", partition)
	if _, has := g.partitions[partition]; has {
		return fmt.Errorf("processor [%s]: partition %d already exists", g.graph.Group(), partition)
	}

	backoff, err := g.opts.builders.backoff()
	if err != nil {
		return fmt.Errorf("processor [%s]: could not build backoff handler: %v", g.graph.Group(), err)
	}
	pproc := newPartitionProcessor(partition, g.graph, session, g.log, g.opts, g.lookupTables, g.saramaConsumer, g.producer, g.tmgr, backoff, g.opts.backoffResetTime)

	g.partitions[partition] = pproc
	return nil
}

// Stop stops the processor.
// This is semantically equivalent of closing the Context
// that was passed to Processor.Run(..).
// This method will return immediately, errors during running
// will be returned from teh Processor.Run(..)
func (g *Processor) Stop() {
	g.cancel()
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
	npar, err = ensureCopartitioned(tm, gg.copartitioned().Topics())
	if err != nil {
		return 0, err
	}

	// TODO(diogo): add output topics
	if ls := gg.LoopStream(); ls != nil {
		ensureStreams := []string{ls.Topic()}
		for _, t := range ensureStreams {
			if err = tm.EnsureStreamExists(t, npar); err != nil {
				return 0, err
			}
		}
	}

	if gt := gg.GroupTable(); gt != nil {
		if err = tm.EnsureTableExists(gt.Topic(), npar); err != nil {
			return 0, err
		}
	}

	return
}

// returns the number of partitions the topics have, and an error if topics are
// not copartitionea.
func ensureCopartitioned(tm TopicManager, topics []string) (int, error) {
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
