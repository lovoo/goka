package goka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
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
	log     logger
	brokers []string

	rebalanceCallback RebalanceCallback

	// rwmutex protecting read/write of partitions and lookuptables.
	mTables sync.RWMutex
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

	done   chan struct{}
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
		done:  make(chan struct{}),
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

	pproc, ok := g.getPartProc(p)
	if !ok {
		return nil, fmt.Errorf("this processor does not contain partition %v", p)
	}
	return pproc.table.st, nil
}

func (g *Processor) getPartProc(partition int32) (*PartitionProcessor, bool) {
	g.mTables.RLock()
	defer g.mTables.RUnlock()
	pproc, ok := g.partitions[partition]
	return pproc, ok
}

func (g *Processor) setPartProc(partition int32, pproc *PartitionProcessor) {
	g.mTables.Lock()
	defer g.mTables.Unlock()
	if pproc == nil {
		delete(g.partitions, partition)
	} else {
		g.partitions[partition] = pproc
	}
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

	// check if the processor was done already
	select {
	case <-g.done:
		return fmt.Errorf("error running processor: it was already run and terminated. Run can only be called once")
	default:
	}

	// create errorgroup
	ctx, g.cancel = context.WithCancel(ctx)
	errg, ctx := multierr.NewErrGroup(ctx)
	defer close(g.done)
	defer g.cancel()

	// set a starting state. From this point on we know that there's a cancel and a valid context set
	// in the processor which we can use for waiting
	g.state.SetState(ProcStateStarting)

	// collect all errors before leaving
	var errs *multierror.Error
	defer func() {
		rerr = multierror.Append(errs, rerr).ErrorOrNil()
	}()

	var err error
	g.saramaConsumer, err = g.opts.builders.consumerSarama(g.brokers, g.opts.clientID)
	if err != nil {
		return fmt.Errorf("Error creating consumer for brokers [%s]: %v", strings.Join(g.brokers, ","), err)
	}

	// close sarama consume after we're done
	defer func() {
		if err := g.saramaConsumer.Close(); err != nil {
			errs = multierror.Append(errs, fmt.Errorf("error closing sarama consumer: %w", err))
		}
	}()

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
			errs = multierror.Append(errs, fmt.Errorf("error closing producer: %w", err))
		}
	}()

	// start all lookup tables
	g.mTables.RLock()
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
	g.mTables.RUnlock()

	if err := g.waitForStartupTables(ctx); err != nil {
		return fmt.Errorf("error waiting for start up tables: %w", err)
	}

	// run the main rebalance-consume-loop
	errg.Go(func() error {
		return g.rebalanceLoop(ctx)
	})

	return errg.Wait().ErrorOrNil()
}

func (g *Processor) rebalanceLoop(ctx context.Context) (rerr error) {
	// create kafka consumer
	consumerGroup, err := g.opts.builders.consumerGroup(g.brokers, string(g.graph.Group()), g.opts.clientID)
	if err != nil {
		return fmt.Errorf(errBuildConsumer, err)
	}

	var topics []string
	for _, e := range g.graph.InputStreams() {
		topics = append(topics, e.Topic())
	}
	if g.graph.LoopStream() != nil {
		topics = append(topics, g.graph.LoopStream().Topic())
	}

	var errs *multierror.Error

	defer func() {
		rerr = multierror.Append(errs, rerr).ErrorOrNil()
	}()

	defer func() {
		g.log.Debugf("closing consumer group")

		closeErr := consumerGroup.Close()
		if closeErr != nil {
			g.log.Printf("Error closing consumer group: %v", closeErr)
			errs = multierror.Append(
				errs,
				fmt.Errorf("Error closing consumer group: %w", closeErr),
			)
		}

		g.log.Debugf("closing consumer group ... done")
	}()

	for {
		sessionCtx, sessionCtxCancel := context.WithCancel(ctx)

		go func() {
			g.handleSessionErrors(ctx, sessionCtx, sessionCtxCancel, consumerGroup)
		}()

		err := consumerGroup.Consume(ctx, topics, g)
		sessionCtxCancel()

		// error consuming, no way to recover so we have to kill the processor
		if err != nil {
			return fmt.Errorf("error consuming from group consumer: %w", err)
		}

		select {
		case <-time.After(5 * time.Second):
			g.log.Printf("Consumer group returned, Rebalancing.")
		case <-ctx.Done():
			g.log.Printf("Consumer group cancelled. Stopping")
			return nil
		}
	}
}

func (g *Processor) handleSessionErrors(ctx, sessionCtx context.Context, sessionCtxCancel context.CancelFunc, consumerGroup sarama.ConsumerGroup) {
	errs := consumerGroup.Errors()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sessionCtx.Done():
			return
		case err, ok := <-errs:
			if !ok {
				return
			}

			if err != nil {
				g.log.Printf("error during execution of consumer group: %v", err)
			}

			var (
				errProc  *errProcessing
				errSetup *errSetup
			)

			if errors.As(err, &errProc) {
				g.log.Debugf("error processing message (non-transient), shutting down processor: %v", err)
				sessionCtxCancel()
			}
			if errors.As(err, &errSetup) {
				g.log.Debugf("setup error (non-transient), shutting down processor: %v", err)
				sessionCtxCancel()
			}
		}
	}
}

// waits for all tables that are supposed to start up
func (g *Processor) waitForStartupTables(ctx context.Context) error {
	errg, startupCtx := multierr.NewErrGroup(ctx)

	var (
		waitMap  = make(map[string]struct{})
		mWaitMap sync.Mutex
	)

	// we'll wait for all lookup tables to have recovered.
	// For this we're looping through all tables and start
	// a new goroutine that terminates when the table is done (or ctx is closed).
	// The extra code adds and removes the table to a map used for logging
	// the items that the processor is still waiting to recover before ready to go.
	g.mTables.RLock()
	for _, view := range g.lookupTables {
		view := view

		errg.Go(func() error {
			name := fmt.Sprintf("lookup-table-%s", view.topic)
			mWaitMap.Lock()
			waitMap[name] = struct{}{}
			mWaitMap.Unlock()

			defer func() {
				mWaitMap.Lock()
				defer mWaitMap.Unlock()
				delete(waitMap, name)
			}()

			select {
			case <-startupCtx.Done():
			case <-view.WaitRunning():
			}
			return nil
		})
	}
	g.mTables.RUnlock()

	// If we recover ahead, we'll also start all partition processors once in recover-only-mode
	// and do the same boilerplate to keep the waitmap up to date.
	if g.opts.recoverAhead {
		partitions, err := g.findStatefulPartitions()
		if err != nil {
			return fmt.Errorf("error finding dependent partitions: %w", err)
		}
		for _, part := range partitions {
			part := part
			pproc, err := g.createPartitionProcessor(ctx, part, runModeRecoverOnly, func(msg *message, meta string) {
				panic("a partition processor in recover-only-mode never commits a message")
			})
			if err != nil {
				return fmt.Errorf("Error creating partition processor for recover-ahead %s/%d: %v", g.Graph().Group(), part, err)
			}
			errg.Go(func() error {
				name := fmt.Sprintf("partition-processor-%d", part)
				mWaitMap.Lock()
				waitMap[name] = struct{}{}
				mWaitMap.Unlock()

				defer func() {
					mWaitMap.Lock()
					defer mWaitMap.Unlock()
					delete(waitMap, name)
				}()

				g.setPartProc(part, pproc)
				defer g.setPartProc(part, nil)

				err := pproc.Start(ctx, ctx)
				if err != nil {
					return err
				}
				return pproc.Stop()
			})
		}
	}

	var (
		start     = time.Now()
		logTicker = time.NewTicker(1 * time.Minute)
	)

	// Now run through
	defer logTicker.Stop()
	errgWaiter := errg.WaitChan()
	for {
		select {
		// the context has closed, no point in waiting
		case <-ctx.Done():
			g.log.Debugf("Stopping to wait for views to get up, context closed")
			return fmt.Errorf("context closed while waiting for startup tables to become ready")

			// the error group is done, which means
			// * err==nil --> it's done
			// * err!=nil --> it failed, let's return the error
		case errs := <-errgWaiter:
			err := errs.ErrorOrNil()
			if err == nil {
				g.log.Debugf("View catchup finished")
			}
			return err

		// log the things we're still waiting for
		case <-logTicker.C:
			var tablesWaiting []string
			mWaitMap.Lock()
			for table := range waitMap {
				tablesWaiting = append(tablesWaiting, table)
			}
			mWaitMap.Unlock()

			g.log.Printf("Waiting for [%s] to start up since %.2f minutes",
				strings.Join(tablesWaiting, ", "),
				time.Since(start).Minutes())
		}
	}
}

// find partitions that will any type of local state for this processor.
// This includes joins and the group-table. If neither are present, it returns an empty list, because
// it means that the processor is stateless and has only streaming-input.
// It returns the list of partitions as an error, or empty if there are no such partitions.
//
// Supports to pass optional map of excluded partitions if the function is used to determine partitions
// that are not part of the current assignment
func (g *Processor) findStatefulPartitions() ([]int32, error) {
	var (
		err           error
		allPartitions []int32
	)
	for _, edge := range chainEdges(g.graph.groupTable, g.graph.inputTables) {
		allPartitions, err = g.tmgr.Partitions(edge.Topic())
		if err != nil && err != errTopicNotFound {
			return nil, err
		}
		if len(allPartitions) > 0 {
			break
		}
	}

	return allPartitions, err
}

// Recovered returns whether the processor is running, i.e. if the processor
// has recovered all lookups/joins/tables and is running
func (g *Processor) Recovered() bool {
	return g.state.IsState(ProcStateRunning)
}

// StateReader returns a read only interface of the processors state.
func (g *Processor) StateReader() StateReader {
	return g.state
}

func (g *Processor) assignmentFromSession(session sarama.ConsumerGroupSession) (Assignment, error) {
	var assignment Assignment

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
	g.log.Printf("setup generation %d, claims=%#v", session.GenerationID(), session.Claims())
	defer g.log.Debugf("setup generation %d ... done", session.GenerationID())

	if err := g.createAssignedPartitions(session); err != nil {
		return err
	}

	if err := g.createHotStandbyPartitions(session); err != nil {
		return err
	}

	// setup all processors
	setupErrg, setupCtx := multierr.NewErrGroup(session.Context())
	g.mTables.RLock()
	for partID, partition := range g.partitions {
		pproc := partition
		setupErrg.Go(func() error {
			// the partition processors need two contexts:
			// setupCtx --> for this setup, which we'll wait for
			// the runner ctx, which is active during a session
			err := pproc.Start(setupCtx, session.Context())
			if err != nil {
				return newErrSetup(partID, err)
			}
			return nil
		})
	}
	g.mTables.RUnlock()

	return setupErrg.Wait().ErrorOrNil()
}

func (g *Processor) createAssignedPartitions(session sarama.ConsumerGroupSession) error {
	assignment, err := g.assignmentFromSession(session)
	if err != nil {
		return fmt.Errorf("Error verifying assignment from session: %v", err)
	}

	if g.rebalanceCallback != nil {
		g.rebalanceCallback(assignment)
	}

	// no partitions configured, just print a log but continue
	// in case we have configured standby, we should still start the standby-processors
	if len(assignment) == 0 {
		g.log.Printf("No partitions assigned. Claims were: %#v. Will probably sleep this generation", session.Claims())
	}
	// create partition views for all partitions
	for partition := range assignment {
		// create partition processor for our partition
		pproc, err := g.createPartitionProcessor(session.Context(), partition, runModeActive,
			createMessageCommitter(session),
		)
		if err != nil {
			return fmt.Errorf("Error creating partition processor for %s/%d: %v", g.Graph().Group(), partition, err)
		}
		g.setPartProc(partition, pproc)
	}
	return nil
}

// if hot standby is configured, we find the partitions that are missing
// from the current assignment, and create those processors in standby mode
func (g *Processor) createHotStandbyPartitions(session sarama.ConsumerGroupSession) error {
	if !g.opts.hotStandby {
		return nil
	}
	allPartitions, err := g.findStatefulPartitions()
	if err != nil {
		return fmt.Errorf("Error finding partitions for standby")
	}
	for _, standby := range allPartitions {
		// if the partition already exists, it means it is part of the assignment, so we don't
		// need to keep it on hot-standby and can ignore it.

		// we check if the partition processor exists and if it does, ignore it
		// since it's part of the assignment
		if _, exists := g.getPartProc(standby); exists {
			continue
		}

		// otherwise, let's start the partition processor in passive mode
		pproc, err := g.createPartitionProcessor(session.Context(), standby, runModePassive,
			func(msg *message, metadata string) {
				panic("a passive partition processor should never receive input messages, thus never commit any messages")
			})
		if err != nil {
			return fmt.Errorf("Error creating partition processor for %s/%d: %v", g.Graph().Group(), standby, err)
		}
		g.setPartProc(standby, pproc)
	}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (g *Processor) Cleanup(session sarama.ConsumerGroupSession) error {
	g.log.Debugf("Cleaning up for %d", session.GenerationID())
	defer g.log.Debugf("Cleaning up for %d ... done", session.GenerationID())

	g.state.SetState(ProcStateStopping)
	defer g.state.SetState(ProcStateIdle)
	errg, _ := multierr.NewErrGroup(session.Context())
	g.mTables.RLock()
	for part, partition := range g.partitions {
		partID, pproc := part, partition
		errg.Go(func() error {
			err := pproc.Stop()
			if err != nil {
				g.log.Printf("Error running/stopping partition processor %d: %v", partID, err)
				return err
			}
			return nil
		})
	}
	g.mTables.RUnlock()
	err := errg.Wait().ErrorOrNil()

	// reset the partitions after the session ends
	g.mTables.Lock()
	defer g.mTables.Unlock()
	g.partitions = make(map[int32]*PartitionProcessor)
	return err
}

// WaitForReady waits until the processor is ready to consume messages (or is actually consuming messages)
// i.e., it is done catching up all partition tables, joins and lookup tables
func (g *Processor) WaitForReady() {
	// wait for the processor to be started (or stopped)
	select {
	case <-g.state.WaitForStateMin(ProcStateStarting):
	case <-g.done:
		return
	}
	// wait that the processor is actually running
	select {
	case <-g.state.WaitForState(ProcStateRunning):
	case <-g.done:
	}

	// wait for all partitionprocessors to be running

	// copy them first with the mutex so we don't run into a deadlock

	g.mTables.RLock()
	parts := make([]*PartitionProcessor, 0, len(g.partitions))
	for _, part := range g.partitions {
		parts = append(parts, part)
	}
	g.mTables.RUnlock()

	for _, part := range parts {
		select {
		case <-part.state.WaitForState(PPStateRunning):
		case <-g.done:
		}
	}
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (g *Processor) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	g.log.Debugf("ConsumeClaim for topic/partition %s/%d, initialOffset=%d", claim.Topic(), claim.Partition(), claim.InitialOffset())
	defer g.log.Debugf("ConsumeClaim done for topic/partition %s/%d", claim.Topic(), claim.Partition())
	part, has := g.getPartProc(claim.Partition())
	if !has {
		return fmt.Errorf("no partition (%d) to handle input in topic %s", claim.Partition(), claim.Topic())
	}

	messages := claim.Messages()
	stopping := part.stopping()

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				return nil
			}

			select {
			case part.input <- &message{
				key:       string(msg.Key),
				topic:     msg.Topic,
				offset:    msg.Offset,
				partition: msg.Partition,
				timestamp: msg.Timestamp,
				headers:   msg.Headers,
				value:     msg.Value,
			}:
			case <-stopping:
				return nil
			case <-session.Context().Done():
				return nil
			}

		case <-stopping:
			return nil
		case <-session.Context().Done():
			return nil
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
		stats *ProcessorStats
	)

	errg, ctx := multierr.NewErrGroup(ctx)

	// get partition-processor stats

	g.mTables.RLock()
	stats = newProcessorStats(len(g.partitions))

	for partID, proc := range g.partitions {
		partID, proc := partID, proc
		errg.Go(func() error {
			partStats := proc.fetchStats(ctx)

			m.Lock()
			defer m.Unlock()

			if partStats != nil {
				stats.Group[partID] = partStats
			}
			return nil
		})
	}

	// get the lookup table stats
	for topic, view := range g.lookupTables {
		topic, view := topic, view
		errg.Go(func() error {
			m.Lock()
			defer m.Unlock()
			viewStats := view.Stats(ctx)

			if viewStats != nil {
				stats.Lookup[topic] = viewStats
			}
			return nil
		})
	}
	g.mTables.RUnlock()

	err := errg.Wait().ErrorOrNil()
	if err != nil {
		g.log.Printf("Error retrieving stats: %v", err)
	}
	return stats
}

// creates the partition that is responsible for the group processor's table
func (g *Processor) createPartitionProcessor(ctx context.Context, partition int32, runMode PPRunMode, commit commitCallback) (*PartitionProcessor, error) {
	g.log.Debugf("Creating partition processor for partition %d", partition)
	if _, has := g.getPartProc(partition); has {
		return nil, fmt.Errorf("processor [%s]: partition %d already exists", g.graph.Group(), partition)
	}

	backoff, err := g.opts.builders.backoff()
	if err != nil {
		return nil, fmt.Errorf("processor [%s]: could not build backoff handler: %v", g.graph.Group(), err)
	}
	return newPartitionProcessor(partition,
		g.graph,
		commit,
		g.log,
		g.opts,
		runMode,
		g.lookupTables,
		g.saramaConsumer,
		g.producer,
		g.tmgr,
		backoff,
		g.opts.backoffResetTime), nil
}

// Stop stops the processor.
// This is semantically equivalent of closing the Context
// that was passed to Processor.Run(..).
// This method will return immediately, errors during running
// will be returned from Processor.Run(..)
func (g *Processor) Stop() {
	g.cancel()
}

// VisitAllWithStats visits all keys in parallel by passing the visit request
// to all partitions.
// The optional argument "meta" will be forwarded to the visit-function of each key of the table.
// The function returns when
// * all values have been visited or
// * the context is cancelled or
// * the processor rebalances/shuts down
// Return parameters:
// * number of visited items
// * error
func (g *Processor) VisitAllWithStats(ctx context.Context, name string, meta interface{}) (int64, error) {
	g.mTables.RLock()

	if g.isStateless() {
		return 0, fmt.Errorf("cannot visit values in stateless processor")
	}

	errg, visitCtx := multierr.NewErrGroup(ctx)
	var visited int64
	for _, part := range g.partitions {
		// we'll only do the visit for active mode partitions
		// because visit essentially provides write access, which passive partitions don't have
		if part.runMode != runModeActive {
			continue
		}
		part := part
		errg.Go(func() error {
			return part.VisitValues(visitCtx, name, meta, &visited)
		})
	}

	g.mTables.RUnlock()

	// wait for the visitors
	err := errg.Wait().ErrorOrNil()
	return visited, err
}

// VisitAll visits all values from the processor table.
func (g *Processor) VisitAll(ctx context.Context, name string, meta interface{}) error {
	_, err := g.VisitAllWithStats(ctx, name, meta)
	return err
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

	if ls := gg.LoopStream(); ls != nil {
		if err = tm.EnsureStreamExists(ls.Topic(), npar); err != nil {
			return 0, err
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
			return 0, fmt.Errorf("topics are not copartitioned! Topic '%s' does not have %d partitions like the rest but %d", topic, npar, len(partitions))
		}
	}
	return npar, nil
}

// createMessageCommitter returns a commitCallback that allows to commit a message into the passed
// sarama.ConsumerGroupSession.
//
// Note that the offset to be committed must be the offset that the consumer expects to consume next, not the offset of the message.
// See documentation at https://kafka.apache.org/25/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html which says:
//
//   Note: The committed offset should always be the offset of the next message that your application will read. Thus, when calling commitSync(offsets) you should add one to the offset of the last message processed.
//
// This has the same semantics as sarama's implementation of session.MarkMessage (which calls MarkOffset with offset+1)
func createMessageCommitter(session sarama.ConsumerGroupSession) commitCallback {
	return func(msg *message, metadata string) {
		session.MarkOffset(msg.topic, msg.partition, msg.offset+1, metadata)
	}
}
