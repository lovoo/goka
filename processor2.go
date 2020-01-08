package goka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
)

const (
	ProcStateIdle State = iota
	ProcStateStarting
	ProcStateSetup
	ProcStateRunning
	ProcStateStopping
)

// Processor is a set of stateful callback functions that, on the arrival of
// messages, modify the content of a table (the group table) and emit messages into other
// topics. Messages as well as rows in the group table are key-value pairs.
// A group is composed by multiple processor instances.
type Processor2 struct {
	opts    *poptions
	log     logger.Logger
	brokers []string

	// Partition processors
	partitions map[int32]*PartitionProcessor
	// lookup tables
	lookupTables map[string]*View

	partitionCount int

	graph *GroupGraph

	saramaConsumer sarama.Consumer
	producer       kafka.Producer

	state *Signal

	errors *multierr.Errors
	cancel func()
	ctx    context.Context
}

// NewProcessor creates a processor instance in a group given the address of
// Kafka brokers, the consumer group name, a list of subscriptions (topics,
// codecs, and callbacks), and series of options.
func NewProcessor2(brokers []string, gg *GroupGraph, options ...ProcessorOption) (*Processor2, error) {
	options = append(
		// default options comes first
		[]ProcessorOption{
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
			WithViewPartitionChannelSize(opts.partitionChannelSize),
			WithViewClientID(opts.clientID),
			WithViewTopicManagerBuilder(opts.builders.topicmgr),
			WithViewStorageBuilder(opts.builders.storage),
			WithViewConsumerBuilder(opts.builders.consumer),
		)
		if err != nil {
			return nil, fmt.Errorf("error creating view: %v", err)
		}
		lookupTables[t.Topic()] = view
	}

	// combine things together
	processor := &Processor2{
		opts:    opts,
		log:     opts.log.Prefix(fmt.Sprintf("Processor %s", gg.Group())),
		brokers: brokers,

		partitions:     make(map[int32]*PartitionProcessor),
		partitionCount: npar,
		lookupTables:   lookupTables,

		graph: gg,

		state: NewSignal(ProcStateIdle, ProcStateStarting, ProcStateSetup, ProcStateRunning, ProcStateStopping).SetState(ProcStateIdle),
	}

	return processor, nil
}

// isStateless returns whether the processor is a stateless one.
func (g *Processor2) isStateless() bool {
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
func (g *Processor2) Get(key string) (interface{}, error) {
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

func (g *Processor2) find(key string) (storage.Storage, error) {
	p, err := g.hash(key)
	if err != nil {
		return nil, err
	}

	if _, ok := g.partitions[p]; !ok {
		return nil, fmt.Errorf("this processor does not contain partition %v", p)
	}

	return g.partitions[p].table.st, nil
}

func (g *Processor2) hash(key string) (int32, error) {
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

func (g *Processor2) Run(ctx context.Context) (rerr error) {
	g.log.Printf("starting")
	defer g.log.Printf("stopped")

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

	defer errors.Collect(consumerGroup.Close())

	go func() {
		for err := range consumerGroup.Errors() {
			g.log.Printf("Error executing group consumer: %v", err)
			g.errors.Collect(err)
			// TODO decide here whether we need to stop the processor or if the dispatcher takes care of
			// recreating/rebalancing.
		}
	}()

	g.saramaConsumer, err = g.opts.builders.consumerSarama(g.brokers, g.opts.clientID)
	if err != nil {
		return fmt.Errorf("Error creating consumer for brokers [%s]: %v", strings.Join(g.brokers, ","), err)
	}
	// create kafka producer
	g.log.Printf("creating producer")
	producer, err := g.opts.builders.producer(g.brokers, g.opts.clientID, g.opts.hasher)
	if err != nil {
		return fmt.Errorf(errBuildProducer, err)
	}
	g.producer = producer
	defer func() {
		g.log.Printf("closing producer")
		if err := g.producer.Close(); err != nil {
			errors.Collect(fmt.Errorf("error closing producer: %v", err))
		}
	}()

	// start all lookup tables
	for topic, view := range g.lookupTables {
		g.log.Printf("Starting lookup table for %s", topic)
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
		var topics []string
		for _, e := range g.graph.InputStreams() {
			topics = append(topics, e.Topic())
		}

		for {
			var (
				err  error
				done = make(chan struct{})
			)
			go func() {
				defer close(done)
				g.log.Printf("consuming from consumer loop")
				err = consumerGroup.Consume(ctx, topics, g)
			}()
			select {
			case <-done:
				g.log.Printf("Consumer group loop done, will stop here")
				if err != nil {
					return fmt.Errorf("error running consumergroup: %v", err)
				}
				break
			case <-ctx.Done():
				g.log.Printf("context closed, waiting for processor to finish up")
				<-done
				g.log.Printf("context closed, processor finished")
				if err != nil {
					return fmt.Errorf("error running consumergroup: %v", err)
				}
				return nil
			}
			if err != nil {
				return fmt.Errorf("error running consumergroup: %v", err)
			}
		}
	})

	return errg.Wait().NilOrError()
}

func (g *Processor2) waitForLookupTables() {

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
			g.log.Printf("Stopping to wait for views to get up, context closed")
			return
		case <-multiWait.Done():
			g.log.Printf("View catchup finished")
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

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (g *Processor2) Setup(session sarama.ConsumerGroupSession) error {
	g.state.SetState(ProcStateSetup)
	defer g.state.SetState(ProcStateRunning)
	g.log.Printf("setup generation %d", session.GenerationID())
	errs := new(multierr.Errors)
	var partitions []int32
	for _, claim := range session.Claims() {
		partitions = claim
		break
	}

	session.Claims()

	// no partitions configured, we cannot setup anything
	if len(partitions) == 0 {
		return errs.Collect(fmt.Errorf("No partitions assigned. Claims were: %#v", session.Claims())).NilOrError()
	}

	// create partition views for all partitions
	for _, partition := range partitions {
		// create partition processor for our partition
		errs.Collect(g.createPartitionProcessor(session.Context(), partition, session))
	}

	// setup all processors
	errg, _ := multierr.NewErrGroup(session.Context())
	for _, partition := range g.partitions {
		errg.Go(func() error {
			return partition.Setup(session.Context())
		})
	}

	errs.Collect(errg.Wait().NilOrError())
	return errs.NilOrError()
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (g *Processor2) Cleanup(session sarama.ConsumerGroupSession) error {
	g.state.SetState(ProcStateStopping)
	defer g.state.SetState(ProcStateIdle)
	errg, _ := multierr.NewErrGroup(session.Context())
	for part, pproc := range g.partitions {
		errg.Go(func() error {
			err := pproc.Stop()
			if err != nil {
				return fmt.Errorf("error stopping partition processor %d: %v", part, err)
			}
			return nil
		})
	}
	err := errg.Wait().NilOrError()
	g.partitions = make(map[int32]*PartitionProcessor)
	return err
}

func (g *Processor2) WaitForReady() {
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
func (g *Processor2) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	part, has := g.partitions[claim.Partition()]
	if !has {
		return fmt.Errorf("No partition (%d) to handle input in topic %s", claim.Partition(), claim.Topic())
	}
	for msg := range claim.Messages() {
		part.EnqueueMessage(msg)
	}
	return nil
}

// creates the partition that is responsible for the group processor's table
func (g *Processor2) createPartitionProcessor(ctx context.Context, partition int32, session sarama.ConsumerGroupSession) error {
	g.log.Printf("Creating partition processor for partition %d", partition)
	if _, has := g.partitions[partition]; has {
		return fmt.Errorf("processor [%s]: partition %d already exists", g.graph.Group(), partition)
	}

	pproc := newPartitionProcessor(partition, g.graph, g, session)

	g.partitions[partition] = pproc
	return nil
}

func (g *Processor2) Stop() {
	g.cancel()
}
