package goka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/multierr"
)

const (
	PPStateIdle State = iota
	PPStateRecovering
	PPStateRunning
	PPStateStopping
)

type PartitionProcessor struct {
	callbacks map[string]ProcessCallback

	log logger.Logger

	table   *PartitionTable
	joins   map[string]*PartitionTable
	lookups map[string]*View
	graph   *GroupGraph

	state *Signal

	partition int32

	input       chan *sarama.ConsumerMessage
	inputTopics []string

	runnerGroup       *multierr.ErrGroup
	cancelRunnerGroup func()

	consumer sarama.Consumer
	tmgr     TopicManager
	stats    *PartitionProcStats

	requestStats  chan bool
	responseStats chan *PartitionProcStats

	session  sarama.ConsumerGroupSession
	producer Producer

	opts *poptions
}

func newPartitionProcessor(partition int32,
	graph *GroupGraph,
	session sarama.ConsumerGroupSession,
	logger logger.Logger,
	opts *poptions,
	lookupTables map[string]*View,
	consumer sarama.Consumer,
	producer Producer,
	tmgr TopicManager) *PartitionProcessor {

	// collect all topics I am responsible for
	topicMap := make(map[string]bool)
	for _, stream := range graph.InputStreams() {
		topicMap[stream.Topic()] = true
	}
	if loop := graph.LoopStream(); loop != nil {
		topicMap[loop.Topic()] = true
	}

	var (
		topicList  []string
		outputList []string
		callbacks  = make(map[string]ProcessCallback)
	)
	for t := range topicMap {
		topicList = append(topicList, t)
		callbacks[t] = graph.callback(t)
	}
	for _, output := range graph.OutputStreams() {
		outputList = append(outputList, output.Topic())
	}
	if graph.LoopStream() != nil {
		outputList = append(outputList, graph.LoopStream().Topic())
	}

	if graph.GroupTable() != nil {
		outputList = append(outputList, graph.GroupTable().Topic())
	}

	log := logger.Prefix(fmt.Sprintf("PartitionProcessor (%d)", partition))

	partProc := &PartitionProcessor{
		log:           log,
		opts:          opts,
		partition:     partition,
		state:         NewSignal(PPStateIdle, PPStateRecovering, PPStateRunning, PPStateStopping).SetState(PPStateIdle),
		callbacks:     callbacks,
		lookups:       lookupTables,
		consumer:      consumer,
		producer:      producer,
		tmgr:          tmgr,
		joins:         make(map[string]*PartitionTable),
		input:         make(chan *sarama.ConsumerMessage, opts.partitionChannelSize),
		inputTopics:   topicList,
		graph:         graph,
		stats:         newPartitionProcStats(topicList, outputList),
		requestStats:  make(chan bool),
		responseStats: make(chan *PartitionProcStats, 1),
		session:       session,
	}

	if graph.GroupTable() != nil {
		partProc.table = newPartitionTable(graph.GroupTable().Topic(),
			partition,
			consumer,
			tmgr,
			opts.updateCallback,
			opts.builders.storage,
			log.Prefix("PartTable"),
		)
	}
	return partProc
}

func (pp *PartitionProcessor) EnqueueMessage(msg *sarama.ConsumerMessage) {
	pp.input <- msg
}

func (pp *PartitionProcessor) Recovered() bool {
	return pp.state.IsState(PPStateRunning)
}

func (pp *PartitionProcessor) Setup(ctx context.Context) error {
	ctx, pp.cancelRunnerGroup = context.WithCancel(ctx)

	pp.runnerGroup, _ = multierr.NewErrGroup(ctx)

	pp.state.SetState(PPStateRecovering)
	defer pp.state.SetState(PPStateRunning)

	setupErrg, setupCtx := multierr.NewErrGroup(ctx)

	if pp.table != nil {
		setupErrg.Go(func() error {
			pp.log.Printf("catching up table")
			defer pp.log.Printf("catching up table done")
			return pp.table.SetupAndCatchup(setupCtx)
		})
	}

	for _, join := range pp.graph.JointTables() {
		table := newPartitionTable(join.Topic(),
			pp.partition,
			pp.consumer, pp.tmgr, pp.opts.updateCallback, pp.opts.builders.storage, pp.opts.log)
		pp.joins[join.Topic()] = table
		setupErrg.Go(func() error {
			return pp.startJoinTable(setupCtx, table)
		})
	}

	// here we wait for our table and the joins to recover
	err := setupErrg.Wait().NilOrError()
	if err != nil {
		return fmt.Errorf("Setup failed. Cannot start processor for partition %d: %v", pp.partition, err)
	}

	// now run the processor in a runner-group
	pp.runnerGroup.Go(func() error {
		return pp.run(ctx)
	})
	return nil
}

func (pp *PartitionProcessor) startJoinTable(ctx context.Context, table *PartitionTable) error {

	recoveredChan, errorChan := table.SetupAndCatchupForever(ctx, false)

	// the join tables keep updating while we're running, so run them in the runner-group to check for errors
	pp.runnerGroup.Go(func() error {
		err, ok := <-errorChan
		if ok && err != nil {
			return fmt.Errorf("Error while setup/catchingup/updating topic/partition %s/%d: %v", table.topic, pp.partition, err)
		}
		return nil
	})

	select {
	case <-recoveredChan:
		return nil
	case <-ctx.Done():
		return nil
	}
}

func (pp *PartitionProcessor) Stop() error {
	pp.log.Printf("Stopping")
	defer pp.log.Printf("... Stopping done")
	pp.state.SetState(PPStateStopping)
	defer pp.state.SetState(PPStateIdle)
	errs := new(multierr.Errors)

	if pp.cancelRunnerGroup != nil {
		pp.cancelRunnerGroup()
	}
	if pp.runnerGroup != nil {
		errs.Collect(pp.runnerGroup.Wait().NilOrError())
	}
	if pp.table != nil {
		errs.Collect(pp.table.Close())
	}

	return errs.NilOrError()
}

func (pp *PartitionProcessor) run(ctx context.Context) error {
	pp.log.Printf("starting")
	var (
		// syncFailer is called synchronously from the callback within *this*
		// goroutine
		syncFailer = func(err error) {
			// only fail processor if context not already Done
			select {
			case <-ctx.Done():
				return
			default:
			}
			panic(err)
		}

		asyncErrors = make(chan error, pp.opts.partitionChannelSize)
		// asyncFailer is called asynchronously from other goroutines, e.g.
		// when the
		asyncFailer = func(err error) {
			asyncErrors <- err
		}

		wg sync.WaitGroup
	)
	// wait for the wg-group but with timeout in case there's a messed up race condition somewhere
	defer func() {
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.NewTimer(10 * time.Second).C:
			pp.log.Printf("partition shutdown timed out. Will stop waiting.")
		}
	}()

	updateHwmStatsTicker := time.NewTicker(statsHwmUpdateInterval)
	defer updateHwmStatsTicker.Stop()

	for {
		select {
		case ev, isOpen := <-pp.input:
			// channel already closed, ev will be nil
			if !isOpen {
				return nil
			}
			err := pp.processMessage(ctx, &wg, ev, syncFailer, asyncFailer)
			if err != nil {
				return fmt.Errorf("error processing message: %v", err)
			}

			pp.updateStats(ev)

		case <-pp.requestStats:
			stats := pp.collectStats(ctx)
			select {
			case pp.responseStats <- stats:
			case <-ctx.Done():
				pp.log.Printf("exiting, context is cancelled")
				return nil
			}
		case <-updateHwmStatsTicker.C:
			pp.updateHwmStats()

		case <-ctx.Done():
			pp.log.Printf("exiting, context is cancelled")
			return nil
		case err := <-asyncErrors:
			// close it so the other messages that might write to the channel will panic
			// TODO: is there a more elegant solution?
			close(asyncErrors)
			return fmt.Errorf("error processing a message: %v", err)
		}
	}
}

func (pp *PartitionProcessor) updateStats(ev *sarama.ConsumerMessage) {
	ip := pp.stats.Input[ev.Topic]
	ip.Bytes += len(ev.Value)
	ip.LastOffset = ev.Offset
	if !ev.Timestamp.IsZero() {
		ip.Delay = time.Since(ev.Timestamp)
	}
	ip.Count++
}

func (pp *PartitionProcessor) updateHwmStats() {
	hwms := pp.consumer.HighWaterMarks()
	for input, inputStats := range pp.stats.Input {
		hwm := hwms[input][pp.partition]
		if hwm != 0 && inputStats.LastOffset != 0 {
			inputStats.LastOffset = hwm - inputStats.LastOffset
		}
	}
}

func (pp *PartitionProcessor) collectStats(ctx context.Context) *PartitionProcStats {
	var (
		stats = pp.stats.clone()
		m     sync.Mutex
	)

	errg, ctx := multierr.NewErrGroup(ctx)

	for topic, join := range pp.joins {
		topic, join := topic, join
		errg.Go(func() error {
			joinStats := join.fetchStats(ctx)
			m.Lock()
			defer m.Unlock()
			stats.Joined[topic] = joinStats
			return nil
		})
	}

	if pp.table != nil {
		errg.Go(func() error {
			stats.TableStats = pp.table.fetchStats(ctx)
			return nil
		})
	}

	err := errg.Wait().NilOrError()
	if err != nil {
		pp.log.Printf("Error retrieving stats: %v", err)
	}

	return stats
}

func (pp *PartitionProcessor) fetchStats(ctx context.Context) *PartitionProcStats {
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(fetchStatsTimeout):
		return nil
	case pp.requestStats <- true:
	}

	// retrieve from response-channel
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(fetchStatsTimeout):
		return nil
	case stats := <-pp.responseStats:
		return stats
	}
}

func (pp *PartitionProcessor) processMessage(ctx context.Context, wg *sync.WaitGroup, msg *sarama.ConsumerMessage, syncFailer func(err error), asyncFailer func(err error)) error {
	msgContext := &cbContext{
		ctx:   ctx,
		graph: pp.graph,

		partProcStats: pp.stats,
		pviews:        pp.joins,
		views:         pp.lookups,
		wg:            wg,
		msg:           msg,
		failer:        syncFailer,
		emitter: func(topic string, key string, value []byte) *Promise {
			return pp.producer.Emit(topic, key, value).Then(func(err error) {
				if err != nil {
					asyncFailer(fmt.Errorf("error emitting message to %s (key=%s): %v", topic, key, err))
				}
			})
		},
		table: pp.table,
	}
	msgContext.commit = func() {
		// write group table offset to local storage
		if msgContext.counters.stores > 0 {
			err := msgContext.table.IncrementOffsets(int64(msgContext.counters.stores))
			if err != nil {
				asyncFailer(fmt.Errorf("error incrementing offset for %s/%d: %v", pp.graph.GroupTable().Topic(), msg.Partition, err))
				return
			}
		}

		// mark upstream offset
		pp.session.MarkMessage(msg, "")
	}

	var (
		m   interface{}
		err error
	)

	// decide whether to decode or ignore message
	switch {
	case msg.Value == nil && pp.opts.nilHandling == NilIgnore:
		// drop nil messages
		return nil
	case msg.Value == nil && pp.opts.nilHandling == NilProcess:
		// process nil messages without decoding them
		m = nil
	default:
		// get stream subcription
		codec := pp.graph.codec(msg.Topic)
		if codec == nil {
			return fmt.Errorf("cannot handle topic %s", msg.Topic)
		}

		// decode message
		m, err = codec.Decode(msg.Value)
		if err != nil {
			return fmt.Errorf("error decoding message for key %s from %s/%d: %v", msg.Key, msg.Topic, msg.Partition, err)
		}
	}

	cb := pp.callbacks[msg.Topic]
	if cb == nil {
		return fmt.Errorf("error processing message for key %s from %s/%d: %v", string(msg.Key), msg.Topic, msg.Partition, err)
	}

	// start context and call the ProcessorCallback cb
	msgContext.start()

	// call finish with the panic or with nil depending on a pending panic
	defer func() {
		if r := recover(); r != nil {
			// if handling the message panicked, we will still mark the
			// context as done, so we don't wait in the waitgroup forever
			msgContext.markDone()
			msgContext.finish(fmt.Errorf("panic: %v", r))
			panic(r) // propagate panic up
		} else {
			msgContext.finish(nil)
		}
	}()
	// now call cb
	cb(msgContext, m)
	return nil
}
