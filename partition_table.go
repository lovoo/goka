package goka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
)

const (
	defaultPartitionChannelSize = 10
	defaultStallPeriod          = 30 * time.Second
	defaultStalledTimeout       = 2 * time.Minute

	// internal offset we use to detect if the offset has never been stored locally
	offsetNotStored int64 = -3
)

// Backoff is used for adding backoff capabilities to the restarting
// of failing partition tables.
type Backoff interface {
	Duration() time.Duration
	Reset()
}

// PartitionTable manages the usage of a table for one partition.
// It allows to setup and recover/catchup the table contents from kafka,
// allow updates via Get/Set/Delete accessors
type PartitionTable struct {
	log            logger.Logger
	topic          string
	partition      int32
	state          *Signal
	builder        storage.Builder
	st             *storageProxy
	consumer       sarama.Consumer
	tmgr           TopicManager
	updateCallback UpdateCallback

	stats         *TableStats
	requestStats  chan bool
	responseStats chan *TableStats
	updateStats   chan func()

	offsetM sync.Mutex
	// current offset
	offset int64
	hwm    int64

	// stall config
	stallPeriod    time.Duration
	stalledTimeout time.Duration

	backoff             Backoff
	backoffResetTimeout time.Duration
}

func newPartitionTableState() *Signal {
	return NewSignal(
		State(PartitionStopped),
		State(PartitionInitializing),
		State(PartitionConnecting),
		State(PartitionRecovering),
		State(PartitionPreparing),
		State(PartitionRunning),
	).SetState(State(PartitionStopped))
}

func newPartitionTable(topic string,
	partition int32,
	consumer sarama.Consumer,
	tmgr TopicManager,
	updateCallback UpdateCallback,
	builder storage.Builder,
	log logger.Logger,
	backoff Backoff,
	backoffResetTimeout time.Duration) *PartitionTable {

	pt := &PartitionTable{
		partition:      partition,
		state:          newPartitionTableState(),
		consumer:       consumer,
		tmgr:           tmgr,
		topic:          topic,
		updateCallback: updateCallback,
		builder:        builder,
		log:            log,
		stallPeriod:    defaultStallPeriod,
		stalledTimeout: defaultStalledTimeout,

		stats:         newTableStats(),
		requestStats:  make(chan bool),
		responseStats: make(chan *TableStats, 1),
		updateStats:   make(chan func(), 10),

		backoff:             backoff,
		backoffResetTimeout: backoffResetTimeout,
	}

	return pt
}

// SetupAndRecover sets up the partition storage and recovers to HWM
func (p *PartitionTable) SetupAndRecover(ctx context.Context, restartOnError bool) error {

	err := p.setup(ctx)
	if err != nil {
		return err
	}
	// do not continue if the context is already cancelled.
	// this can happen if the context was closed during opening the storage.
	// Since this is no error we have to check it here, otherwise it'll nil-panic later.
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	if restartOnError {
		return p.loadRestarting(ctx, true)
	}
	return p.load(ctx, true)
}

// CatchupForever starts catching the partition table forever (until the context is cancelled).
// Option restartOnError allows the view to stay open/intact even in case of consumer errors
func (p *PartitionTable) CatchupForever(ctx context.Context, restartOnError bool) error {
	if restartOnError {
		return p.loadRestarting(ctx, false)
	}
	return p.load(ctx, false)
}

func (p *PartitionTable) loadRestarting(ctx context.Context, stopAfterCatchup bool) error {
	var (
		resetTimer *time.Timer
		retries    int
	)

	for {
		err := p.load(ctx, stopAfterCatchup)
		if err != nil {
			p.log.Printf("Error while starting up: %v", err)

			retries++
			if resetTimer != nil {
				resetTimer.Stop()
			}
			resetTimer = time.AfterFunc(p.backoffResetTimeout, func() {
				p.backoff.Reset()
				retries = 0
			})
		} else {
			return nil
		}

		retryDuration := p.backoff.Duration()
		p.log.Printf("Will retry in %.0f seconds (retried %d times so far)", retryDuration.Seconds(), retries)
		select {
		case <-ctx.Done():
			return nil

		case <-time.After(retryDuration):
		}
	}
}

// Setup creates the storage for the partition table
func (p *PartitionTable) setup(ctx context.Context) error {
	p.state.SetState(State(PartitionInitializing))
	storage, err := p.createStorage(ctx)
	if err != nil {
		p.state.SetState(State(PartitionStopped))
		return fmt.Errorf("error setting up partition table: %v", err)
	}

	p.st = storage
	return nil
}

// Close closes the partition table
func (p *PartitionTable) Close() error {
	if p.st != nil {
		return p.st.Close()
	}
	return nil
}

func (p *PartitionTable) createStorage(ctx context.Context) (*storageProxy, error) {
	var (
		err  error
		st   storage.Storage
		done = make(chan struct{})
	)
	start := time.Now()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	go func() {
		defer close(done)
		st, err = p.builder(p.topic, p.partition)
		if err != nil {
			return
		}
		err = st.Open()
	}()

WaitLoop:
	for {
		select {
		case <-ctx.Done():
			return nil, nil
		case <-ticker.C:
			p.log.Printf("creating storage for topic %s/%d for %.1f minutes ...", p.topic, p.partition, time.Since(start).Minutes())
		case <-done:
			p.log.Debugf("finished building storage for topic %s/%d in %.1f minutes", p.topic, p.partition, time.Since(start).Minutes())
			if err != nil {
				return nil, fmt.Errorf("error building storage: %v", err)
			}
			break WaitLoop
		}
	}

	return &storageProxy{
		Storage:   st,
		partition: p.partition,
		update:    p.updateCallback,
	}, nil

}

// findOffsetToLoad returns the first offset to load and the high watermark.
func (p *PartitionTable) findOffsetToLoad(storedOffset int64) (int64, int64, error) {
	oldest, err := p.tmgr.GetOffset(p.topic, p.partition, sarama.OffsetOldest)
	if err != nil {
		return 0, 0, fmt.Errorf("Error getting oldest offset for topic/partition %s/%d: %v", p.topic, p.partition, err)
	}
	hwm, err := p.tmgr.GetOffset(p.topic, p.partition, sarama.OffsetNewest)
	if err != nil {
		return 0, 0, fmt.Errorf("Error getting newest offset for topic/partition %s/%d: %v", p.topic, p.partition, err)
	}
	p.log.Debugf("topic manager gives us oldest: %d, hwm: %d", oldest, hwm)

	var start int64

	// if no offset is found in the local storage start with the oldest offset known
	// to kafka.
	// Otherwise start with the next element not stored locally.
	if storedOffset == offsetNotStored {
		start = oldest
	} else {
		start = storedOffset + 1
	}

	// if kafka does not have the offset we're looking for, use the oldest kafka has
	// This can happen when the log compaction removes offsets that we stored.
	if start < oldest {
		start = oldest
	}
	return start, hwm, nil
}

func (p *PartitionTable) load(ctx context.Context, stopAfterCatchup bool) (rerr error) {
	var (
		storedOffset int64
		partConsumer sarama.PartitionConsumer
		err          error
		errs         = new(multierr.Errors)
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// deferred error handling
	defer func() {
		errs.Collect(rerr)

		rerr = errs.NilOrError()
		return
	}()

	p.state.SetState(State(PartitionConnecting))

	// fetch local offset
	storedOffset, err = p.st.GetOffset(offsetNotStored)
	if err != nil {
		errs.Collect(fmt.Errorf("error reading local offset: %v", err))
		return
	}

	loadOffset, hwm, err := p.findOffsetToLoad(storedOffset)
	if err != nil {
		errs.Collect(err)
		return
	}

	if storedOffset > 0 && hwm == 0 {
		errs.Collect(fmt.Errorf("kafka tells us there's no message in the topic, but our cache has one. The table might be gone. Try to delete your local cache! Topic %s, partition %d, hwm %d, local offset %d", p.topic, p.partition, hwm, storedOffset))
		return
	}

	if storedOffset >= hwm {
		p.log.Printf("Error: local offset is higher than partition offset. topic %s, partition %d, hwm %d, local offset %d. This can have several reasons: \n(1) The kafka topic storing the table is gone --> delete the local cache and restart! \n(2) the processor crashed last time while writing to disk. \n(3) You found a bug!", p.topic, p.partition, hwm, storedOffset)

		// we'll just pretend we were done so the partition looks recovered
		loadOffset = hwm
	}

	// initialize recovery stats here, in case we don't do the recovery because
	// we're up to date already
	if stopAfterCatchup {
		p.enqueueStatsUpdate(ctx, func() {
			p.stats.Recovery.StartTime = time.Now()
			p.stats.Recovery.Hwm = hwm
			p.stats.Recovery.Offset = loadOffset
		})
	}

	// we are exactly where we're supposed to be
	// AND we're here for catchup, so let's stop here
	// and do not attempt to load anything
	if stopAfterCatchup && loadOffset >= hwm {
		errs.Collect(p.markRecovered(ctx))
		return
	}

	if stopAfterCatchup {
		p.log.Debugf("Recovering from %d to hwm=%d; (local offset is %d)", loadOffset, hwm, storedOffset)
	} else {
		p.log.Debugf("Catching up from %d to hwm=%d; (local offset is %d)", loadOffset, hwm, storedOffset)
	}

	defer p.log.Debugf("... Loading done")

	partConsumer, err = p.consumer.ConsumePartition(p.topic, p.partition, loadOffset)
	if err != nil {
		errs.Collect(fmt.Errorf("Error creating partition consumer for topic %s, partition %d, offset %d: %v", p.topic, p.partition, storedOffset, err))
		return
	}

	// consume errors asynchronously
	go p.handleConsumerErrors(ctx, errs, partConsumer)

	// close the consumer
	defer func() {
		partConsumer.AsyncClose()
		p.drainConsumer(partConsumer, errs)
	}()

	if stopAfterCatchup {
		p.state.SetState(State(PartitionRecovering))
	} else {
		p.state.SetState(State(PartitionRunning))
	}

	// load messages and stop when you're at HWM
	loadErr := p.loadMessages(ctx, partConsumer, hwm, stopAfterCatchup)

	if loadErr != nil {
		errs.Collect(loadErr)
		return
	}

	if stopAfterCatchup {
		errs.Collect(p.markRecovered(ctx))

		now := time.Now()
		p.enqueueStatsUpdate(ctx, func() { p.stats.Recovery.RecoveryTime = now })
	}
	return
}

func (p *PartitionTable) observeStateChanges() *StateChangeObserver {
	return p.state.ObserveStateChange()
}

func (p *PartitionTable) markRecovered(ctx context.Context) error {
	var (
		start  = time.Now()
		ticker = time.NewTicker(10 * time.Second)
		done   = make(chan error, 1)
	)
	defer ticker.Stop()

	p.state.SetState(State(PartitionPreparing))
	now := time.Now()
	p.enqueueStatsUpdate(ctx, func() { p.stats.Recovery.RecoveryTime = now })

	go func() {
		defer close(done)
		err := p.st.MarkRecovered()
		if err != nil {
			done <- err
		}
	}()

	for {
		select {
		case <-ticker.C:
			p.log.Printf("Committing storage after recovery for topic/partition %s/%d since %0.f seconds", p.topic, p.partition, time.Since(start).Seconds())
		case <-ctx.Done():
			return nil
		case err := <-done:
			if err != nil {
				return err
			}
			p.state.SetState(State(PartitionRunning))
			return nil
		}
	}
}

func (p *PartitionTable) handleConsumerErrors(ctx context.Context, errs *multierr.Errors, cons sarama.PartitionConsumer) {
	for {
		select {
		case consError, ok := <-cons.Errors():
			if !ok {
				return
			}
			err := fmt.Errorf("Consumer error: %v", consError)
			p.log.Printf("%v", err)
			errs.Collect(err)
			// if there's an error, close the consumer
			cons.AsyncClose()
		case <-ctx.Done():
			return
		}
	}
}

func (p *PartitionTable) drainConsumer(cons sarama.PartitionConsumer, errs *multierr.Errors) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errg, ctx := multierr.NewErrGroup(ctx)

	// drain errors channel
	errg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				p.log.Printf("draining errors channel timed out")
				return nil
			case err, ok := <-cons.Errors():
				if !ok {
					return nil
				}
				errs.Collect(err)
			}
			return nil
		}
	})

	// drain message channel
	errg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				p.log.Printf("draining messages channel timed out")
				return nil
			case _, ok := <-cons.Messages():
				if !ok {
					return nil
				}
			}
			return nil
		}
	})

	errg.Wait()
}

func (p *PartitionTable) loadMessages(ctx context.Context, cons sarama.PartitionConsumer, partitionHwm int64, stopAfterCatchup bool) (rerr error) {
	errs := new(multierr.Errors)

	// deferred error handling
	defer func() {
		errs.Collect(rerr)

		rerr = errs.NilOrError()
		return
	}()

	stallTicker := time.NewTicker(p.stallPeriod)
	defer stallTicker.Stop()

	lastMessage := time.Now()

	for {
		select {
		case msg, ok := <-cons.Messages():
			if !ok {
				return
			}

			// This case is for the Tester to achieve synchronity.
			// Nil messages are never generated by the Sarama Consumer
			if msg == nil {
				continue
			}

			if p.state.IsState(State(PartitionRunning)) && stopAfterCatchup {
				// TODO: should we really ignore the message?
				// Shouldn't we instead break here to avoid losing messages or fail or just consume it?
				p.log.Printf("received message in topic %s, partition %s after catchup. Another processor is still producing messages. Ignoring message.", p.topic, p.partition)
				continue
			}

			lastMessage = time.Now()
			if err := p.storeEvent(string(msg.Key), msg.Value, msg.Offset); err != nil {
				errs.Collect(fmt.Errorf("load: error updating storage: %v", err))
				return
			}

			if stopAfterCatchup {
				p.enqueueStatsUpdate(ctx, func() { p.stats.Recovery.Offset = msg.Offset })
			}

			p.enqueueStatsUpdate(ctx, func() { p.trackIncomingMessageStats(msg) })

			if stopAfterCatchup && msg.Offset >= partitionHwm-1 {
				return
			}

		case now := <-stallTicker.C:
			// only set to stalled, if the last message was earlier
			// than the stalled timeout
			if now.Sub(lastMessage) > p.stalledTimeout {
				p.enqueueStatsUpdate(ctx, func() { p.stats.Stalled = true })
			}

		case <-ctx.Done():
			return
		}
	}
}

func (p *PartitionTable) enqueueStatsUpdate(ctx context.Context, updater func()) {
	select {
	case p.updateStats <- updater:
	case <-ctx.Done():
	default:
		// going to default indicates the updateStats channel is not read, so so the stats
		// loop is not actually running.
		// We must not block here, so we'll skip the update
	}
}

// RunStatsLoop starts the handler for stats requests. This loop runs detached from the
// recover/catchup mechanism so clients can always request stats even if the partition table is not
// running (like a processor table after it's recovered).
func (p *PartitionTable) RunStatsLoop(ctx context.Context) {

	updateHwmStatsTicker := time.NewTicker(statsHwmUpdateInterval)
	defer updateHwmStatsTicker.Stop()
	for {
		select {
		case <-p.requestStats:
			p.handleStatsRequest(ctx)
		case update := <-p.updateStats:
			update()
		case <-updateHwmStatsTicker.C:
			p.updateHwmStats()
		case <-ctx.Done():
			return
		}
	}
}

func (p *PartitionTable) handleStatsRequest(ctx context.Context) {
	stats := p.stats.clone()
	stats.Status = PartitionStatus(p.state.State())
	select {
	case p.responseStats <- stats:
	case <-ctx.Done():
		p.log.Debugf("exiting, context is cancelled")
	}
}

func (p *PartitionTable) fetchStats(ctx context.Context) *TableStats {
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(fetchStatsTimeout):
		p.log.Printf("requesting stats timed out")
		return nil
	case p.requestStats <- true:
	}

	// retrieve from response-channel
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(fetchStatsTimeout):
		p.log.Printf("fetching stats timed out")
		return nil
	case stats := <-p.responseStats:
		return stats
	}
}

func (p *PartitionTable) trackIncomingMessageStats(msg *sarama.ConsumerMessage) {
	ip := p.stats.Input
	ip.Bytes += len(msg.Value)
	ip.LastOffset = msg.Offset
	if !msg.Timestamp.IsZero() {
		ip.Delay = time.Since(msg.Timestamp)
	}
	ip.Count++
	p.stats.Stalled = false
}

// TrackMessageWrite updates the write stats to passed length
func (p *PartitionTable) TrackMessageWrite(ctx context.Context, length int) {
	p.enqueueStatsUpdate(ctx, func() {
		p.stats.Writes.Bytes += length
		p.stats.Writes.Count++
	})
}

func (p *PartitionTable) updateHwmStats() {
	hwms := p.consumer.HighWaterMarks()
	hwm := hwms[p.topic][p.partition]
	if hwm != 0 {
		p.stats.Input.OffsetLag = hwm - p.stats.Input.LastOffset
	}
}

func (p *PartitionTable) storeEvent(key string, value []byte, offset int64) error {
	err := p.st.Update(key, value)
	if err != nil {
		return fmt.Errorf("Error from the update callback while recovering from the log: %v", err)
	}
	err = p.st.SetOffset(offset)
	if err != nil {
		return fmt.Errorf("Error updating offset in local storage while recovering from the log: %v", err)
	}
	return nil
}

// IsRecovered returns whether the partition table is recovered
func (p *PartitionTable) IsRecovered() bool {
	return p.state.IsState(State(PartitionRunning))
}

// CurrentState returns the partition's current status
func (p *PartitionTable) CurrentState() PartitionStatus {
	return PartitionStatus(p.state.State())
}

// WaitRecovered returns a channel that closes when the partition table enters state `PartitionRunning`
func (p *PartitionTable) WaitRecovered() chan struct{} {
	return p.state.WaitForState(State(PartitionRunning))
}

// Get returns the value for passed key
func (p *PartitionTable) Get(key string) ([]byte, error) {
	if !p.state.IsState(State(PartitionRunning)) {
		return nil, fmt.Errorf("Partition is not running so it's not safe to read values")
	}
	return p.st.Get(key)
}

// Has returns whether the storage contains passed key
func (p *PartitionTable) Has(key string) (bool, error) {
	if !p.state.IsState(State(PartitionRunning)) {
		return false, fmt.Errorf("Partition is not running so it's not safe to read values")
	}
	return p.st.Has(key)
}

// Set sets a key value key in the partition table by modifying the underlying storage
func (p *PartitionTable) Set(key string, value []byte) error {
	return p.st.Set(key, value)
}

// Delete removes the passed key from the partition table by deleting from the underlying storage
func (p *PartitionTable) Delete(key string) error {
	return p.st.Delete(key)
}

func (p *PartitionTable) storeNewestOffset(newOffset int64) error {
	p.offsetM.Lock()
	defer p.offsetM.Unlock()

	oldOffset, err := p.GetOffset(offsetNotStored)
	if err != nil {
		return err
	}

	if offsetNotStored != oldOffset && oldOffset <= newOffset {
		return p.SetOffset(newOffset)
	}
	return nil
}

// SetOffset sets the magic offset value in storage
func (p *PartitionTable) SetOffset(value int64) error {
	return p.st.SetOffset(value)
}

// GetOffset returns the magic offset value from storage
func (p *PartitionTable) GetOffset(defValue int64) (int64, error) {
	return p.st.GetOffset(defValue)
}
