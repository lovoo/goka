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
)

// PartitionTable manages the usage of a table for one partition.
// It allows to setup and recover/catchup the table contents from kafka,
// allow updates via Get/Set/Delete accessors
type PartitionTable struct {
	log            logger.Logger
	topic          string
	partition      int32
	state          *Signal
	stats          *TableStats
	builder        storage.Builder
	st             *storageProxy
	consumer       sarama.Consumer
	tmgr           TopicManager
	updateCallback UpdateCallback

	offsetM sync.Mutex
	// current offset
	offset int64
	hwm    int64

	// stall config
	stallPeriod    time.Duration
	stalledTimeout time.Duration
}

func newPartitionTable(topic string,
	partition int32,
	consumer sarama.Consumer,
	tmgr TopicManager,
	updateCallback UpdateCallback,
	builder storage.Builder,
	log logger.Logger) *PartitionTable {
	return &PartitionTable{
		partition:      partition,
		state:          NewSignal(State(PartitionRecovering), State(PartitionPreparing), State(PartitionRunning)),
		stats:          newTableStats(),
		consumer:       consumer,
		tmgr:           tmgr,
		topic:          topic,
		updateCallback: updateCallback,
		builder:        builder,
		log:            log,
		stallPeriod:    defaultStallPeriod,
		stalledTimeout: defaultStalledTimeout,
	}
}

func (p *PartitionTable) SetupAndCatchup(ctx context.Context) error {
	err := p.setup(ctx)
	if err != nil {
		return err
	}
	return p.catchupToHwm(ctx)
}

func (p *PartitionTable) SetupAndCatchupForever(ctx context.Context, restartOnError bool) (chan struct{}, chan error) {
	errChan := make(chan error)
	err := p.setup(ctx)
	if err != nil {
		go func() {
			defer close(errChan)
			errChan <- err
		}()
		return p.WaitRecovered(), errChan
	}

	if restartOnError {
		go func() {
			errChanIn := make(chan error)
			defer close(errChan)
			defer close(errChanIn)
			for {
				cCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				go func(ctx context.Context) {
					errChanIn <- p.catchupForever(ctx)
				}(cCtx)
				select {
				case <-ctx.Done():
					return
				case err, ok := <-errChanIn:
					if ok {
						p.log.Printf("Error while catching up, but we'll try to keep it running: %v", err)
					}
					cancel()
				}
			}
		}()
	} else {
		go func() {
			defer close(errChan)
			errChan <- p.catchupForever(ctx)
		}()
	}

	return p.WaitRecovered(), errChan
}

// Setup creates the storage for the partition table
func (p *PartitionTable) setup(ctx context.Context) error {
	storage, err := p.createStorage(ctx)
	if err != nil {
		return fmt.Errorf("error setting up partition table: %v", err)
	}

	p.st = storage
	return nil
}

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
	}()

WaitLoop:
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context canceled")
		case <-ticker.C:
			p.log.Printf("creating storage for topic %s/%d for %.1f minutes ...", p.topic, p.partition, time.Since(start).Minutes())
		case <-done:
			p.log.Printf("finished building storage for topic %s/%d", p.topic, p.partition)
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

// start loads the table partition up to HWM and then consumes streams
func (p *PartitionTable) catchupToHwm(ctx context.Context) error {
	p.stats.StartTime = time.Now()
	// catchup until hwm
	return p.load(ctx, true)
}

// continue
func (p *PartitionTable) catchupForever(ctx context.Context) error {
	p.stats.StartTime = time.Now()
	err := p.load(ctx, true)
	if err != nil {
		return fmt.Errorf("Error catching up: %v", err)
	}

	return p.load(ctx, false)
}

// TODO(jb): refactor comment
// findOffsetToLoad returns the first and the last offset (hwm) to load.
// If localOffset is sarama.OffsetOldest the oldest offset known to kafka is returned as first offset.
// If localOffset is sarama.OffsetNewest the hwm is returned as first offset.
// If localOffset is higher than the hwm, the hwm is returned as first offset.
// If localOffset is lower than the oldest offset, the oldest offset is returned as first offset.
func (p *PartitionTable) findOffsetToLoad(localOffset int64) (int64, int64, error) {
	oldest, err := p.tmgr.GetOffset(p.topic, p.partition, sarama.OffsetOldest)
	if err != nil {
		return 0, 0, fmt.Errorf("Error getting oldest offset for topic/partition %s/%d: %v", p.topic, p.partition, err)
	}
	hwm, err := p.tmgr.GetOffset(p.topic, p.partition, sarama.OffsetNewest)
	if err != nil {
		return 0, 0, fmt.Errorf("Error getting newest offset for topic/partition %s/%d: %v", p.topic, p.partition, err)
	}

	p.log.Printf("topic manager gives us oldest: %d, hwm: %d", oldest, hwm)

	start := localOffset

	if localOffset == sarama.OffsetOldest {
		start = oldest
	} else if localOffset == sarama.OffsetNewest {
		start = hwm
	}

	// TODO(jb): check if thats not an error case (local > hwm)
	if start > hwm {
		start = hwm
	}
	if start < oldest {
		start = oldest
	}
	return start, hwm, nil
}

func (p *PartitionTable) load(ctx context.Context, stopAfterCatchup bool) (rerr error) {
	var (
		localOffset  int64
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

	// fetch local offset
	localOffset, err = p.st.GetOffset(sarama.OffsetOldest)
	if err != nil {
		errs.Collect(fmt.Errorf("error reading local offset: %v", err))
		return
	}

	p.log.Printf("Offset stored locally: %d", localOffset)

	loadOffset, hwm, err := p.findOffsetToLoad(localOffset)
	if err != nil {
		errs.Collect(err)
		return
	}

	if localOffset >= hwm {
		errs.Collect(fmt.Errorf("local offset is higher than partition offset. topic %s, partition %d, hwm %d, local offset %d", p.topic, p.partition, hwm, localOffset))
		return
	}

	// we are exactly where we're supposed to be
	// AND we're here for catchup, so let's stop here
	// and do not attempt to load anything
	if stopAfterCatchup && loadOffset >= hwm-1 {
		errs.Collect(p.markRecovered(ctx))
		return nil
	}

	p.log.Printf("Loading partition table from %d (hwm=%d)", loadOffset, hwm)
	defer p.log.Printf("... Loading done")

	partConsumer, err = p.consumer.ConsumePartition(p.topic, p.partition, loadOffset)
	if err != nil {
		errs.Collect(fmt.Errorf("Error creating partition consumer for topic %s, partition %d, offset %d: %v", p.topic, p.partition, localOffset, err))
		return
	}
	// close the consumer
	defer func() {
		errs.Collect(partConsumer.Close())
	}()

	// reset stats after load
	defer p.stats.reset()

	// consume errors asynchronously
	go p.handleConsumerErrors(ctx, errs, partConsumer)

	// load messages and stop when you're at HWM
	loadErr := p.loadMessages(ctx, partConsumer, hwm, stopAfterCatchup)

	if loadErr != nil {
		errs.Collect(loadErr)
		return
	}

	if stopAfterCatchup {
		errs.Collect(p.markRecovered(ctx))
	}
	return
}

func (p *PartitionTable) markRecovered(ctx context.Context) error {
	var (
		start  = time.Now()
		ticker = time.NewTicker(10 * time.Second)
		done   = make(chan error, 1)
	)
	defer ticker.Stop()

	p.state.SetState(State(PartitionPreparing))

	go func() {
		defer close(done)
		done <- p.st.MarkRecovered()
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
		case <-ctx.Done():
			return
		}
	}
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
			p.offset = msg.Offset

			if stopAfterCatchup && msg.Offset >= partitionHwm-1 {
				return
			}

			// update metrics
			s := p.stats.Input[msg.Topic]
			s.Count++
			s.Bytes += len(msg.Value)
			if !msg.Timestamp.IsZero() {
				s.Delay = time.Since(msg.Timestamp)
			}
			p.stats.Input[msg.Topic] = s
			p.stats.Stalled = false
		case now := <-stallTicker.C:
			// only set to stalled, if the last message was earlier
			// than the stalled timeout
			if now.Sub(lastMessage) > p.stalledTimeout {
				p.stats.Stalled = true
			}

		case <-ctx.Done():
			return
		}
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

func (p *PartitionTable) IsRecovered() bool {
	return p.state.IsState(State(PartitionRunning))
}

func (p *PartitionTable) WaitRecovered() chan struct{} {
	return p.state.WaitForState(State(PartitionRunning))
}

func (p *PartitionTable) Get(key string) ([]byte, error) {
	return p.st.Get(key)
}
func (p *PartitionTable) Set(key string, value []byte) error {
	return p.st.Set(key, value)
}
func (p *PartitionTable) Delete(key string) error {
	return p.st.Delete(key)
}
func (p *PartitionTable) IncrementOffsets(increment int64) error {
	p.offsetM.Lock()
	defer p.offsetM.Unlock()

	offset, err := p.GetOffset(0)
	if err != nil {
		return err
	}

	return p.SetOffset(offset + increment)
}

func (p *PartitionTable) SetOffset(value int64) error {
	return p.st.SetOffset(value)
}
func (p *PartitionTable) GetOffset(defValue int64) (int64, error) {
	return p.st.GetOffset(defValue)
}
