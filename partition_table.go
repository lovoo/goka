package goka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
)

type PartitionTable struct {
	log            logger.Logger
	topic          string
	partition      int32
	state          *Signal
	stats          *TableStats
	builder        storage.Builder
	st             *storageProxy
	consumer       sarama.Consumer
	tmgr           kafka.TopicManager
	updateCallback UpdateCallback

	offsetM sync.Mutex
	// current offset
	offset int64
	hwm    int64
}

func newPartitionTable(topic string,
	partition int32,
	consumer sarama.Consumer,
	tmgr kafka.TopicManager,
	updateCallback UpdateCallback,
	builder storage.Builder,
	log logger.Logger) *PartitionTable {
	return &PartitionTable{
		state:          NewSignal(State(PartitionRecovering), State(PartitionPreparing), State(PartitionRunning)),
		stats:          newTableStats(),
		consumer:       consumer,
		tmgr:           tmgr,
		topic:          topic,
		updateCallback: updateCallback,
		builder:        builder,
		log:            log,
	}
}

func (p *PartitionTable) SetupAndCatchup(ctx context.Context) error {
	err := p.Setup(ctx)
	if err != nil {
		return err
	}
	return p.catchupToHwm(ctx)
}

func (p *PartitionTable) SetupAndCatchupForever(ctx context.Context, restartOnError bool) (chan struct{}, chan error, error) {
	err := p.Setup(ctx)
	if err != nil {
		return nil, nil, err
	}

	errChan := make(chan error)

	if restartOnError {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case err, ok := <-errChan:
					if ok {
						p.log.Printf("Error while catching up, but we'll try to keep it running: %v", err)
					}
				}
			}
		}()
	} else {
		go func() {
			errChan <- p.catchupForever(ctx)
		}()
	}

	return p.WaitRecovered(), errChan, nil
}

// Setup creates the storage for the partition table
func (p *PartitionTable) Setup(ctx context.Context) error {

	storage, err := p.createStorage(ctx)
	if err != nil {
		return fmt.Errorf("error setting up partition table: %v", err)
	}

	p.st = storage
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
			return nil, nil
		case <-ticker.C:
			p.log.Printf("creating storage for topic %s for %.1f minutes ...", p.topic, time.Since(start).Minutes())
		case <-done:
			p.log.Printf("finished building storage for topic %s", p.topic)
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

func (p *PartitionTable) findOffsetToLoad(localOffset int64) (int64, int64, error) {
	oldest, err := p.tmgr.GetOffset(p.topic, p.partition, sarama.OffsetOldest)
	if err != nil {
		return 0, 0, fmt.Errorf("Error getting oldest offset for topic/partition %s/%d: %v", p.topic, p.partition, err)
	}
	hwm, err := p.tmgr.GetOffset(p.topic, p.partition, sarama.OffsetNewest)
	if err != nil {
		return 0, 0, fmt.Errorf("Error getting newest offset for topic/partition %s/%d: %v", p.topic, p.partition, err)
	}

	start := localOffset

	if localOffset == sarama.OffsetOldest {
		start = oldest
	} else if localOffset == sarama.OffsetNewest {
		start = hwm
	}

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
		partitionHwm int64
		partConsumer sarama.PartitionConsumer
		err          error
		errs         = new(multierr.Errors)
	)

	p.log.Printf("Loading ...")
	defer p.log.Printf("... Loading done")

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

	loadOffset, hwm, err := p.findOffsetToLoad(localOffset)
	if err != nil {
		errs.Collect(err)
		return
	}

	if localOffset >= hwm {
		errs.Collect(fmt.Errorf("local offset is higher than partition offset. topic %s, partition %d, hwm %d, local offset %d", p.topic, p.partition, partitionHwm, localOffset))
		return
	}

	// we are exactly where we're supposed to be
	// AND we're here for catchup, so let's stop here
	// and do not attempt to load anything
	if stopAfterCatchup && loadOffset == hwm {
		return nil
	}

	partConsumer, err = p.consumer.ConsumePartition(p.topic, p.partition, loadOffset)
	if err != nil {
		errs.Collect(fmt.Errorf("Error creating partition consumer for topic %s, partition %d, offset %d: %v", p.topic, p.partition, localOffset, err))
		return
	}

	// reset stats after load
	defer p.stats.reset()

	// consume errors asynchronously
	go func() {
		err := p.collectConsumerErrors(ctx, partConsumer)
		errs.Collect(err)
	}()

	// load messages and stop when you're at HWM
	loadErr := p.loadMessages(ctx, partConsumer, partitionHwm, stopAfterCatchup)

	// close the consumer
	errs.Collect(partConsumer.Close())

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
	defer p.state.SetState(State(PartitionRunning))

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
		case err, ok := <-done:
			if !ok {
				return nil
			}

			return err
		}
	}
}

func (p *PartitionTable) collectConsumerErrors(ctx context.Context, cons sarama.PartitionConsumer) error {
	// internally collect all errors
	var errs = new(multierr.Errors)

ErrLoop:
	for {
		select {
		case consError, ok := <-cons.Errors():
			if !ok {
				break
			}
			p.log.Printf("Consumer error for table/partition %s/%d: %v", p.topic, p.partition, consError)
			errs.Collect(consError)
		case <-ctx.Done():
			break ErrLoop
		}
	}
	return errs.NilOrError()
}

func (p *PartitionTable) loadMessages(ctx context.Context, cons sarama.PartitionConsumer, partitionHwm int64, stopAfterCatchup bool) (rerr error) {

	errs := new(multierr.Errors)

	// deferred error handling
	defer func() {
		errs.Collect(rerr)

		rerr = errs.NilOrError()
		return
	}()

	stallTicker := time.NewTicker(stallPeriod)
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
			if msg.Topic != p.topic {
				errs.Collect(fmt.Errorf("Got unexpected topic %s, require %s", msg.Topic, p.topic))
				return
			}
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
			if now.Sub(lastMessage) > stalledTimeout {
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
