package goka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"

	"github.com/Shopify/sarama"
)

const (
	defaultPartitionChannelSize = 10
	stallPeriod                 = 30 * time.Second
	stalledTimeout              = 2 * time.Minute
)

// partition represents one partition of a group table and handles the updates to
// this table via UpdateCallback and ProcessCallback.
//
// partition can be started in two modes:
// - catchup-mode: used by views, starts with startCatchup(), only UpdateCallback called
// - processing-mode: used by processors, starts with start(),
//                    recovers table with UpdateCallback
//                    processes input streams with ProcessCallback
//
// The partition should never be called with a closed storage proxy.
// - Before starting the partition in either way, the client must open the storage proxy.
// - A partition may be restarted even if it returned errors. Before restarting
//   it, the client must call reinit().
// - To release all resources, after stopping the partition, the client must
//   close the storage proxy.
//
type partition struct {
	log   logger.Logger
	topic string

	ch      chan kafka.Event
	st      *storageProxy
	proxy   kafkaProxy
	process processCallback

	recoveredFlag int32
	hwm           int64
	offset        int64

	recoveredOnce sync.Once

	stats         *PartitionStats
	lastStats     *PartitionStats
	requestStats  chan bool
	responseStats chan *PartitionStats
}

type kafkaProxy interface {
	Add(string, int64) error
	Remove(string) error
	AddGroup()
	Stop()
}

type processCallback func(msg *message, st storage.Storage, wg *sync.WaitGroup, pstats *PartitionStats) (int, error)

func newPartition(log logger.Logger, topic string, cb processCallback, st *storageProxy, proxy kafkaProxy, channelSize int) *partition {
	return &partition{
		log:   log,
		topic: topic,

		ch:      make(chan kafka.Event, channelSize),
		st:      st,
		proxy:   proxy,
		process: cb,

		stats:         newPartitionStats(),
		lastStats:     newPartitionStats(),
		requestStats:  make(chan bool),
		responseStats: make(chan *PartitionStats, 1),
	}
}

// reinit reinitialzes the partition to connect to Kafka and start its goroutine
func (p *partition) reinit(proxy kafkaProxy) {
	if proxy != nil {
		p.proxy = proxy
	}
}

// start loads the table partition up to HWM and then consumes streams
func (p *partition) start(ctx context.Context) error {
	defer p.proxy.Stop()
	p.stats.Table.StartTime = time.Now()

	if p.st.Stateless() {
		if err := p.markRecovered(false); err != nil {
			return fmt.Errorf("error marking stateless partition as recovered: %v", err)
		}
	} else if err := p.recover(ctx); err != nil {
		return err
	}

	// if stopped, just return
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	return p.run(ctx)
}

// startCatchup continually loads the table partition
func (p *partition) startCatchup(ctx context.Context) error {
	defer p.proxy.Stop()
	p.stats.Table.StartTime = time.Now()

	return p.catchup(ctx)
}

///////////////////////////////////////////////////////////////////////////////
// processing
///////////////////////////////////////////////////////////////////////////////
func newMessage(ev *kafka.Message) *message {
	return &message{
		Topic:     ev.Topic,
		Partition: ev.Partition,
		Offset:    ev.Offset,
		Timestamp: ev.Timestamp,
		Data:      ev.Value,
		Key:       ev.Key,
	}
}

func (p *partition) run(ctx context.Context) error {
	var wg sync.WaitGroup
	p.proxy.AddGroup()
	defer wg.Wait()

	for {
		select {
		case ev, isOpen := <-p.ch:
			// channel already closed, ev will be nil
			if !isOpen {
				return nil
			}
			switch ev := ev.(type) {
			case *kafka.Message:
				if ev.Topic == p.topic {
					return fmt.Errorf("received message from group table topic after recovery: %s", p.topic)
				}

				updates, err := p.process(newMessage(ev), p.st, &wg, p.stats)
				if err != nil {
					return fmt.Errorf("error processing message: %v", err)
				}
				p.offset += int64(updates)
				p.hwm = p.offset + 1

				// metrics
				s := p.stats.Input[ev.Topic]
				s.Count++
				s.Bytes += len(ev.Value)
				if !ev.Timestamp.IsZero() {
					s.Delay = time.Since(ev.Timestamp)
				}
				p.stats.Input[ev.Topic] = s

			case *kafka.NOP:
				// don't do anything but also don't log.
			case *kafka.EOF:
			//	if ev.Topic != p.topic {
			//		return fmt.Errorf("received EOF of topic that is not ours. This should not happend (ours=%s, received=%s)", p.topic, ev.Topic)
			//	}
			default:
				return fmt.Errorf("load: cannot handle %T = %v", ev, ev)
			}

		case <-p.requestStats:
			p.lastStats = newPartitionStats().init(p.stats, p.offset, p.hwm)
			select {
			case p.responseStats <- p.lastStats:
			case <-ctx.Done():
				return nil
			}

		case <-ctx.Done():
			return nil
		}

	}
}

///////////////////////////////////////////////////////////////////////////////
// loading storage
///////////////////////////////////////////////////////////////////////////////

func (p *partition) catchup(ctx context.Context) error {
	return p.load(ctx, true)
}

func (p *partition) recover(ctx context.Context) error {
	return p.load(ctx, false)
}

func (p *partition) recovered() bool {
	return atomic.LoadInt32(&p.recoveredFlag) == 1
}

func (p *partition) load(ctx context.Context, catchup bool) (rerr error) {
	// fetch local offset
	if local, err := p.st.GetOffset(sarama.OffsetOldest); err != nil {
		return fmt.Errorf("error reading local offset: %v", err)
	} else if err = p.proxy.Add(p.topic, local); err != nil {
		return err
	}

	defer func() {
		var derr multierr.Errors
		_ = derr.Collect(rerr)
		if e := p.proxy.Remove(p.topic); e != nil {
			_ = derr.Collect(e)
		}
		rerr = derr.NilOrError()
	}()

	stallTicker := time.NewTicker(stallPeriod)
	defer stallTicker.Stop()

	// reset stats after load
	defer p.stats.reset()

	var lastMessage time.Time
	for {
		select {
		case ev, isOpen := <-p.ch:

			// channel already closed, ev will be nil
			if !isOpen {
				return nil
			}

			switch ev := ev.(type) {
			case *kafka.BOF:
				p.hwm = ev.Hwm

				if ev.Offset == ev.Hwm {
					// nothing to recover
					if err := p.markRecovered(false); err != nil {
						return fmt.Errorf("error setting recovered: %v", err)
					}
				}

			case *kafka.EOF:
				p.offset = ev.Hwm - 1
				p.hwm = ev.Hwm

				if err := p.markRecovered(catchup); err != nil {
					return fmt.Errorf("error setting recovered: %v", err)
				}

				if catchup {
					continue
				}
				return nil

			case *kafka.Message:
				lastMessage = time.Now()
				if ev.Topic != p.topic {
					p.log.Printf("dropping message from topic = %s while loading", ev.Topic)
					continue
				}
				if err := p.storeEvent(ev); err != nil {
					return fmt.Errorf("load: error updating storage: %v", err)
				}
				p.offset = ev.Offset
				if p.offset >= p.hwm-1 {
					if err := p.markRecovered(catchup); err != nil {
						return fmt.Errorf("error setting recovered: %v", err)
					}
				}

				// update metrics
				s := p.stats.Input[ev.Topic]
				s.Count++
				s.Bytes += len(ev.Value)
				if !ev.Timestamp.IsZero() {
					s.Delay = time.Since(ev.Timestamp)
				}
				p.stats.Input[ev.Topic] = s
				p.stats.Table.Stalled = false

			case *kafka.NOP:
				// don't do anything

			default:
				return fmt.Errorf("load: cannot handle %T = %v", ev, ev)
			}

		case now := <-stallTicker.C:
			// only set to stalled, if the last message was earlier
			// than the stalled timeout
			if now.Sub(lastMessage) > stalledTimeout {
				p.stats.Table.Stalled = true
			}

		case <-p.requestStats:
			p.lastStats = newPartitionStats().init(p.stats, p.offset, p.hwm)
			select {
			case p.responseStats <- p.lastStats:
			case <-ctx.Done():
				return nil
			}

		case <-ctx.Done():
			return nil
		}
	}
}

func (p *partition) storeEvent(msg *kafka.Message) error {
	err := p.st.Update(msg.Key, msg.Value)
	if err != nil {
		return fmt.Errorf("Error from the update callback while recovering from the log: %v", err)
	}
	err = p.st.SetOffset(msg.Offset)
	if err != nil {
		return fmt.Errorf("Error updating offset in local storage while recovering from the log: %v", err)
	}
	return nil
}

// mark storage as recovered
func (p *partition) markRecovered(catchup bool) (err error) {
	p.recoveredOnce.Do(func() {
		p.lastStats = newPartitionStats().init(p.stats, p.offset, p.hwm)
		p.lastStats.Table.Status = PartitionPreparing

		var (
			done = make(chan bool)
			wg   sync.WaitGroup
		)
		if catchup {
			// if catching up (views), stop reading from topic before marking
			// partition as recovered to avoid blocking other partitions when
			// p.ch gets full
			if err = p.proxy.Remove(p.topic); err != nil {
				return
			}

			// drain events channel -- we'll fetch them again later
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-p.ch:
					case <-done:
						return
					}
				}
			}()
		}

		// mark storage as recovered -- this may take long
		if err = p.st.MarkRecovered(); err != nil {
			close(done)
			return
		}

		if catchup {
			close(done)
			wg.Wait()
			// start reading from topic again if in catchup mode
			if err = p.proxy.Add(p.topic, p.hwm); err != nil {
				return
			}
		}

		// update stats
		p.stats.Table.Status = PartitionRunning
		p.stats.Table.RecoveryTime = time.Now()

		atomic.StoreInt32(&p.recoveredFlag, 1)
	})

	// Be sure to mark partition as not stalled after EOF arrives, as
	// this will not be written in the run-method
	p.stats.Table.Stalled = false
	return
}

func (p *partition) fetchStats(ctx context.Context) *PartitionStats {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case p.requestStats <- true:
	case <-ctx.Done():
		return newPartitionStats().init(p.lastStats, p.offset, p.hwm)
	case <-timer.C:
		return p.lastStats
	}

	select {
	case s := <-p.responseStats:
		return s
	case <-ctx.Done():
		return newPartitionStats().init(p.lastStats, p.offset, p.hwm)
	}
}
