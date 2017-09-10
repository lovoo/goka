package goka

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/storage"

	"github.com/Shopify/sarama"
)

const (
	defaultPartitionChannelSize = 10
	stallPeriod                 = 30 * time.Second
	stalledTimeout              = 2 * time.Minute
)

type partition struct {
	log   logger.Logger
	topic string

	ch      chan kafka.Event
	st      *storageProxy
	proxy   kafkaProxy
	process processCallback

	dying    chan bool
	done     chan bool
	stopFlag int64

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
	Add(string, int64)
	Remove(string)
	AddGroup()
	Stop()
}

type processCallback func(msg *message, st storage.Storage, wg *sync.WaitGroup, pstats *PartitionStats) (int, error)

func newPartition(log logger.Logger, topic string, cb processCallback, st *storageProxy, proxy kafkaProxy, channelSize int) *partition {
	return &partition{
		log:   log,
		topic: topic,

		ch:    make(chan kafka.Event, channelSize),
		dying: make(chan bool),
		done:  make(chan bool),

		st:            st,
		recoveredOnce: sync.Once{},
		proxy:         proxy,
		process:       cb,

		stats:         newPartitionStats(),
		lastStats:     newPartitionStats(),
		requestStats:  make(chan bool),
		responseStats: make(chan *PartitionStats, 1),
	}
}

func (p *partition) start() error {
	defer close(p.done)
	defer p.proxy.Stop()
	p.stats.Table.StartTime = time.Now()

	if !p.st.Stateless() {
		err := p.st.Open()
		if err != nil {
			return err
		}
		defer p.st.Close()

		if err := p.recover(); err != nil {
			return err
		}
	} else {
		p.markRecovered(false)
	}

	// if stopped, just return
	if atomic.LoadInt64(&p.stopFlag) == 1 {
		return nil
	}
	return p.run()
}

func (p *partition) startCatchup() error {
	defer close(p.done)
	defer p.proxy.Stop()
	p.stats.Table.StartTime = time.Now()

	err := p.st.Open()
	if err != nil {
		return err
	}
	defer p.st.Close()

	return p.catchup()
}

func (p *partition) stop() {
	atomic.StoreInt64(&p.stopFlag, 1)
	close(p.dying)
	<-p.done
	close(p.ch)
}

///////////////////////////////////////////////////////////////////////////////
// processing
///////////////////////////////////////////////////////////////////////////////
func newMessage(ev *kafka.Message) *message {
	return &message{
		Topic:     string(ev.Topic),
		Partition: int32(ev.Partition),
		Offset:    int64(ev.Offset),
		Timestamp: ev.Timestamp,
		Data:      ev.Value,
		Key:       string(ev.Key),
	}
}

func (p *partition) run() error {
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
			case <-p.dying:
				return nil
			}

		case <-p.dying:
			return nil
		}

	}
}

///////////////////////////////////////////////////////////////////////////////
// loading storage
///////////////////////////////////////////////////////////////////////////////

func (p *partition) catchup() error {
	return p.load(true)
}

func (p *partition) recover() error {
	return p.load(false)
}

func (p *partition) recovered() bool {
	return atomic.LoadInt32(&p.recoveredFlag) == 1
}

func (p *partition) load(catchup bool) error {
	// fetch local offset
	local, err := p.st.GetOffset(sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("Error reading local offset: %v", err)
	}
	p.proxy.Add(p.topic, local)
	defer p.proxy.Remove(p.topic)

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
					return fmt.Errorf("load: wrong topic = %s", ev.Topic)
				}
				err := p.storeEvent(ev)
				if err != nil {
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
			case <-p.dying:
				return nil
			}

		case <-p.dying:
			return nil
		}
	}
}

func (p *partition) storeEvent(msg *kafka.Message) error {
	err := p.st.Update(msg.Key, msg.Value)
	if err != nil {
		return fmt.Errorf("Error from the update callback while recovering from the log: %v", err)
	}
	err = p.st.SetOffset(int64(msg.Offset))
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

		if catchup {
			// if catching up (views), stop reading from topic before marking
			// partition as recovered to avoid blocking other partitions when
			// p.ch gets full
			p.proxy.Remove(p.topic)

			// drain events channel -- we'll fetch them again later
			done := make(chan bool)
			go func() {
				for {
					select {
					case <-p.ch:
					case <-done:
						return

					}
				}
			}()
			defer close(done)
		}

		// mark storage as recovered -- this may take long
		if err = p.st.MarkRecovered(); err != nil {
			return
		}

		if catchup {
			// start reading from topic again if in catchup mode
			p.proxy.Add(p.topic, p.hwm)
		}

		// update stats
		p.stats.Table.Status = PartitionRunning
		p.stats.Table.RecoveryTime = time.Now()

		atomic.StoreInt32(&p.recoveredFlag, 1)
	})
	return
}

func (p *partition) fetchStats() *PartitionStats {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case p.requestStats <- true:
	case <-p.dying:
		// if closing, return empty stats
		return newPartitionStats()
	case <-timer.C:
		return p.lastStats
	}

	select {
	case s := <-p.responseStats:
		return s
	case <-p.dying:
		// if closing, return empty stats
		return newPartitionStats()
	}
}
