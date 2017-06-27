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
	metrics "github.com/rcrowley/go-metrics"
)

const (
	mxStatus               = "status"
	mxConsumed             = "consumed_messages"
	mxConsumedRate         = "consumed_messages_rate"
	mxConsumedOffset       = "consumed_offset"
	mxConsumedPending      = "consumed_messages_pending"
	mxRecoverHwm           = "recover_hwm"
	mxPartitionLoaderHwm   = "partition_loader_hwm" // current high water mark of the partition loader.
	mxRecoverStartOffset   = "recover_start_offset"
	mxRecoverCurrentOffset = "recover_current_offset"

	defaultPartitionChannelSize = 10
	syncInterval                = 30 * time.Second
	stalledTimeout              = 2 * time.Minute
)

const (
	partitionOpen int64 = iota
	partitionRecovering
	partitionRecoverStalled
	partitionRecovered
	partitionRunning
	partitionClosing
	partitionClosed
)

type partition struct {
	log   logger.Logger
	topic string

	ch         chan kafka.Event
	dying      chan bool
	done       chan bool
	stopFlag   int64
	readyFlag  int32
	initialHwm int64

	st      *storageProxy
	proxy   kafkaProxy
	process processCallback

	// metrics
	registry          metrics.Registry
	mxStatus          metrics.Gauge   // partition status = ?
	mxConsumed        metrics.Counter // number of processed messages
	mxConsumedRate    metrics.Meter   // rate of processed messages
	mxConsumedPending metrics.Gauge   // size of the partition channel (p.ch)
	mxConsumedOffset  metrics.Gauge   // last offset processed

	mxRecoverStartOffset   metrics.Gauge
	mxRecoverCurrentOffset metrics.Gauge

	mxRecoverHwm         metrics.Gauge
	mxPartitionLoaderHwm metrics.Gauge
}

type kafkaProxy interface {
	Add(string, int64)
	Remove(string)
	AddGroup()
	Stop()
}

type processCallback func(msg *message, st storage.Storage, wg *sync.WaitGroup) error

func newPartition(log logger.Logger, topic string, cb processCallback, st *storageProxy, proxy kafkaProxy, reg metrics.Registry, channelSize int) *partition {
	return &partition{
		log:   log,
		topic: topic,

		ch:    make(chan kafka.Event, channelSize),
		dying: make(chan bool),
		done:  make(chan bool),

		st:      st,
		proxy:   proxy,
		process: cb,

		// metrics
		registry:               reg,
		mxConsumed:             metrics.GetOrRegisterCounter(mxConsumed, reg),
		mxConsumedRate:         metrics.GetOrRegisterMeter(mxConsumedRate, reg),
		mxConsumedPending:      metrics.GetOrRegisterGauge(mxConsumedPending, reg),
		mxConsumedOffset:       metrics.GetOrRegisterGauge(mxConsumedOffset, reg),
		mxStatus:               metrics.GetOrRegisterGauge(mxStatus, reg),
		mxRecoverHwm:           metrics.GetOrRegisterGauge(mxRecoverHwm, reg),
		mxPartitionLoaderHwm:   metrics.GetOrRegisterGauge(mxPartitionLoaderHwm, reg),
		mxRecoverStartOffset:   metrics.GetOrRegisterGauge(mxRecoverStartOffset, reg),
		mxRecoverCurrentOffset: metrics.GetOrRegisterGauge(mxRecoverCurrentOffset, reg),
	}
}

func (p *partition) start() error {
	defer close(p.done)
	defer p.proxy.Stop()
	defer p.mxStatus.Update(partitionClosed)

	if !p.st.Stateless() {
		err := p.st.Open()
		if err != nil {
			return err
		}
		defer p.st.Close()

		if err := p.recover(); err != nil {
			return err
		}
	}

	// if stopped, just return
	if atomic.LoadInt64(&p.stopFlag) == 1 {
		return nil
	}

	p.mxStatus.Update(partitionRunning)
	return p.run()
}

func (p *partition) startCatchup() error {
	defer close(p.done)
	defer p.proxy.Stop()

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
		Data:      ev.Value,
		Key:       string(ev.Key),
	}
}

func (p *partition) run() error {
	var wg sync.WaitGroup
	p.proxy.AddGroup()
	syncTicker := time.NewTicker(syncInterval)

	defer func() {
		p.st.Sync()
		wg.Wait()
		syncTicker.Stop()
	}()

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
					return fmt.Errorf("received message from group table topic after recovery")
				}
				msg := newMessage(ev)

				err := p.process(msg, p.st, &wg)
				if err != nil {
					return fmt.Errorf("error processing message: %v", err)
				}

				p.st.Sync()

				// metrics
				p.mxConsumed.Inc(1)
				p.mxConsumedPending.Update(int64(len(p.ch)))
				p.mxConsumedRate.Mark(1)
				p.mxConsumedOffset.Update(msg.Offset)
				metrics.GetOrRegisterCounter(fmt.Sprintf("%s.%s", ev.Topic, mxConsumed), p.registry).Inc(1)
				metrics.GetOrRegisterMeter(fmt.Sprintf("%s.%s", ev.Topic, mxConsumedRate), p.registry).Mark(1)
				metrics.GetOrRegisterGauge(fmt.Sprintf("%s.%s", ev.Topic, mxConsumedOffset), p.registry).Update(msg.Offset)

			case *kafka.NOP:
				// don't do anything but also don't log.
			case *kafka.EOF:
				if ev.Topic != p.topic {
					return fmt.Errorf("received EOF of topic that is not ours. This should not happend (ours=%s, received=%s)", p.topic, ev.Topic)
				}
			default:
				return fmt.Errorf("load: cannot handle %T = %v", ev, ev)
			}

		case <-syncTicker.C:
			p.st.Sync()

		case <-p.dying:
			return nil
		}

	}
}

///////////////////////////////////////////////////////////////////////////////
// loading storage
///////////////////////////////////////////////////////////////////////////////

func (p *partition) catchup() error {
	p.mxStatus.Update(partitionRecovering)
	return p.load(true)
}

func (p *partition) recover() error {
	p.mxStatus.Update(partitionRecovering)
	return p.load(false)
}

func (p *partition) ready() bool {
	return atomic.LoadInt32(&p.readyFlag) == 1
}

func (p *partition) load(catchup bool) error {
	// fetch local offset
	local, err := p.st.GetOffset(sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("Error reading local offset: %v", err)
	}
	p.proxy.Add(p.topic, local)
	defer p.proxy.Remove(p.topic)
	syncTicker := time.NewTicker(syncInterval)
	defer syncTicker.Stop()
	var lastMessage time.Time
	// create loader
	for {
		select {
		case ev, isOpen := <-p.ch:

			// channel already closed, ev will be nil
			if !isOpen {
				return nil
			}

			switch ev := ev.(type) {
			case *kafka.BOF:
				p.mxRecoverStartOffset.Update(ev.Offset)
				p.mxRecoverHwm.Update(ev.Hwm)
				p.mxPartitionLoaderHwm.Update(ev.Hwm)
				p.initialHwm = ev.Hwm

				// nothing to recover
				if ev.Offset == ev.Hwm {
					p.mxStatus.Update(partitionRecovered)
					atomic.StoreInt32(&p.readyFlag, 1)
				}

			case *kafka.EOF:
				p.mxPartitionLoaderHwm.Update(ev.Hwm)
				if atomic.LoadInt32(&p.readyFlag) == 0 {
					p.log.Printf("readyFlag was false when EOF arrived")
					p.mxStatus.Update(partitionRecovered)
					atomic.StoreInt32(&p.readyFlag, 1)
				}
				if catchup {
					continue
				}
				return nil

			case *kafka.Message:
				if ev.Topic != p.topic {
					return fmt.Errorf("load: wrong topic = %s", ev.Topic)
				}
				err := p.storeEvent(ev)
				if err != nil {
					return fmt.Errorf("load: error updating storage: %v", err)
				}
				if ev.Offset >= p.initialHwm-1 {
					atomic.StoreInt32(&p.readyFlag, 1)
				}
				lastMessage = time.Now()
				// update metrics
				p.mxConsumedRate.Mark(1)
				metrics.GetOrRegisterMeter(fmt.Sprintf("%s.%s", ev.Topic, mxConsumedRate), p.registry).Mark(1)
				p.mxRecoverCurrentOffset.Update(ev.Offset)
				if ev.Offset < p.initialHwm-1 {
					p.mxStatus.Update(partitionRecovering)
				} else {
					p.mxStatus.Update(partitionRecovered)
				}
			case *kafka.NOP:
				// don't do anything

			default:
				return fmt.Errorf("load: cannot handle %T = %v", ev, ev)
			}

		case now := <-syncTicker.C:
			p.st.Sync()

			// only set to stalled, if the last message was earlier
			// than the stalled timeout
			if now.Sub(lastMessage) > stalledTimeout {
				p.mxStatus.Update(partitionRecoverStalled)
			}

		case <-p.dying:
			p.st.Sync()
			return nil
		}
	}
}

func (p *partition) storeEvent(msg *kafka.Message) error {
	err := p.st.Update(msg.Key, msg.Value)
	if err != nil {
		return fmt.Errorf("Error from the update callback while recovering from the log: %v", err)
	}

	// update offset in local storage
	err = p.st.SetOffset(int64(msg.Offset))
	if err != nil {
		return fmt.Errorf("Error updating offset in local storage while recovering from the log: %v", err)
	}

	p.st.Sync()
	return nil
}
