package systemtest

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/internal/test"
)

func TestEmitter_KafkaDisconnect(t *testing.T) {
	brokers := initSystemTest(t)
	var (
		topic = goka.Stream(fmt.Sprintf("goka_systemtest_emitter_disconnect-%d", time.Now().Unix()))
	)

	tmgr, err := goka.DefaultTopicManagerBuilder(brokers)
	test.AssertNil(t, err)
	test.AssertNil(t, tmgr.EnsureStreamExists(string(topic), 10))

	cfg := goka.DefaultConfig()

	fi := NewFIProxy()
	cfg.Net.Proxy.Enable = true
	cfg.Net.Proxy.Dialer = fi

	// get it faster over with
	cfg.Producer.Retry.Max = 1
	cfg.Producer.Retry.Backoff = 0

	em, err := goka.NewEmitter(brokers, topic, new(codec.Int64),
		goka.WithEmitterProducerBuilder(goka.ProducerBuilderWithConfig(cfg)),
	)
	test.AssertNil(t, err)
	var (
		i       int64
		success int64
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		var closeOnce sync.Once
		stop := make(chan struct{})
		for {
			select {
			case <-stop:
				return
			default:
			}

			prom, err := em.Emit(fmt.Sprintf("key-%d", i%20), i)
			if err != nil {
				if errors.Is(err, goka.ErrEmitterAlreadyClosed) {
					return
				}
				log.Printf("error emitting: %v", err)
			}
			prom.Then(func(err error) {
				if err != nil {
					log.Printf("error emitting (async): %v", err)
					closeOnce.Do(func() {
						close(stop)
					})
					return
				}
				if err == nil {
					atomic.AddInt64(&success, 1)
				}

			})
			time.Sleep(10 * time.Millisecond)
			i++
		}

	}()

	pollTimed(t, "emitter emitted something successfully", 10, func() bool {
		return atomic.LoadInt64(&success) > 0
	})

	fi.SetWriteError(syscall.EPIPE)
	<-done
	test.AssertNil(t, em.Finish())
}
