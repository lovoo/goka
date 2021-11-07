package systemtest

import (
	"context"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/internal/test"
	"github.com/lovoo/goka/multierr"
)

func TestProcessorShutdown_KafkaDisconnect(t *testing.T) {
	brokers := initSystemTest(t)
	var (
		topic = goka.Stream(fmt.Sprintf("goka_systemtest_proc_shutdown_disconnect-%d", time.Now().Unix()))
		group = goka.Group(topic)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errg, ctx := multierr.NewErrGroup(ctx)

	tmgr, err := goka.DefaultTopicManagerBuilder(brokers)
	test.AssertNil(t, err)
	test.AssertNil(t, tmgr.EnsureStreamExists(string(topic), 10))

	// emit values
	errg.Go(func() error {
		em, err := goka.NewEmitter(brokers, topic, new(codec.Int64))
		test.AssertNil(t, err)
		defer em.Finish()
		var i int64
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			prom, err := em.Emit(fmt.Sprintf("key-%d", i%20), i)
			test.AssertNil(t, err)
			prom.Then(func(err error) {
				test.AssertNil(t, err)
			})
			time.Sleep(100 * time.Millisecond)
			i++
		}
	})

	cfg := goka.DefaultConfig()

	fi := NewFIProxy()
	cfg.Net.Proxy.Enable = true
	cfg.Net.Proxy.Dialer = fi

	proc, err := goka.NewProcessor(brokers,
		goka.DefineGroup(
			group,
			goka.Input(topic, new(codec.Int64), func(ctx goka.Context, msg interface{}) {
				if val := ctx.Value(); val != nil {
					ctx.SetValue(val.(int64) + msg.(int64))
				} else {
					ctx.SetValue(msg)
				}
			}),
			goka.Persist(new(codec.Int64)),
		),
		goka.WithConsumerGroupBuilder(goka.ConsumerGroupBuilderWithConfig(cfg)),
		goka.WithProducerBuilder(goka.ProducerBuilderWithConfig(cfg)),
		goka.WithConsumerSaramaBuilder(goka.SaramaConsumerBuilderWithConfig(cfg)),
	)
	test.AssertNil(t, err)

	errg.Go(func() error {
		return proc.Run(ctx)
	})
	pollTimed(t, "proc running", 10, proc.Recovered, func() bool {
		if val, _ := proc.Get("key-15"); val != nil && val.(int64) > 0 {
			return true
		}
		return false
	})

	log.Printf("disconnecting consumer-group")
	fi.SetReadError(io.EOF)
	fi.SetWriteError(io.ErrClosedPipe)
	err = errg.Wait().ErrorOrNil()

	test.AssertNotNil(t, err)
}
