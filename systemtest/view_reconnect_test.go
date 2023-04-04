package systemtest

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/multierr"
	"github.com/stretchr/testify/require"
)

// Tests the following scenario:
// A view started with `WithViewAutoReconnect` should still return values even after losing connection to kafka.
// Therefore we start a view on a topic fed by an emitter, the view proxies through the FIProxy and loses connection
// after recovering. The values are still be served/returned
func TestView_Reconnect(t *testing.T) {
	topic := fmt.Sprintf("goka_systemtest_view_reconnect_test-%d", time.Now().Unix())
	brokers := initSystemTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errg, ctx := multierr.NewErrGroup(ctx)

	tmgr, err := goka.DefaultTopicManagerBuilder(brokers)
	require.NoError(t, err)
	require.NoError(t, tmgr.EnsureStreamExists(topic, 10))

	errg.Go(func() error {
		em, err := goka.NewEmitter(brokers, goka.Stream(topic), new(codec.Int64))
		if err != nil {
			return err
		}
		defer em.Finish()
		var i int64
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			require.NoError(t, em.EmitSync("key", i))
			time.Sleep(10 * time.Millisecond)
			i++
		}
	})

	cfg := goka.DefaultConfig()

	fi := NewFIProxy()
	cfg.Net.Proxy.Enable = true
	cfg.Net.Proxy.Dialer = fi

	// we'll use a view on the stream.
	view, err := goka.NewView(brokers, goka.Table(topic), new(codec.Int64),
		goka.WithViewAutoReconnect(),
		goka.WithViewConsumerSaramaBuilder(goka.SaramaConsumerBuilderWithConfig(cfg)),
		goka.WithViewTopicManagerBuilder(goka.TopicManagerBuilderWithConfig(cfg, goka.NewTopicManagerConfig())),
	)
	require.NoError(t, err)

	// Start view and wait for it to be recovered
	errg.Go(func() error {
		return view.Run(ctx)
	})
	pollTimed(t, "view-recovered", view.Recovered)

	val := func() int64 {
		val, err := view.Get("key")
		require.NoError(t, err)
		if val == nil {
			return 0
		}
		return val.(int64)
	}

	pollTimed(t, "wait-first-value", func() bool {
		return val() > 0
	})
	firstVal := val()

	time.Sleep(500 * time.Millisecond)

	// kill kafka connection
	fi.SetReadError(io.EOF)
	pollTimed(t, "view-reconnecting", func() bool {
		return view.CurrentState() == goka.ViewStateConnecting
	})

	// the view still should have gotten the update before the EOF
	secondVal := val()
	require.True(t, secondVal > firstVal)

	// let some time pass -> the value should not have updated
	time.Sleep(500 * time.Millisecond)
	require.True(t, val() == secondVal)

	// connect kafka again, wait until it's running -> the value should have changed
	fi.ResetErrors()
	pollTimed(t, "view-running", func() bool {
		return view.CurrentState() == goka.ViewStateRunning
	})
	pollTimed(t, "value-propagated", func() bool {
		return val() > secondVal
	})

	// shut everything down
	cancel()
	require.NoError(t, errg.Wait().ErrorOrNil())
}
