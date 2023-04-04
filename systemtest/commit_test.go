package systemtest

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/stretchr/testify/require"
)

// TestAutoCommit tests/demonstrates the behavior of disabling the auto-commit functionality.
// The autocommiter sends the offsets of the marked messages to the broker regularily. If the processor shuts down
// (or the group rebalances), the offsets are sent one last time, so just turning it of is not enough.
// To get a processor to not commit any offsets, we're using a fault-injecting proxy
// and cut the connections before shutdown, so the last-commit is failing.
func TestAutoCommit(t *testing.T) {
	var (
		group       goka.Group = goka.Group(fmt.Sprintf("%s-%d", "goka-commit-test", time.Now().Unix()))
		inputStream            = goka.Stream(group) + "-input"
		brokers                = initSystemTest(t)
	)

	// we'll use the proxy for cutting the connection before the final commit.
	fi := NewFIProxy()

	cfg := goka.DefaultConfig()
	// make sure we consume everything
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	// disable auto-commit
	cfg.Consumer.Offsets.AutoCommit.Enable = false

	// use the fault-injecting proxy
	cfg.Net.Proxy.Enable = true
	cfg.Net.Proxy.Dialer = fi

	goka.ReplaceGlobalConfig(cfg)

	tmgr, err := goka.DefaultTopicManagerBuilder(brokers)
	require.NoError(t, err)
	require.NoError(t, tmgr.EnsureStreamExists(string(inputStream), 10))
	require.NoError(t, tmgr.Close())

	defer func() {
		goka.ReplaceGlobalConfig(goka.DefaultConfig())
	}()

	var offsets []int64

	em, err := goka.NewEmitter(brokers, inputStream, new(codec.Int64))
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, em.EmitSync("key", int64(i)))
	}

	require.NoError(t, em.Finish())

	createProc := func() *goka.Processor {
		proc, err := goka.NewProcessor(brokers, goka.DefineGroup(group,
			goka.Input(inputStream, new(codec.Int64), func(ctx goka.Context, msg interface{}) {
				val := msg.(int64)

				// append offset
				offsets = append(offsets, val)
			}),
		))

		require.NoError(t, err)
		return proc
	}

	// run the first processor
	_, cancel, done := runProc(createProc())
	pollTimed(t, "all-received1", func() bool {
		return len(offsets) == 10 && offsets[0] == 0
	})

	// make all connections fail
	fi.SetWriteError(errors.New("cutting connecting"))

	// cancel processor
	cancel()
	<-done

	// reset errors, reset offsets and restart processor
	fi.ResetErrors()
	offsets = nil
	_, cancel, done = runProc(createProc())

	// --> we'll receive all messages again
	// --> i.e., no offsets were committed
	pollTimed(t, "all-received2", func() bool {
		return len(offsets) == 10 && offsets[0] == 0
	})

	cancel()
	<-done
}

// Test a failing processor does not mark the message.
// Two messages (1, 2) are emitted, after consuming (2), it crashes.
// Starting it a second time will reconsume it.
func TestUnmarkedMessages(t *testing.T) {
	var (
		group       goka.Group = goka.Group(fmt.Sprintf("%s-%d", "goka-mark-test", time.Now().Unix()))
		inputStream            = goka.Stream(group) + "-input"
		brokers                = initSystemTest(t)
	)

	// make sure we consume everything
	cfg := goka.DefaultConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	goka.ReplaceGlobalConfig(cfg)

	tmgr, err := goka.DefaultTopicManagerBuilder(brokers)
	require.NoError(t, err)
	require.NoError(t, tmgr.EnsureStreamExists(string(inputStream), 10))
	require.NoError(t, tmgr.Close())

	var values []int64

	// emit exactly one message
	em, err := goka.NewEmitter(brokers, inputStream, new(codec.Int64))
	require.NoError(t, err)
	require.NoError(t, em.EmitSync("key", int64(1)))
	require.NoError(t, em.EmitSync("key", int64(2)))
	require.NoError(t, em.Finish())

	createProc := func() *goka.Processor {
		proc, err := goka.NewProcessor(brokers, goka.DefineGroup(group,
			goka.Input(inputStream, new(codec.Int64), func(ctx goka.Context, msg interface{}) {
				val := msg.(int64)
				values = append(values, val)

				// the only way to not commit a message is to fail the processor.
				// We'll fail after the second message
				if val == 2 {
					ctx.Fail(errors.New("test"))
				}
			}),
		))

		require.NoError(t, err)
		return proc
	}

	// run the first processor
	runProc(createProc())
	pollTimed(t, "all-received1", func() bool {
		return len(values) == 2 && values[0] == 1
	})

	// reset values
	values = nil

	// restart -> we'll only receive the second message
	runProc(createProc())
	pollTimed(t, "all-received2", func() bool {
		return len(values) == 1 && values[0] == 2
	})
}
