package systemtest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/storage"
	"github.com/stretchr/testify/require"
)

// size of the channel used for the visitor, defined in goka/config.go
var (
	visitChannelSize = 100
	numPartitions    = 10
)

// TestProcessorVisit tests the visiting functionality.
func TestProcessorVisit(t *testing.T) {
	brokers := initSystemTest(t)

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	cfg := goka.DefaultConfig()
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	require.NoError(t, err)

	// counts tests executed to get a unique id for group/topic to have every test start
	// with empty topics on kafka
	var testNum int
	nextTopics := func() (goka.Group, goka.Stream) {
		testNum++
		group := goka.Group(fmt.Sprintf("goka-systemtest-processor-visit-%d-%d", time.Now().Unix(), testNum))
		return group, goka.Stream(string(group) + "-input")
	}

	createEmitter := func(topic goka.Stream) (*goka.Emitter, func()) {
		err = tm.EnsureStreamExists(string(topic), numPartitions)
		require.NoError(t, err)

		em, err := goka.NewEmitter(brokers, topic, new(codec.Int64),
			goka.WithEmitterTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		)
		require.NoError(t, err)
		return em, func() {
			require.NoError(t, em.Finish())
		}
	}

	createProc := func(group goka.Group, input goka.Stream, pause time.Duration) *goka.Processor {
		proc, err := goka.NewProcessor(brokers,
			goka.DefineGroup(
				goka.Group(group),
				goka.Input(input, new(codec.Int64), func(ctx goka.Context, msg interface{}) { ctx.SetValue(msg) }),
				goka.Persist(new(codec.Int64)),
				goka.Visitor("visitor", func(ctx goka.Context, msg interface{}) {
					select {
					case <-ctx.Context().Done():
					case <-time.After(pause):
						ctx.SetValue(msg)
					}
				}),
			),
			goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
			goka.WithStorageBuilder(storage.MemoryBuilder()),
		)

		require.NoError(t, err)

		return proc
	}

	createView := func(group goka.Group) *goka.View {
		view, err := goka.NewView(brokers,
			goka.GroupTable(group),
			new(codec.Int64),
			goka.WithViewTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
			goka.WithViewStorageBuilder(storage.MemoryBuilder()),
		)

		require.NoError(t, err)

		return view
	}

	t.Run("visit-success", func(t *testing.T) {
		group, input := nextTopics()
		em, finish := createEmitter(input)
		defer finish()
		proc, cancel, done := runProc(createProc(group, input, 0))

		pollTimed(t, "recovered", proc.Recovered)

		_ = em.EmitSync("value1", int64(1))

		pollTimed(t, "value-ok", func() bool {
			val1, _ := proc.Get("value1")
			return val1 != nil && val1.(int64) == 1
		})

		require.NoError(t, proc.VisitAll(context.Background(), "visitor", int64(25)))
		pollTimed(t, "values-ok", func() bool {
			val1, _ := proc.Get("value1")
			return val1 != nil && val1.(int64) == 25
		})

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("visit-panic", func(t *testing.T) {
		group, input := nextTopics()
		em, finish := createEmitter(input)
		defer finish()
		proc, cancel, done := runProc(createProc(group, input, 0))

		pollTimed(t, "recovered", proc.Recovered)

		_ = em.EmitSync("value1", int64(1))

		pollTimed(t, "value-ok", func() bool {
			val1, _ := proc.Get("value1")
			return val1 != nil && val1.(int64) == 1
		})

		require.NoError(t, proc.VisitAll(context.Background(), "visitor", "asdf")) // pass wrong type to visitor -> which will be passed to the visit --> will panic

		// no need to cancel, the visitAll will kill the processor.
		_ = cancel
		require.Error(t, <-done)
	})

	// Tests if a panic occurs while visiting, while the iterator is still pushing
	// messages into the partition processor's visit-channel.
	// Regression test for https://github.com/lovoo/goka/issues/433
	t.Run("visit-panic-slow", func(t *testing.T) {
		group, input := nextTopics()
		em, finish := createEmitter(input)
		defer finish()
		proc, cancel, done := runProc(createProc(group, input, 500*time.Millisecond))

		pollTimed(t, "recovered", proc.Recovered)

		// create twice as many items in the table as the visit-channel's size.
		// This way we can make sure that the visitor will have to block on
		// pushing it to the partition-processor visitInputChannel.
		numMsgs := visitChannelSize * numPartitions * 2
		for i := 0; i < numMsgs; i++ {
			_, _ = em.Emit(fmt.Sprintf("value-%d", i), int64(1))
		}

		// wait for all messages to have propagated
		pollTimed(t, "value-ok", func() bool {
			val1, _ := proc.Get(fmt.Sprintf("value-%d", numMsgs-1))
			return val1 != nil && val1.(int64) == 1
		})

		// pass wrong type to visitor -> which will be passed to the visit --> will panic
		require.Error(t, proc.VisitAll(context.Background(), "visitor", "asdf"))

		// no need to cancel, the visitAll will kill the processor.
		_ = cancel
		require.Error(t, <-done)
	})

	// Verifies a visit is gracefully shutdown when the processor is canceled while
	// the visit is running.
	t.Run("visit-shutdown-slow", func(t *testing.T) {
		group, input := nextTopics()
		em, finish := createEmitter(input)
		defer finish()
		proc, cancel, done := runProc(createProc(group, input, 1*time.Second))

		pollTimed(t, "recovered", proc.Recovered)

		// create twice as many items in the table as the visit-channel's size.
		// This way we can make sure that the visitor will have to block on
		// pushing it to the partition-processor visitInputChannel.
		numMsgs := visitChannelSize * numPartitions * 2
		for i := 0; i < numMsgs; i++ {
			_, _ = em.Emit(fmt.Sprintf("value-%d", i), int64(1))
		}

		// wait for all messages to have propagated
		pollTimed(t, "value-ok", func() bool {
			val1, _ := proc.Get(fmt.Sprintf("value-%d", numMsgs-1))
			return val1 != nil && val1.(int64) == 1
		})

		visitCtx, visitCancel := context.WithCancel(context.Background())
		defer visitCancel()

		var (
			visitErr  error
			visitDone = make(chan struct{})
		)
		// pass wrong type to visitor -> which will be passed to the visit --> will panic
		go func() {
			defer close(visitDone)
			visitErr = proc.VisitAll(visitCtx, "visitor", int64(25))
		}()

		time.Sleep(500 * time.Millisecond)
		// stop the visit
		visitCancel()

		<-visitDone
		require.ErrorContains(t, visitErr, "canceled")

		cancel()
		require.NoError(t, <-done)
	})

	t.Run("visit-shutdown", func(t *testing.T) {
		group, input := nextTopics()
		em, finish := createEmitter(input)
		defer finish()
		proc, cancel, done := runProc(createProc(group, input, 500*time.Millisecond))

		pollTimed(t, "recovered", proc.Recovered)

		// emit two values where goka.DefaultHasher says they're in the same partition.
		// We need to achieve this to test that a shutdown will visit one value but not the other
		_ = em.EmitSync("0", int64(1))
		_ = em.EmitSync("02", int64(1))

		pollTimed(t, "value-ok", func() bool {
			val1, _ := proc.Get("02")
			val2, _ := proc.Get("0")
			return val1 != nil && val1.(int64) == 1 && val2 != nil && val2.(int64) == 1
		})

		ctx, visitCancel := context.WithCancel(context.Background())

		var (
			visitDone = make(chan struct{})
			visited   int64
			err       error
		)
		go func() {
			defer close(visitDone)
			visited, err = proc.VisitAllWithStats(ctx, "visitor", int64(42))
		}()

		// since every visit waits 500ms (as configured when creating the producer),
		// we'll wait 750ms so one will be visited and the second will be aborted.
		time.Sleep(750 * time.Millisecond)
		visitCancel()

		<-visitDone
		require.Equal(t, int64(1), visited)
		require.True(t, errors.Is(err, context.Canceled), err)

		val1, _ := proc.Get("0")
		val2, _ := proc.Get("02")

		// val1 was visited, the other was cancelled
		require.Equal(t, int64(42), val1.(int64))
		require.Equal(t, int64(1), val2.(int64))

		// let's revisit everything again.
		visited, err = proc.VisitAllWithStats(context.Background(), "visitor", int64(43))
		require.NoError(t, err)
		require.Equal(t, int64(2), visited)
		val1, _ = proc.Get("0")
		val2, _ = proc.Get("02")
		// both were visited
		require.Equal(t, int64(43), val1.(int64))
		require.Equal(t, int64(43), val2.(int64))

		// shutdown processor without error
		cancel()
		require.NoError(t, <-done)
	})

	t.Run("processor-shutdown", func(t *testing.T) {
		group, input := nextTopics()
		em, emFinish := createEmitter(input)
		defer emFinish()
		// create the group table manually, otherwise the proc and the view are racing

		_ = tm.EnsureTableExists(string(goka.GroupTable(group)), 10)
		// scenario: sleep in visit, processor shuts down--> visit should cancel too
		proc, cancel, done := runProc(createProc(group, input, 500*time.Millisecond))
		view, viewCancel, viewDone := runView(createView(group))

		pollTimed(t, "recovered", proc.Recovered)
		pollTimed(t, "recovered", view.Recovered)

		// emit two values where goka.DefaultHasher says they're in the same partition.
		// We need to achieve this to test that a shutdown will visit one value but not the other
		for i := 0; i < 100; i++ {
			_, _ = em.Emit(fmt.Sprintf("value-%d", i), int64(1))
		}
		// emFinish()

		// poll until all values are there
		pollTimed(t, "value-ok", func() bool {
			for i := 0; i < 100; i++ {
				val, _ := proc.Get(fmt.Sprintf("value-%d", i))
				if val == nil || val.(int64) != 1 {
					return false
				}
			}
			return true
		})

		var (
			visitDone = make(chan struct{})
			visited   int64
			err       error
		)
		go func() {
			defer close(visitDone)
			visited, err = proc.VisitAllWithStats(context.Background(), "visitor", int64(42))
		}()

		time.Sleep(750 * time.Millisecond)

		// shutdown processor without error
		cancel()
		require.NoError(t, <-done)
		<-visitDone

		require.True(t, visited > 0 && visited < 100, fmt.Sprintf("visited is %d", visited))
		require.True(t, errors.Is(err, goka.ErrVisitAborted), err)

		viewCancel()
		require.NoError(t, <-viewDone)
	})

	t.Run("processor-rebalance", func(t *testing.T) {
		group, input := nextTopics()
		em, finish := createEmitter(input)
		defer finish()
		// create the group table manually, otherwise the proc and the view are racing
		_ = tm.EnsureTableExists(string(goka.GroupTable(group)), 10)
		// scenario: sleep in visit, processor shuts down--> visit should cancel too
		proc1, cancel1, done1 := runProc(createProc(group, input, 500*time.Millisecond))

		pollTimed(t, "recovered", proc1.Recovered)

		// emit two values where goka.DefaultHasher says they're in the same partition.
		// We need to achieve this to test that a shutdown will visit one value but not the other
		for i := 0; i < 100; i++ {
			_, _ = em.Emit(fmt.Sprintf("value-%d", i), int64(1))
		}

		// poll until all values are there
		pollTimed(t, "value-ok", func() bool {
			for i := 0; i < 100; i++ {
				val, _ := proc1.Get(fmt.Sprintf("value-%d", i))
				if val == nil || val.(int64) != 1 {
					return false
				}
			}
			return true
		})

		var (
			visitDone = make(chan struct{})
			visited   int64
			visitErr  error
		)
		go func() {
			defer close(visitDone)
			visited, visitErr = proc1.VisitAllWithStats(context.Background(), "visitor", int64(42))
		}()

		time.Sleep(750 * time.Millisecond)
		_, cancel2, done2 := runProc(createProc(group, input, 500*time.Millisecond))

		// wait until the visit is aborted by the new processor (rebalance)
		pollTimed(t, "visit-abort", func() bool {
			select {
			case <-visitDone:
				return errors.Is(visitErr, goka.ErrVisitAborted) && visited > 0 && visited < 100
			default:
				return false
			}
		})

		// shutdown all processors
		cancel1()
		require.NoError(t, <-done1)
		cancel2()
		require.NoError(t, <-done2)
	})
}
