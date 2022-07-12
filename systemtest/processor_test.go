package systemtest

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
	"github.com/stretchr/testify/require"
)

// Tests the processor option WithHotStandby. This requires a (local) running kafka cluster.
// Two processors are initialized, one plain, the other with hotstandby enabled.
// after initializing and sending a message to each partition, we verify that the hot-standby-processor
// has both values in the respective storage
func TestHotStandby(t *testing.T) {
	var (
		group       goka.Group = goka.Group(fmt.Sprintf("%s-%d", "goka-systemtest-hotstandby", time.Now().Unix()))
		inputStream string     = string(group) + "-input"
		table                  = string(goka.GroupTable(group))
		joinTable   goka.Table = goka.Table(fmt.Sprintf("%s-join", group))
	)

	brokers := initSystemTest(t)

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	cfg := goka.DefaultConfig()
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	require.NoError(t, err)

	err = tm.EnsureStreamExists(inputStream, 2)
	require.NoError(t, err)
	err = tm.EnsureTableExists(string(joinTable), 2)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	proc1Storages := newStorageTracker()

	proc1, err := goka.NewProcessor(brokers,
		goka.DefineGroup(
			group,
			goka.Input(goka.Stream(inputStream), new(codec.String), func(ctx goka.Context, msg interface{}) { ctx.SetValue(msg) }),
			goka.Join(joinTable, new(codec.String)),
			goka.Persist(new(codec.String)),
		),
		goka.WithStorageBuilder(proc1Storages.Build),
	)
	require.NoError(t, err)

	proc2Storages := newStorageTracker()

	proc2, err := goka.NewProcessor(brokers,
		goka.DefineGroup(
			group,
			goka.Input(goka.Stream(inputStream), new(codec.String), func(ctx goka.Context, msg interface{}) { ctx.SetValue(msg) }),
			goka.Join(joinTable, new(codec.String)),
			goka.Persist(new(codec.String)),
		),
		goka.WithHotStandby(),
		goka.WithStorageBuilder(proc2Storages.Build),
	)

	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errg, ctx := multierr.NewErrGroup(ctx)

	errg.Go(func() error {
		// simulatenously stopping multiple processors sometimes fails the processors, so this one gets delayed
		// see issue #376 for details
		return proc1.Run(DelayedCtxCloser(ctx, 5*time.Second))
	})

	errg.Go(func() error {
		return proc2.Run(ctx)
	})

	pollTimed(t, "procs 1&2 recovered", 25.0, proc1.Recovered, proc2.Recovered)

	// check the storages that were initalized by the processors:
	// proc1 is without hotstandby -> only two storages: (1 for the table, 1 for the join)
	// proc2 uses hotstandby --> 4 storages (2 for table, 2 for join)
	require.Equal(t, 2, len(proc1Storages.storages))
	require.Equal(t, 4, len(proc2Storages.storages))

	inputEmitter, err := goka.NewEmitter(brokers, goka.Stream(inputStream), new(codec.String))
	require.NoError(t, err)
	defer inputEmitter.Finish()
	require.NoError(t, inputEmitter.EmitSync("key1", "message1"))
	require.NoError(t, inputEmitter.EmitSync("key2", "message2"))

	// emit something into the join table (like simulating a processor ctx.SetValue()).
	// Our test processors should update their value in the join-table
	joinEmitter, err := goka.NewEmitter(brokers, goka.Stream(joinTable), new(codec.String))
	require.NoError(t, err)
	defer joinEmitter.Finish()
	require.NoError(t, joinEmitter.EmitSync("key1", "joinval1"))
	require.NoError(t, joinEmitter.EmitSync("key2", "joinval2"))

	// determine the partitions for both keys, assert they're not equal
	// (note that the keys might have to be changed if goka's default hasher changes)
	partx := hashKey("key1", 2)
	party := hashKey("key2", 2)
	require.NotEqual(t, partx, party)

	// get the corresponding storages for both table and join-partitions
	tableStorage1 := proc2Storages.storages[proc2Storages.key(string(table), partx)]
	tableStorage2 := proc2Storages.storages[proc2Storages.key(string(table), party)]
	joinStorage1 := proc2Storages.storages[proc2Storages.key(string(joinTable), partx)]
	joinStorage2 := proc2Storages.storages[proc2Storages.key(string(joinTable), party)]

	// wait until the keys are present
	pollTimed(t, "key-values are present", 10,
		func() bool {
			has, _ := tableStorage1.Has("key1")
			return has
		},
		func() bool {
			has, _ := tableStorage2.Has("key2")
			return has
		},
		func() bool {
			has, _ := joinStorage1.Has("key1")
			return has
		},
		func() bool {
			has, _ := joinStorage2.Has("key2")
			return has
		},
	)

	// check the table-values
	val1, _ := tableStorage1.Get("key1")
	val2, _ := tableStorage2.Get("key2")
	require.Equal(t, "message1", string(val1))
	require.Equal(t, "message2", string(val2))

	// check the join-values
	joinval1, _ := joinStorage1.Get("key1")
	joinval2, _ := joinStorage2.Get("key2")
	require.Equal(t, "joinval1", string(joinval1))
	require.Equal(t, "joinval2", string(joinval2))

	// stop everything and wait until it's shut down
	cancel()
	require.NoError(t, errg.Wait().ErrorOrNil())
}

// Tests the processor option WithRecoverAhead. This requires a (local) running kafka cluster.
// Two processors are initialized, but they share an input topic which has only one partition. This
// Test makes sure that still both processors recover the views/tables
func TestRecoverAhead(t *testing.T) {
	brokers := initSystemTest(t)
	var (
		group       = goka.Group(fmt.Sprintf("goka-systemtest-recoverahead-%d", time.Now().Unix()))
		inputStream = fmt.Sprintf("%s-input", group)
		table       = string(goka.GroupTable(group))
		joinTable   = goka.Table(fmt.Sprintf("%s-join", group))
	)

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	cfg := goka.DefaultConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	goka.ReplaceGlobalConfig(cfg)

	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	require.NoError(t, err)

	err = tm.EnsureStreamExists(inputStream, 1)
	require.NoError(t, err)
	err = tm.EnsureTableExists(string(joinTable), 1)
	require.NoError(t, err)
	err = tm.EnsureTableExists(string(table), 1)
	require.NoError(t, err)

	// emit something into the join table (like simulating a processor ctx.SetValue()).
	// Our test processors should update their value in the join-table
	joinEmitter, err := goka.NewEmitter(brokers, goka.Stream(joinTable), new(codec.String))
	require.NoError(t, err)
	require.NoError(t, joinEmitter.EmitSync("key1", "joinval1"))
	require.NoError(t, joinEmitter.Finish())

	// emit something into the join table (like simulating a processor ctx.SetValue()).
	// Our test processors should update their value in the join-table
	tableEmitter, err := goka.NewEmitter(brokers, goka.Stream(table), new(codec.String))
	require.NoError(t, err)
	require.NoError(t, tableEmitter.EmitSync("key1", "tableval1"))
	require.NoError(t, tableEmitter.Finish())

	proc1Storages := newStorageTracker()

	proc1, err := goka.NewProcessor(brokers,
		goka.DefineGroup(
			group,
			goka.Input(goka.Stream(inputStream), new(codec.String), func(ctx goka.Context, msg interface{}) { ctx.SetValue(msg) }),
			goka.Join(joinTable, new(codec.String)),
			goka.Persist(new(codec.String)),
		),
		goka.WithRecoverAhead(),
		goka.WithStorageBuilder(proc1Storages.Build),
	)
	require.NoError(t, err)

	proc2Storages := newStorageTracker()

	proc2, err := goka.NewProcessor(brokers,
		goka.DefineGroup(
			group,
			goka.Input(goka.Stream(inputStream), new(codec.String), func(ctx goka.Context, msg interface{}) { ctx.SetValue(msg) }),
			goka.Join(joinTable, new(codec.String)),
			goka.Persist(new(codec.String)),
		),
		goka.WithRecoverAhead(),
		goka.WithStorageBuilder(proc2Storages.Build),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errg, ctx := multierr.NewErrGroup(ctx)

	errg.Go(func() error {
		// simulatenously stopping multiple processors sometimes fails the processors, so this one gets delayed
		// see issue #376 for details
		return proc1.Run(DelayedCtxCloser(ctx, 5*time.Second))
	})

	errg.Go(func() error {
		return proc2.Run(ctx)
	})

	pollTimed(t, "procs 1&2 recovered", 10.0, func() bool {
		return true
	}, proc1.Recovered, proc2.Recovered)

	// check the storages that were initalized by the processors:
	// both have each 2 storages, because all tables only have 1 partition
	require.Equal(t, 2, len(proc1Storages.storages))
	require.Equal(t, 2, len(proc2Storages.storages))

	// get the corresponding storages for both table and join-partitions
	tableStorage1 := proc1Storages.storages[proc1Storages.key(string(table), 0)]
	tableStorage2 := proc2Storages.storages[proc2Storages.key(string(table), 0)]
	joinStorage1 := proc1Storages.storages[proc1Storages.key(string(joinTable), 0)]
	joinStorage2 := proc2Storages.storages[proc2Storages.key(string(joinTable), 0)]

	// wait until the keys are present
	pollTimed(t, "key-values are present", 20.0,

		func() bool {
			return true
		},
		func() bool {
			has, _ := tableStorage1.Has("key1")
			return has
		},
		func() bool {
			has, _ := tableStorage2.Has("key1")
			return has
		},
		func() bool {
			has, _ := joinStorage1.Has("key1")
			return has
		},
		func() bool {
			has, _ := joinStorage2.Has("key1")
			return has
		},
	)

	// check the table-values
	val1, _ := tableStorage1.Get("key1")
	val2, _ := tableStorage2.Get("key1")
	require.Equal(t, "tableval1", string(val1))
	require.Equal(t, "tableval1", string(val2))

	// check the join-values
	joinval1, _ := joinStorage1.Get("key1")
	joinval2, _ := joinStorage2.Get("key1")
	require.Equal(t, "joinval1", string(joinval1))
	require.Equal(t, "joinval1", string(joinval2))

	// stop everything and wait until it's shut down
	cancel()
	require.NoError(t, errg.Wait().ErrorOrNil())
}

// TestRebalance runs some processors to test rebalance. It's merely a
// runs-without-errors test, not a real functional test.
func TestRebalance(t *testing.T) {
	t.Parallel()
	brokers := initSystemTest(t)

	var (
		group              = goka.Group(fmt.Sprintf("goka-systemtest-rebalance-%d", time.Now().Unix()))
		inputStream string = string(group) + "-input"
		basepath           = "/tmp/goka-rebalance-test"
	)

	require.NoError(t, os.RemoveAll(basepath))

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	cfg := goka.DefaultConfig()
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	require.NoError(t, err)

	err = tm.EnsureStreamExists(inputStream, 20)
	require.NoError(t, err)

	em, err := goka.NewEmitter(brokers, goka.Stream(inputStream), new(codec.String))
	require.NoError(t, err)

	go func() {
		defer em.Finish()
		i := 0
		for {
			i++
			require.NoError(t, em.EmitSync(fmt.Sprintf("%d", i), "value"))
			time.Sleep(50 * time.Microsecond)
		}
	}()

	createProc := func(id int) *goka.Processor {
		proc, err := goka.NewProcessor(brokers,
			goka.DefineGroup(
				group,
				goka.Input(goka.Stream(inputStream), new(codec.String), func(ctx goka.Context, msg interface{}) { ctx.SetValue(msg) }),
				goka.Persist(new(codec.String)),
			),
			goka.WithRecoverAhead(),
			goka.WithHotStandby(),
			goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
			goka.WithStorageBuilder(storage.DefaultBuilder(fmt.Sprintf("%s/proc-%d", basepath, id))),
		)

		require.NoError(t, err)
		return proc
	}

	errg, ctx := multierr.NewErrGroup(context.Background())

	for i := 0; i < 5; i++ {
		i := i
		errg.Go(func() error {
			p := createProc(i)
			ctx, cancel := context.WithTimeout(ctx, time.Duration(16)*time.Second)
			defer cancel()
			return p.Run(ctx)
		})
		time.Sleep(2 * time.Second)
	}

	require.NoError(t, errg.Wait().ErrorOrNil())
}

// TestRebalanceSharePartitions runs two processors one after each other
// and asserts that they rebalance partitions appropriately
func TestRebalanceSharePartitions(t *testing.T) {
	t.Parallel()
	brokers := initSystemTest(t)

	var (
		group                = goka.Group(fmt.Sprintf("goka-systemtest-rebalance-share-partitions-%d", time.Now().Unix()))
		inputStream   string = string(group) + "-input"
		numPartitions        = 20
	)

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	cfg := goka.DefaultConfig()
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	require.NoError(t, err)

	err = tm.EnsureStreamExists(inputStream, numPartitions)
	require.NoError(t, err)

	// start an emitter
	cancelEmit, emitDone := runWithContext(func(ctx context.Context) error {
		em, err := goka.NewEmitter(brokers, goka.Stream(inputStream), new(codec.Int64))
		require.NoError(t, err)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		defer em.Finish()
		for i := 0; ; i++ {
			select {
			case <-ticker.C:
				_, err := em.Emit(fmt.Sprintf("%d", i%100), int64(i))
				if err != nil {
					return nil
				}
			case <-ctx.Done():
				return nil
			}
		}
	})

	createProc := func() *goka.Processor {
		proc, err := goka.NewProcessor(brokers,
			goka.DefineGroup(
				group,
				goka.Input(goka.Stream(inputStream), new(codec.Int64), func(ctx goka.Context, msg interface{}) { ctx.SetValue(msg) }),
				goka.Persist(new(codec.Int64)),
			),
			goka.WithRecoverAhead(),
			goka.WithHotStandby(),
			goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
			goka.WithStorageBuilder(storage.MemoryBuilder()),
		)

		require.NoError(t, err)
		return proc
	}

	activePassivePartitions := func(stats *goka.ProcessorStats) (active, passive int) {
		for _, part := range stats.Group {
			if part.TableStats != nil && part.TableStats.RunMode == 0 {
				active++
			} else {
				passive++
			}
		}
		return
	}

	p1, cancelP1, p1Done := runProc(createProc())
	pollTimed(t, "p1 started", 10, p1.Recovered)

	// p1 has all active partitions
	p1Stats := p1.Stats()
	p1Active, p1Passive := activePassivePartitions(p1Stats)
	require.Equal(t, numPartitions, p1Active)
	require.Equal(t, 0, p1Passive)

	p2, cancelP2, p2Done := runProc(createProc())
	pollTimed(t, "p2 started", 20, p2.Recovered)
	pollTimed(t, "p1 still running", 10, p1.Recovered)

	// now p1 and p2 share the partitions
	p2Stats := p2.Stats()
	p2Active, p2Passive := activePassivePartitions(p2Stats)
	require.Equal(t, numPartitions/2, p2Active)
	require.Equal(t, numPartitions/2, p2Passive)
	p1Stats = p1.Stats()
	p1Active, p1Passive = activePassivePartitions(p1Stats)
	require.Equal(t, numPartitions/2, p1Active)
	require.Equal(t, numPartitions/2, p1Passive)

	// p1 is down
	cancelP1()
	require.True(t, <-p1Done == nil)

	// p2 should have all partitions
	pollTimed(t, "p2 has all partitions", 10, func() bool {
		p2Stats = p2.Stats()
		p2Active, p2Passive := activePassivePartitions(p2Stats)
		return p2Active == numPartitions && p2Passive == 0
	})

	cancelP2()
	require.True(t, <-p2Done == nil)

	// stop emitter
	cancelEmit()
	require.True(t, <-emitDone == nil)
}

func TestCallbackFail(t *testing.T) {
	t.Parallel()
	brokers := initSystemTest(t)

	var (
		group       goka.Group = goka.Group(fmt.Sprintf("goka-systemtest-callback-fail-%d", time.Now().Unix()))
		inputStream string     = string(group) + "-input"
	)

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	cfg := goka.DefaultConfig()
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	require.NoError(t, err)

	err = tm.EnsureStreamExists(inputStream, 10)
	require.NoError(t, err)

	em, err := goka.NewEmitter(brokers, goka.Stream(inputStream), new(codec.Int64))
	require.NoError(t, err)
	defer em.Finish()

	proc, err := goka.NewProcessor(brokers,
		goka.DefineGroup(
			group,
			goka.Input(goka.Stream(inputStream), new(codec.Int64), func(ctx goka.Context, msg interface{}) {
				if ctx.Key() == "key-9995" {
					ctx.Emit("some-invalid-emit", "", nil)
				}
			}),
		),
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		goka.WithStorageBuilder(storage.MemoryBuilder()),
	)
	require.NoError(t, err)

	proc, cancel, done := runProc(proc)

	pollTimed(t, "recovered", 10, proc.Recovered)

	go func() {
		for i := 0; i < 10000; i++ {
			em.Emit(fmt.Sprintf("key-%d", i), int64(1))
		}
	}()

	defer cancel()
	pollTimed(t, "error-response", 10, func() bool {
		select {
		case err, ok := <-done:
			if !ok {
				return false
			}
			if err == nil {
				return false
			}
			if err != nil {
				return true
			}
		default:
		}
		return false
	})
}

func TestProcessorSlowStuck(t *testing.T) {
	t.Parallel()
	brokers := initSystemTest(t)

	var (
		group              = goka.Group(fmt.Sprintf("goka-systemtest-slow-callback-fail-%d", time.Now().Unix()))
		inputStream string = string(group) + "-input"
	)

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	cfg := goka.DefaultConfig()
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	require.NoError(t, err)

	err = tm.EnsureStreamExists(inputStream, 2)
	require.NoError(t, err)

	em, err := goka.NewEmitter(brokers, goka.Stream(inputStream), new(codec.Int64))
	require.NoError(t, err)

	proc, err := goka.NewProcessor(brokers,
		goka.DefineGroup(
			group,
			goka.Input(goka.Stream(inputStream), new(codec.Int64), func(ctx goka.Context, msg interface{}) {
				val := msg.(int64)
				time.Sleep(500 * time.Microsecond)
				if ctx.Partition() == 0 && val > 10 {
					// do an invalid action
					panic("asdf")
				}
			}),
		),
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		goka.WithPartitionChannelSize(10),
	)

	require.NoError(t, err)

	errg, ctx := multierr.NewErrGroup(context.Background())

	errg.Go(func() error {
		ticker := time.NewTicker(10 * time.Microsecond)
		defer em.Finish()
		var i int64
		for {
			select {
			case <-ticker.C:
				i++
				require.NoError(t, em.EmitSync(fmt.Sprintf("%d", i%20), i))
			case <-ctx.Done():
				return nil
			}
		}
	})
	errg.Go(func() error {
		return proc.Run(ctx)
	})
	err = errg.Wait().ErrorOrNil()
	require.True(t, strings.Contains(err.Error(), "panic in callback"))
}

// Test the message commit of a processor, in particular to avoid reprocessing the last message after processor restart.
// Here is how it works:
// * Emit 10 messages with key/value "1"/1 into one topic
// * Create a processor that consumes+accumulates this one value into its state. The final state obviously is 10.
// * restart this processor a couple of times and check whether it stays 10.
//
func TestMessageCommit(t *testing.T) {
	t.Parallel()
	brokers := initSystemTest(t)

	var (
		group       goka.Group  = goka.Group(fmt.Sprintf("%s-%d", "goka-systemtest-message-commit", time.Now().Unix()))
		inputStream goka.Stream = goka.Stream(group) + "-input"
		numMessages             = 10
	)

	// New Emitter that will in total send 10 messages
	emitter, err := goka.NewEmitter(brokers, inputStream, new(codec.Int64))
	require.NoError(t, err)

	// some boiler plate code to create the topics in kafka using
	// only one replication
	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	cfg := goka.DefaultConfig()
	// we want to consume all messages
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	goka.ReplaceGlobalConfig(cfg)

	tmBuilder := goka.TopicManagerBuilderWithConfig(cfg, tmc)
	tm, err := tmBuilder(brokers)
	require.NoError(t, err)

	tm.EnsureStreamExists(string(inputStream), 10)

	for i := 0; i < numMessages; i++ {
		emitter.EmitSync("1", int64(1))
	}
	// close emitter
	require.NoError(t, emitter.Finish())

	// Start a processor a couple of times that accumulates the emitted value.
	// It always end up with a state of "10", but only consume the messages the first time.
	// The second and third time it will just start as there are no new message in the topic.
	for i := 0; i < 3; i++ {
		done := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		proc, err := goka.NewProcessor(brokers, goka.DefineGroup(group,
			goka.Input(inputStream, new(codec.Int64), func(ctx goka.Context, msg interface{}) {
				if val := ctx.Value(); val == nil {
					ctx.SetValue(msg)
				} else {
					ctx.SetValue(val.(int64) + msg.(int64))
				}
			}),
			goka.Persist(new(codec.Int64)),
		),
			goka.WithTopicManagerBuilder(tmBuilder),
		)
		require.NoError(t, err)
		// run a new processor
		go func() {
			defer close(done)
			err := proc.Run(ctx)
			require.NoError(t, err)
		}()

		time.Sleep(10 * time.Second)
		val, err := proc.Get("1")
		require.NoError(t, err)
		require.True(t, val != nil)
		require.Equal(t, int64(numMessages), val.(int64))

		cancel()
		<-done
	}
}

func TestProcessorGracefulShutdownContinue(t *testing.T) {
	t.Parallel()
	brokers := initSystemTest(t)

	var (
		group       goka.Group  = goka.Group(fmt.Sprintf("%s-%d", "goka-systemtest-graceful-shutdown-continue", time.Now().Unix()))
		inputStream goka.Stream = goka.Stream(group) + "-input"
	)

	emitter, err := goka.NewEmitter(brokers, inputStream, new(codec.Int64))
	require.NoError(t, err)

	// some boiler plate code to create the topics in kafka using
	// only one replication
	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	cfg := goka.DefaultConfig()
	// we want to consume all messages
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	goka.ReplaceGlobalConfig(cfg)

	tmBuilder := goka.TopicManagerBuilderWithConfig(cfg, tmc)
	tm, err := tmBuilder(brokers)
	require.NoError(t, err)

	tm.EnsureStreamExists(string(inputStream), 10)

	emitterCtx, cancelEmitter := context.WithCancel(context.Background())
	emitterDone := make(chan struct{})

	valueSum := make(map[string]int64)
	go func() {
		defer close(emitterDone)
		for i := 0; emitterCtx.Err() == nil; i++ {
			time.Sleep(1 * time.Millisecond)
			key := fmt.Sprintf("key-%d", i%23)
			emitter.Emit(key, int64(i))
			valueSum[key] += int64(i)
		}
	}()

	createProc := func() *goka.Processor {
		proc, err := goka.NewProcessor(brokers, goka.DefineGroup(group,
			goka.Input(inputStream, new(codec.Int64), func(ctx goka.Context, msg interface{}) {
				if val := ctx.Value(); val == nil {
					ctx.SetValue(msg)
				} else {
					ctx.SetValue(val.(int64) + msg.(int64))
				}
			}),
			goka.Persist(new(codec.Int64)),
		),
			goka.WithTopicManagerBuilder(tmBuilder),
			goka.WithStorageBuilder(storage.MemoryBuilder()),
		)
		require.NoError(t, err)
		return proc
	}

	proc, cancelProc, procDone := runProc(createProc())

	pollTimed(t, "proc-running", 10, proc.Recovered)

	// stop it
	cancelProc()
	require.NoError(t, <-procDone)

	// start it again --> must fail
	_, _, procDone = runProc(proc)
	require.Error(t, <-procDone)

	// start it 10 more times in a row and stop it
	for i := 0; i < 10; i++ {
		log.Printf("creating proc round %d", i)
		proc, cancelProc, procDone := runProc(createProc())
		pollTimed(t, "proc-running", 10, proc.Recovered)
		// stop it
		cancelProc()
		require.NoError(t, <-procDone)

	}
	// stop emitter
	cancelEmitter()
	<-emitterDone

	// start one last time to check the values
	log.Printf("creating a proc for the last time to check values")
	proc, cancelProc, procDone = runProc(createProc())
	pollTimed(t, "proc-running", 10, proc.Recovered)

	pollTimed(t, "correct-values", 10, func() bool {
		for key, value := range valueSum {
			tableVal, err := proc.Get(key)
			if tableVal == nil || err != nil {
				return false
			}
			if tableVal.(int64) < value {
				return false
			}
		}
		return true
	})

	// stop it
	cancelProc()
	require.NoError(t, <-procDone)
}
