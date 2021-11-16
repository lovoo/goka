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
	"github.com/lovoo/goka/internal/test"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
)

// Tests the processor option WithHotStandby. This requires a (local) running kafka cluster.
// Two processors are initialized, one plain, the other with hotstandby enabled.
// after initializing and sending a message to each partition, we verify that the hot-standby-processor
// has both values in the respective storage
func TestHotStandby(t *testing.T) {
	t.Parallel()
	var (
		group       goka.Group = goka.Group(fmt.Sprintf("%s-%d", "goka-systemtest-hotstandby", time.Now().Unix()))
		inputStream string     = string(group) + "-input"
		table                  = string(goka.GroupTable(group))
		joinTable   goka.Table = goka.Table(fmt.Sprintf("%s-%d", "goka-systemtest-hotstandby-join", time.Now().Unix()))
	)

	brokers := initSystemTest(t)

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	cfg := goka.DefaultConfig()
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	test.AssertNil(t, err)

	err = tm.EnsureStreamExists(inputStream, 2)
	test.AssertNil(t, err)
	err = tm.EnsureTableExists(string(joinTable), 2)
	test.AssertNil(t, err)

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
	test.AssertNil(t, err)

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

	test.AssertNil(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errg, ctx := multierr.NewErrGroup(ctx)

	errg.Go(func() error {
		return proc1.Run(ctx)
	})

	errg.Go(func() error {
		return proc2.Run(ctx)
	})

	pollTimed(t, "procs 1&2 recovered", 10.0, proc1.Recovered, proc2.Recovered)

	// check the storages that were initalized by the processors:
	// proc1 is without hotstandby -> only two storages: (1 for the table, 1 for the join)
	// proc2 uses hotstandby --> 4 storages (2 for table, 2 for join)
	test.AssertEqual(t, len(proc1Storages.storages), 2)
	test.AssertEqual(t, len(proc2Storages.storages), 4)

	inputEmitter, err := goka.NewEmitter(brokers, goka.Stream(inputStream), new(codec.String))
	test.AssertNil(t, err)
	defer inputEmitter.Finish()
	inputEmitter.EmitSync("key1", "message1")
	inputEmitter.EmitSync("key2", "message2")

	// emit something into the join table (like simulating a processor ctx.SetValue()).
	// Our test processors should update their value in the join-table
	joinEmitter, err := goka.NewEmitter(brokers, goka.Stream(joinTable), new(codec.String))
	test.AssertNil(t, err)
	defer joinEmitter.Finish()
	joinEmitter.EmitSync("key1", "joinval1")
	joinEmitter.EmitSync("key2", "joinval2")

	// determine the partitions for both keys, assert they're not equal
	// (note that the keys might have to be changed if goka's default hasher changes)
	partx := hashKey("key1", 2)
	party := hashKey("key2", 2)
	test.AssertNotEqual(t, partx, party)

	// get the corresponding storages for both table and join-partitions
	tableStorage1 := proc2Storages.storages[proc2Storages.key(string(table), partx)]
	tableStorage2 := proc2Storages.storages[proc2Storages.key(string(table), party)]
	joinStorage1 := proc2Storages.storages[proc2Storages.key(string(joinTable), partx)]
	joinStorage2 := proc2Storages.storages[proc2Storages.key(string(joinTable), party)]

	// wait until the keys are present
	pollTimed(t, "key-values are present", 2,
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
	test.AssertEqual(t, string(val1), "message1")
	test.AssertEqual(t, string(val2), "message2")

	// check the join-values
	joinval1, _ := joinStorage1.Get("key1")
	joinval2, _ := joinStorage2.Get("key2")
	test.AssertEqual(t, string(joinval1), "joinval1")
	test.AssertEqual(t, string(joinval2), "joinval2")

	// stop everything and wait until it's shut down
	cancel()
	test.AssertNil(t, errg.Wait().ErrorOrNil())
}

// Tests the processor option WithRecoverAhead. This requires a (local) running kafka cluster.
// Two processors are initialized, but they share an input topic which has only one partition. This
// Test makes sure that still both processors recover the views/tables
func TestRecoverAhead(t *testing.T) {
	t.Parallel()
	brokers := initSystemTest(t)

	var (
		group       goka.Group = "goka-systemtest-recoverahead"
		inputStream string     = string(group) + "-input"
		table                  = string(goka.GroupTable(group))
		joinTable   goka.Table = "goka-systemtest-recoverahead-join"
	)

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	cfg := goka.DefaultConfig()
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	test.AssertNil(t, err)

	err = tm.EnsureStreamExists(inputStream, 1)
	test.AssertNil(t, err)
	err = tm.EnsureTableExists(string(joinTable), 1)
	test.AssertNil(t, err)

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
	test.AssertNil(t, err)

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

	// emit something into the join table (like simulating a processor ctx.SetValue()).
	// Our test processors should update their value in the join-table
	joinEmitter, err := goka.NewEmitter(brokers, goka.Stream(joinTable), new(codec.String))
	test.AssertNil(t, err)
	defer joinEmitter.Finish()
	joinEmitter.EmitSync("key1", "joinval1")

	// emit something into the join table (like simulating a processor ctx.SetValue()).
	// Our test processors should update their value in the join-table
	tableEmitter, err := goka.NewEmitter(brokers, goka.Stream(table), new(codec.String))
	test.AssertNil(t, err)
	defer tableEmitter.Finish()
	tableEmitter.EmitSync("key1", "tableval1")

	test.AssertNil(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errg, ctx := multierr.NewErrGroup(ctx)

	errg.Go(func() error {
		return proc1.Run(ctx)
	})

	errg.Go(func() error {
		return proc2.Run(ctx)
	})

	pollTimed(t, "procs 1&2 recovered", 10.0, proc1.Recovered, proc2.Recovered)

	// check the storages that were initalized by the processors:
	// proc1 is without hotstandby -> only two storages: (1 for the table, 1 for the join)
	// proc2 uses hotstandby --> 4 storages (2 for table, 2 for join)
	test.AssertEqual(t, len(proc1Storages.storages), 2)
	test.AssertEqual(t, len(proc2Storages.storages), 2)

	// get the corresponding storages for both table and join-partitions
	tableStorage1 := proc2Storages.storages[proc1Storages.key(string(table), 0)]
	tableStorage2 := proc2Storages.storages[proc2Storages.key(string(table), 0)]
	joinStorage1 := proc1Storages.storages[proc1Storages.key(string(joinTable), 0)]
	joinStorage2 := proc2Storages.storages[proc2Storages.key(string(joinTable), 0)]

	// wait until the keys are present
	pollTimed(t, "key-values are present", 2,
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
	test.AssertEqual(t, string(val1), "tableval1")
	test.AssertEqual(t, string(val2), "tableval1")

	// check the join-values
	joinval1, _ := joinStorage1.Get("key1")
	joinval2, _ := joinStorage2.Get("key1")
	test.AssertEqual(t, string(joinval1), "joinval1")
	test.AssertEqual(t, string(joinval2), "joinval1")

	// stop everything and wait until it's shut down
	cancel()
	test.AssertNil(t, errg.Wait().ErrorOrNil())
}

// TestRebalance runs some processors to test rebalance. It's merely a
// runs-without-errors test, not a real functional test.
func TestRebalance(t *testing.T) {
	t.Parallel()
	brokers := initSystemTest(t)

	var (
		group       goka.Group = "goka-systemtest-rebalance"
		inputStream string     = string(group) + "-input"
		basepath               = "/tmp/goka-rebalance-test"
	)

	test.AssertNil(t, os.RemoveAll(basepath))

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	cfg := goka.DefaultConfig()
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	test.AssertNil(t, err)

	err = tm.EnsureStreamExists(inputStream, 20)
	test.AssertNil(t, err)

	em, err := goka.NewEmitter(brokers, goka.Stream(inputStream), new(codec.String))
	test.AssertNil(t, err)

	go func() {
		defer em.Finish()
		i := 0
		for {
			i++
			test.AssertNil(t, em.EmitSync(fmt.Sprintf("%d", i), "value"))
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

		test.AssertNil(t, err)
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

	test.AssertNil(t, errg.Wait().ErrorOrNil())
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
	test.AssertNil(t, err)

	err = tm.EnsureStreamExists(inputStream, 10)
	test.AssertNil(t, err)

	em, err := goka.NewEmitter(brokers, goka.Stream(inputStream), new(codec.Int64))
	test.AssertNil(t, err)
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
	test.AssertNil(t, err)

	proc, cancel, done := runProc(proc)

	pollTimed(t, "recovered", 5, proc.Recovered)

	go func() {
		for i := 0; i < 10000; i++ {
			em.Emit(fmt.Sprintf("key-%d", i), int64(1))
		}
	}()

	defer cancel()
	pollTimed(t, "error-response", 1, func() bool {
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
		group       goka.Group = "goka-systemtest-slow-callback-fail"
		inputStream string     = string(group) + "-input"
	)

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	cfg := goka.DefaultConfig()
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	test.AssertNil(t, err)

	err = tm.EnsureStreamExists(inputStream, 2)
	test.AssertNil(t, err)

	em, err := goka.NewEmitter(brokers, goka.Stream(inputStream), new(codec.Int64))
	test.AssertNil(t, err)

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

	test.AssertNil(t, err)

	errg, ctx := multierr.NewErrGroup(context.Background())

	errg.Go(func() error {
		ticker := time.NewTicker(10 * time.Microsecond)
		defer em.Finish()
		var i int64
		for {
			select {
			case <-ticker.C:
				i++
				test.AssertNil(t, em.EmitSync(fmt.Sprintf("%d", i%20), i))
			case <-ctx.Done():
				return nil
			}
		}
	})
	errg.Go(func() error {
		return proc.Run(ctx)
	})
	err = errg.Wait().ErrorOrNil()
	test.AssertTrue(t, strings.Contains(err.Error(), "panic in callback"))
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
	test.AssertNil(t, err)

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
	test.AssertNil(t, err)

	tm.EnsureStreamExists(string(inputStream), 10)

	for i := 0; i < numMessages; i++ {
		emitter.EmitSync("1", int64(1))
	}
	// close emitter
	test.AssertNil(t, emitter.Finish())

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
		test.AssertNil(t, err)
		// run a new processor
		go func() {
			defer close(done)
			err := proc.Run(ctx)
			test.AssertNil(t, err)
		}()

		time.Sleep(10 * time.Second)
		val, err := proc.Get("1")
		test.AssertNil(t, err)
		test.AssertTrue(t, val != nil)
		test.AssertEqual(t, val.(int64), int64(numMessages))

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
	test.AssertNil(t, err)

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
	test.AssertNil(t, err)

	tm.EnsureStreamExists(string(inputStream), 10)

	emitterCtx, cancelEmitter := context.WithCancel(context.Background())
	emitterDone := make(chan struct{})

	var valueSum = make(map[string]int64)
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
		test.AssertNil(t, err)
		return proc
	}

	proc, cancelProc, procDone := runProc(createProc())

	pollTimed(t, "proc-running", 10, proc.Recovered)

	// stop it
	cancelProc()
	test.AssertNil(t, <-procDone)

	// start it again --> must fail
	_, _, procDone = runProc(proc)
	test.AssertNotNil(t, <-procDone)

	// start it 10 more times in a row and stop it
	for i := 0; i < 10; i++ {
		log.Printf("creating proc round %d", i)
		proc, cancelProc, procDone := runProc(createProc())
		pollTimed(t, "proc-running", 10, proc.Recovered)
		// stop it
		cancelProc()
		test.AssertNil(t, <-procDone)

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
	test.AssertNil(t, <-procDone)

}
