package systemtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/internal/test"
	"github.com/lovoo/goka/multierr"
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
		joinTable   goka.Table = goka.Table(fmt.Sprintf("%s-%d", "goka-systemtest-hotstandby-join", time.Now().Unix()))
	)

	if !*systemtest {
		t.Skipf("Ignoring systemtest. pass '-args -systemtest' to `go test` to include them")
	}

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	cfg := goka.DefaultConfig()
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)([]string{*broker})
	test.AssertNil(t, err)

	err = tm.EnsureStreamExists(inputStream, 2)
	test.AssertNil(t, err)
	err = tm.EnsureTableExists(string(joinTable), 2)
	test.AssertNil(t, err)

	proc1Storages := newStorageTracker()

	proc1, err := goka.NewProcessor([]string{*broker},
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

	proc2, err := goka.NewProcessor([]string{*broker},
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

	inputEmitter, err := goka.NewEmitter([]string{*broker}, goka.Stream(inputStream), new(codec.String))
	test.AssertNil(t, err)
	defer inputEmitter.Finish()
	inputEmitter.EmitSync("key1", "message1")
	inputEmitter.EmitSync("key2", "message2")

	// emit something into the join table (like simulating a processor ctx.SetValue()).
	// Our test processors should update their value in the join-table
	joinEmitter, err := goka.NewEmitter([]string{*broker}, goka.Stream(joinTable), new(codec.String))
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
	test.AssertNil(t, errg.Wait().NilOrError())
}

// Tests the processor option WithRecoverAhead. This requires a (local) running kafka cluster.
// Two processors are initialized, but they share an input topic which has only one partition. This
// Test makes sure that still both processors recover the views/tables
func TestRecoverAhead(t *testing.T) {

	var (
		group       goka.Group = "goka-systemtest-recoverahead"
		inputStream string     = string(group) + "-input"
		table                  = string(goka.GroupTable(group))
		joinTable   goka.Table = "goka-systemtest-recoverahead-join"
	)

	if !*systemtest {
		t.Skipf("Ignoring systemtest. pass '-args -systemtest' to `go test` to include them")
	}

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	cfg := goka.DefaultConfig()
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)([]string{*broker})
	test.AssertNil(t, err)

	err = tm.EnsureStreamExists(inputStream, 1)
	test.AssertNil(t, err)
	err = tm.EnsureTableExists(string(joinTable), 1)
	test.AssertNil(t, err)

	proc1Storages := newStorageTracker()

	proc1, err := goka.NewProcessor([]string{*broker},
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

	proc2, err := goka.NewProcessor([]string{*broker},
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
	joinEmitter, err := goka.NewEmitter([]string{*broker}, goka.Stream(joinTable), new(codec.String))
	test.AssertNil(t, err)
	defer joinEmitter.Finish()
	joinEmitter.EmitSync("key1", "joinval1")

	// emit something into the join table (like simulating a processor ctx.SetValue()).
	// Our test processors should update their value in the join-table
	tableEmitter, err := goka.NewEmitter([]string{*broker}, goka.Stream(table), new(codec.String))
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
	test.AssertNil(t, errg.Wait().NilOrError())
}
