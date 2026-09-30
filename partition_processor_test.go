package goka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lovoo/goka/codec"
	"github.com/stretchr/testify/require"
)

// newTestPartitionProcessor creates a minimal stateless partition processor that
// records the offsets it commits instead of marking them in a consumer group
// session.
func newTestPartitionProcessor(t *testing.T, cb ProcessCallback) (*PartitionProcessor, func() []int64) {
	t.Helper()

	opts := new(poptions)
	opts.log = defaultLogger
	opts.contextWrapper = func(ctx Context) Context { return ctx }
	opts.partitionChannelSize = defaultPartitionChannelSize

	var (
		mCommitted sync.Mutex
		committed  []int64
	)
	commit := func(msg *message, meta string) {
		mCommitted.Lock()
		defer mCommitted.Unlock()
		committed = append(committed, msg.offset)
	}

	ctrl, bm := createMockBuilder(t)
	t.Cleanup(ctrl.Finish)

	pp := newPartitionProcessor(0,
		DefineGroup("test-group", Input("input", new(codec.Int64), cb)),
		commit, defaultLogger, opts, runModeActive,
		nil, NewMockAutoConsumer(t, nil), bm.producer, bm.tmgr,
		NewSimpleBackoff(defaultBackoffStep, defaultBackoffMax), time.Minute)

	return pp, func() []int64 {
		mCommitted.Lock()
		defer mCommitted.Unlock()
		return append([]int64(nil), committed...)
	}
}

// runPartitionProcessor starts pp.run and returns a function that waits for it
// to terminate, returning its error.
func runPartitionProcessor(t *testing.T, pp *PartitionProcessor, ctx context.Context) func() error {
	t.Helper()

	runDone := make(chan error, 1)
	go func() { runDone <- pp.run(ctx) }()

	return func() error {
		t.Helper()
		select {
		case err := <-runDone:
			return err
		case <-time.After(30 * time.Second):
			t.Fatal("partition processor did not shut down")
			return nil
		}
	}
}

// TestPartitionProcessor_FailWithCancelledContext verifies that Context.Fail
// aborts the callback and skips the commit even when the partition context has
// already been cancelled, e.g. because a rebalance revoked the partition while
// the callback was in flight.
//
// Fail used to return normally in that situation, which let processMessage reach
// msgContext.finish and commit the offset of a message that was never processed
// successfully. The message was then never redelivered to the new owner of the
// partition.
func TestPartitionProcessor_FailWithCancelledContext(t *testing.T) {
	var (
		callbackEntered  = make(chan struct{})
		resumedAfterFail bool
	)

	cb := func(ctx Context, msg interface{}) {
		close(callbackEntered)
		// emulate a blocking request that is aborted when the partition context
		// is cancelled
		<-ctx.Context().Done()
		ctx.Fail(fmt.Errorf("request aborted: %w", ctx.Context().Err()))
		resumedAfterFail = true
	}

	pp, committed := newTestPartitionProcessor(t, cb)

	ctx, cancel := context.WithCancel(context.Background())
	waitRun := runPartitionProcessor(t, pp, ctx)

	pp.input <- &message{topic: "input", partition: 0, offset: 42, key: "key", value: []byte("1")}

	<-callbackEntered
	cancel()

	require.Error(t, waitRun(), "run must report the processing error")
	require.False(t, resumedAfterFail, "Fail must stop the callback by panicking")
	require.Empty(t, committed(), "offset of a failed message must not be committed")
}

// TestPartitionProcessor_SuccessWithCancelledContext verifies the counterpart of
// TestPartitionProcessor_FailWithCancelledContext: a callback that completes
// successfully has already emitted its messages and written its table updates,
// so its offset must be committed even if the partition context was cancelled
// meanwhile. Withholding the commit would turn completed work into a guaranteed
// reprocessing by the next owner of the partition.
func TestPartitionProcessor_SuccessWithCancelledContext(t *testing.T) {
	callbackEntered := make(chan struct{})

	cb := func(ctx Context, msg interface{}) {
		close(callbackEntered)
		// the partition is revoked while the callback runs, but the callback does
		// not depend on the context and completes successfully
		<-ctx.Context().Done()
	}

	pp, committed := newTestPartitionProcessor(t, cb)

	ctx, cancel := context.WithCancel(context.Background())
	waitRun := runPartitionProcessor(t, pp, ctx)

	pp.input <- &message{topic: "input", partition: 0, offset: 7, key: "key", value: []byte("1")}

	<-callbackEntered
	cancel()
	waitRun()

	require.Equal(t, []int64{7}, committed(), "successfully processed message must be committed")
}
