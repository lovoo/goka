package goka

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/storage"
	"github.com/stretchr/testify/require"
)

// TestProcessMessageCommitsInOffsetOrder pins the ordering property the
// commit path depends on: committing a message acknowledges every
// earlier message of that topic, so no message may commit while an
// earlier one is still in flight.
//
// Two messages are dispatched in offset order, as the run loop
// dispatches them. Message 0's callback emits and its emit is held, so
// the callback returns with the emit outstanding and tryCommit
// declines to commit. Message 1's callback emits nothing, so nothing
// holds its commit back — and since createMessageCommitter marks
// msg.offset+1, committing message 1 acknowledges message 0 too.
//
// Message 0's emit then fails, as a broker unavailable past its retry
// budget would fail it. tryCommit routes that to the asyncFailer and
// does not commit — but if message 1 has already been committed,
// message 0's offset is acknowledged anyway and it will never be
// redelivered. The loss is silent: the offsets are indistinguishable
// from a run in which message 0 succeeded.
func TestProcessMessageCommitsInOffsetOrder(t *testing.T) {
	const inputTopic = "input"

	var (
		mu        sync.Mutex
		committed []int64
		asyncErrs []error
	)

	graph := DefineGroup("commit-order",
		Input(Stream(inputTopic), new(codec.String), func(ctx Context, msg any) {
			// Message 0 emits; message 1 does not. Every ctx.SetValue
			// in a stateful processor is an emit, so "a callback that
			// emits" is the ordinary case rather than a contrived one.
			if ctx.Key() == "emits" {
				ctx.Emit("output", ctx.Key(), msg)
			}
		}),
		Output(Stream("output"), new(codec.String)),
	)

	held, finishHeld := NewPromiseWithFinisher()
	prod := &heldEmitProducer{hold: "emits", held: held}

	opts := new(poptions)
	require.NoError(t, opts.applyOptions(graph,
		WithStorageBuilder(storage.MemoryBuilder()),
	))

	pp := newPartitionProcessor(0, graph,
		func(msg *message, meta string) {
			mu.Lock()
			defer mu.Unlock()
			committed = append(committed, msg.offset)
		},
		defaultLogger, opts, runModeActive, nil, nil, prod, nil, nil, 0)

	asyncFailer := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		asyncErrs = append(asyncErrs, err)
	}
	syncFailer := func(err error) { t.Errorf("unexpected synchronous failure: %v", err) }

	encoded, err := new(codec.String).Encode("v")
	require.NoError(t, err)

	var wg sync.WaitGroup
	ctx := context.Background()

	// Offset 0: emits, and the emit is held.
	require.NoError(t, pp.processMessage(ctx, &wg,
		&message{topic: inputTopic, partition: 0, offset: 0, key: "emits", value: encoded},
		syncFailer, asyncFailer))

	mu.Lock()
	require.Empty(t, committed, "message 0 must not commit while its emit is outstanding")
	mu.Unlock()

	// Offset 1: emits nothing, so nothing defers its commit.
	require.NoError(t, pp.processMessage(ctx, &wg,
		&message{topic: inputTopic, partition: 0, offset: 1, key: "no-emits", value: encoded},
		syncFailer, asyncFailer))

	// Message 0's emit fails.
	finishHeld(nil, errors.New("broker unavailable past retry budget"))
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, asyncErrs, "message 0's failed emit must be reported")

	for _, offset := range committed {
		require.Less(t, offset, int64(1),
			"message at offset %d was committed, which acknowledges message 0 — whose emit "+
				"failed, and which will therefore never be redelivered", offset)
	}
}

// heldEmitProducer holds the emits for one key so a test can decide
// when, and whether, they succeed.
type heldEmitProducer struct {
	hold string
	held *Promise
}

func (p *heldEmitProducer) Emit(topic, key string, value []byte) *Promise {
	return p.EmitWithHeaders(topic, key, value, nil)
}

func (p *heldEmitProducer) EmitWithHeaders(topic, key string, value []byte, headers Headers) *Promise {
	if key == p.hold {
		return p.held
	}
	done, finish := NewPromiseWithFinisher()
	finish(nil, nil)
	return done
}

func (p *heldEmitProducer) Close() error { return nil }
