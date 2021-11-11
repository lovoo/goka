package goka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
)

type emitter func(topic string, key string, value []byte, headers Headers) *Promise

// Context provides access to the processor's table and emit capabilities to
// arbitrary topics in kafka.
// Upon arrival of a message from subscribed topics, the respective
// ConsumeCallback is invoked with a context object along with the input message.
// The context is only valid within the callback, do not store it or pass it to other goroutines.
//
// Error handling
//
// Most methods of the context can fail due to different reasons, which are handled in different ways:
// Synchronous errors like
// * wrong codec for topic (a message cannot be marshalled or unmarshalled)
// * Emit to a topic without the Output definition in the group graph
// * Value/SetValue without defining Persist in the group graph
// * Join/Lookup without the definition in the group graph etc..
// will result in a panic to stop the callback immediately and shutdown the processor.
// This is necessary to preserve integrity of the processor and avoid further actions.
// Do not recover from that panic, otherwise the goroutine will deadlock.
//
// Retrying synchronous errors must be implemented by restarting the processor.
// If errors must be tolerated (which is not advisable because they're usually persistent), provide
// fail-tolerant versions of the producer, storage or codec as needed.
//
// Asynchronous errors can occur when the callback has been finished, but e.g. sending a batched
// message to kafka fails due to connection errors or leader election in the cluster.
// Those errors still shutdown the processor but will not result in a panic in the callback.
type Context interface {
	// Topic returns the topic of input message.
	Topic() Stream

	// Key returns the key of the input message.
	Key() string

	// Partition returns the partition of the input message.
	Partition() int32

	// Offset returns the offset of the input message.
	Offset() int64

	// Group returns the group of the input message
	Group() Group

	// Value returns the value of the key in the group table.
	//
	// This method might panic to initiate an immediate shutdown of the processor
	// to maintain data integrity. Do not recover from that panic or
	// the processor might deadlock.
	Value() interface{}

	// Headers returns the headers of the input message
	Headers() Headers

	// SetValue updates the value of the key in the group table.
	// It stores the value in the local cache and sends the
	// update to the Kafka topic representing the group table.
	//
	// This method might panic to initiate an immediate shutdown of the processor
	// to maintain data integrity. Do not recover from that panic or
	// the processor might deadlock.
	SetValue(value interface{}, options ...ContextOption)

	// Delete deletes a value from the group table. IMPORTANT: this deletes the
	// value associated with the key from both the local cache and the persisted
	// table in Kafka.
	//
	// This method might panic to initiate an immediate shutdown of the processor
	// to maintain data integrity. Do not recover from that panic or
	// the processor might deadlock.
	Delete(options ...ContextOption)

	// Timestamp returns the timestamp of the input message. If the timestamp is
	// invalid, a zero time will be returned.
	Timestamp() time.Time

	// Join returns the value of key in the copartitioned table.
	//
	// This method might panic to initiate an immediate shutdown of the processor
	// to maintain data integrity. Do not recover from that panic or
	// the processor might deadlock.
	Join(topic Table) interface{}

	// Lookup returns the value of key in the view of table.
	//
	// This method might panic to initiate an immediate shutdown of the processor
	// to maintain data integrity. Do not recover from that panic or
	// the processor might deadlock.
	Lookup(topic Table, key string) interface{}

	// Emit asynchronously writes a message into a topic.
	//
	// This method might panic to initiate an immediate shutdown of the processor
	// to maintain data integrity. Do not recover from that panic or
	// the processor might deadlock.
	Emit(topic Stream, key string, value interface{}, options ...ContextOption)

	// Loopback asynchronously sends a message to another key of the group
	// table. Value passed to loopback is encoded via the codec given in the
	// Loop subscription.
	//
	// This method might panic to initiate an immediate shutdown of the processor
	// to maintain data integrity. Do not recover from that panic or
	// the processor might deadlock.
	Loopback(key string, value interface{}, options ...ContextOption)

	// Fail stops execution and shuts down the processor
	// The callback is stopped immediately by panicking. Do not recover from that panic or
	// the processor might deadlock.
	Fail(err error)

	// Context returns the underlying context used to start the processor or a
	// subcontext. Returned context.Context can safely be passed to asynchronous code and goroutines.
	Context() context.Context

	// DeferCommit makes the callback omit the final commit when the callback returns.
	// It returns a function that *must* be called eventually to mark the message processing as finished.
	// If the function is not called, the processor might reprocess the message in future.
	// Note when calling DeferCommit multiple times, all returned functions must be called.
	// *Important*: the context where `DeferCommit` is called, is only safe to use within this callback,
	// never pass it into asynchronous code or goroutines.
	DeferCommit() func(error)
}

type message struct {
	key       string
	timestamp time.Time
	topic     string
	offset    int64
	partition int32
	headers   []*sarama.RecordHeader
	value     []byte
}

type cbContext struct {
	ctx   context.Context
	graph *GroupGraph
	// commit commits the message in the consumer session
	commit func()

	emitter               emitter
	emitterDefaultHeaders Headers

	asyncFailer func(err error)
	syncFailer  func(err error)

	// Headers as passed from sarama. Note that this field will be filled
	// lazily after the first call to Headers
	headers Headers

	table *PartitionTable
	// joins
	pviews map[string]*PartitionTable
	// lookup tables
	views map[string]*View

	// helper function that is provided by the partition processor to allow
	// tracking statistics for the output topic
	trackOutputStats func(ctx context.Context, topic string, size int)

	msg      *message
	done     bool
	counters struct {
		emits  int
		dones  int
		stores int
	}
	errors *multierror.Error
	m      sync.Mutex
	wg     *sync.WaitGroup
}

// Emit sends a message asynchronously to a topic.
func (ctx *cbContext) Emit(topic Stream, key string, value interface{}, options ...ContextOption) {
	opts := new(ctxOptions)
	opts.applyOptions(options...)
	if topic == "" {
		ctx.Fail(errors.New("cannot emit to empty topic"))
	}
	if loopName(ctx.graph.Group()) == string(topic) {
		ctx.Fail(errors.New("cannot emit to loop topic (use Loopback instead)"))
	}
	if tableName(ctx.graph.Group()) == string(topic) {
		ctx.Fail(errors.New("cannot emit to table topic (use SetValue instead)"))
	}
	if !ctx.graph.isOutputTopic(topic) {
		ctx.Fail(fmt.Errorf("topic %s is not configured for output. Did you specify goka.Output(..) when defining the processor?", topic))
	}

	c := ctx.graph.codec(string(topic))
	if c == nil {
		ctx.Fail(fmt.Errorf("no codec for topic %s", topic))
	}

	var data []byte
	if value != nil {
		var err error
		data, err = c.Encode(value)
		if err != nil {
			ctx.Fail(fmt.Errorf("error encoding message for topic %s: %v", topic, err))
		}
	}

	ctx.emit(string(topic), key, data, opts.emitHeaders)
}

// Loopback sends a message to another key of the processor.
func (ctx *cbContext) Loopback(key string, value interface{}, options ...ContextOption) {
	opts := new(ctxOptions)
	opts.applyOptions(options...)
	l := ctx.graph.LoopStream()
	if l == nil {
		ctx.Fail(errors.New("no loop topic configured"))
	}

	data, err := l.Codec().Encode(value)
	if err != nil {
		ctx.Fail(fmt.Errorf("error encoding message for key %s: %v", key, err))
	}

	ctx.emit(l.Topic(), key, data, opts.emitHeaders)
}

func (ctx *cbContext) emit(topic string, key string, value []byte, headers Headers) {
	ctx.counters.emits++
	ctx.emitter(topic, key, value, ctx.emitterDefaultHeaders.Merged(headers)).Then(func(err error) {
		if err != nil {
			err = fmt.Errorf("error emitting to %s: %v", topic, err)
		}
		ctx.emitDone(err)
	})
	ctx.trackOutputStats(ctx.ctx, topic, len(value))
}

func (ctx *cbContext) Delete(options ...ContextOption) {
	opts := new(ctxOptions)
	opts.applyOptions(options...)
	if err := ctx.deleteKey(ctx.Key(), opts.emitHeaders); err != nil {
		ctx.Fail(err)
	}
}

// Value returns the value of the key in the group table.
func (ctx *cbContext) Value() interface{} {
	val, err := ctx.valueForKey(ctx.Key())
	if err != nil {
		ctx.Fail(err)
	}
	return val
}

// SetValue updates the value of the key in the group table.
func (ctx *cbContext) SetValue(value interface{}, options ...ContextOption) {
	opts := new(ctxOptions)
	opts.applyOptions(options...)
	if err := ctx.setValueForKey(ctx.Key(), value, opts.emitHeaders); err != nil {
		ctx.Fail(err)
	}
}

// Timestamp returns the timestamp of the input message.
func (ctx *cbContext) Timestamp() time.Time {
	return ctx.msg.timestamp
}

func (ctx *cbContext) Key() string {
	return ctx.msg.key
}

func (ctx *cbContext) Topic() Stream {
	return Stream(ctx.msg.topic)
}

func (ctx *cbContext) Offset() int64 {
	return ctx.msg.offset
}

func (ctx *cbContext) Group() Group {
	return ctx.graph.Group()
}

func (ctx *cbContext) Partition() int32 {
	return ctx.msg.partition
}

func (ctx *cbContext) Headers() Headers {
	if ctx.headers == nil {
		ctx.headers = HeadersFromSarama(ctx.msg.headers)
	}
	return ctx.headers
}

func (ctx *cbContext) Join(topic Table) interface{} {
	if ctx.pviews == nil {
		ctx.Fail(fmt.Errorf("table %s not subscribed", topic))
	}
	v, ok := ctx.pviews[string(topic)]
	if !ok {
		ctx.Fail(fmt.Errorf("table %s not subscribed", topic))
	}
	data, err := v.st.Get(ctx.Key())
	if err != nil {
		ctx.Fail(fmt.Errorf("error getting key %s of table %s: %v", ctx.Key(), topic, err))
	} else if data == nil {
		return nil
	}

	value, err := ctx.graph.codec(string(topic)).Decode(data)
	if err != nil {
		ctx.Fail(fmt.Errorf("error decoding value key %s of table %s: %v", ctx.Key(), topic, err))
	}
	return value
}

func (ctx *cbContext) Lookup(topic Table, key string) interface{} {
	if ctx.views == nil {
		ctx.Fail(fmt.Errorf("topic %s not subscribed", topic))
	}
	v, ok := ctx.views[string(topic)]
	if !ok {
		ctx.Fail(fmt.Errorf("topic %s not subscribed", topic))
	}
	val, err := v.Get(key)
	if err != nil {
		ctx.Fail(fmt.Errorf("error getting key %s of table %s: %v", key, topic, err))
	}
	return val
}

// valueForKey returns the value of key in the processor state.
func (ctx *cbContext) valueForKey(key string) (interface{}, error) {
	if ctx.table == nil {
		return nil, fmt.Errorf("Cannot access state in stateless processor")
	}

	data, err := ctx.table.Get(key)
	if err != nil {
		return nil, fmt.Errorf("error reading value: %v", err)
	} else if data == nil {
		return nil, nil
	}

	value, err := ctx.graph.GroupTable().Codec().Decode(data)
	if err != nil {
		return nil, fmt.Errorf("error decoding value: %v", err)
	}
	return value, nil
}

func (ctx *cbContext) deleteKey(key string, headers Headers) error {
	if ctx.graph.GroupTable() == nil {
		return fmt.Errorf("Cannot access state in stateless processor")
	}

	ctx.counters.stores++
	if err := ctx.table.Delete(key); err != nil {
		return fmt.Errorf("error deleting key (%s) from storage: %v", key, err)
	}

	ctx.counters.emits++
	ctx.emitter(ctx.graph.GroupTable().Topic(), key, nil, headers).Then(func(err error) {
		ctx.emitDone(err)
	})

	return nil
}

// setValueForKey sets a value for a key in the processor state.
func (ctx *cbContext) setValueForKey(key string, value interface{}, headers Headers) error {
	if ctx.graph.GroupTable() == nil {
		return fmt.Errorf("Cannot access state in stateless processor")
	}

	if value == nil {
		return fmt.Errorf("cannot set nil as value")
	}

	encodedValue, err := ctx.graph.GroupTable().Codec().Encode(value)
	if err != nil {
		return fmt.Errorf("error encoding value: %v", err)
	}

	ctx.counters.stores++
	if err = ctx.table.Set(key, encodedValue); err != nil {
		return fmt.Errorf("error storing value: %v", err)
	}

	table := ctx.graph.GroupTable().Topic()
	ctx.counters.emits++
	ctx.emitter(table, key, encodedValue, headers).ThenWithMessage(func(msg *sarama.ProducerMessage, err error) {
		if err == nil && msg != nil && msg.Offset != 0 {
			err = ctx.table.SetOffset(msg.Offset)
		}
		ctx.emitDone(err)
	})

	// for a table write we're tracking both the diskwrites and the kafka output
	ctx.trackOutputStats(ctx.ctx, table, len(encodedValue))
	ctx.table.TrackMessageWrite(ctx.ctx, len(encodedValue))

	return nil
}

func (ctx *cbContext) emitDone(err error) {
	ctx.m.Lock()
	defer ctx.m.Unlock()
	ctx.counters.dones++
	ctx.tryCommit(err)
}

// called after all emits
func (ctx *cbContext) finish(err error) {
	ctx.m.Lock()
	defer ctx.m.Unlock()
	ctx.done = true
	ctx.tryCommit(err)
}

// called before any emit
func (ctx *cbContext) start() {
	ctx.wg.Add(1)
}

// calls ctx.commit once all emits have successfully finished, or fails context
// if some emit failed.
// this function must be called from a locked function.
func (ctx *cbContext) tryCommit(err error) {
	if err != nil {
		ctx.errors = multierror.Append(ctx.errors, err)
	}

	// not all calls are done yet, do not send the ack upstream.
	if !ctx.done || ctx.counters.emits > ctx.counters.dones {
		return
	}

	// commit if no errors, otherwise fail context
	if ctx.errors.ErrorOrNil() != nil {
		ctx.asyncFailer(ctx.errors.ErrorOrNil())
	} else {
		ctx.commit()
	}

	ctx.markDone()
}

// markdone marks the context as done
func (ctx *cbContext) markDone() {
	ctx.wg.Done()
}

// Fail stops execution and shuts down the processor
func (ctx *cbContext) Fail(err error) {
	ctx.syncFailer(err)
}

func (ctx *cbContext) Context() context.Context {
	return ctx.ctx
}

func (ctx *cbContext) DeferCommit() func(err error) {
	ctx.m.Lock()
	defer ctx.m.Unlock()
	ctx.counters.emits++

	var once sync.Once

	return func(err error) {
		once.Do(func() {
			ctx.emitDone(err)
		})
	}
}
