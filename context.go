package goka

import (
	"fmt"
	"sync"

	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/storage"
)

// Context provides access to the processor's table and emit capabilities to
// arbitrary topics in kafka.
// Upon arrival of a message from subscribed topics, the respective
// ConsumeCallback is invoked with a context object along
// with the input message.
type Context interface {
	// Topic returns the topic of input message.
	Topic() string

	// Key returns the key of the input message.
	Key() string

	// Value returns the value of the key in the group table.
	Value() (interface{}, error)

	// SetValue updates the value of the key in the group table.
	SetValue(value interface{}) error

	// Join returns the value of key in the copartitioned table. Only
	// subscribed tables are accessible with Join.
	Join(table string) (interface{}, error)

	// Emit asynchronously writes a message into a topic.
	Emit(topic string, key string, value []byte) error

	// Loopback asynchronously sends a message to another key of the group
	// table. Value passed to loopback is encoded via the codec given in the
	// Loop subscription.
	Loopback(key string, value interface{}) error

	// Fail stops execution and shuts down the processor
	Fail(err error)
}

type emitter func(topic string, key string, value []byte) *kafka.Promise

type context struct {
	loopTopic  Subscription
	tableTopic Subscription

	commit  func()
	emitter emitter
	failer  func(err error)

	storage storage.Storage
	codec   codec.Codec
	views   map[string]*partition

	msg      *message
	done     bool
	ackSent  bool
	counters struct {
		calls     int32
		callsDone int32
	}
	m  sync.Mutex
	wg *sync.WaitGroup
}

// Emit sends a message asynchronously to a topic.
func (ctx *context) Emit(topic string, key string, value []byte) error {
	if ctx.loopTopic.is(topic) {
		return fmt.Errorf("Cannot emit to loop topic, use Loopback() instead.")
	}
	if ctx.tableTopic.is(topic) {
		return fmt.Errorf("Cannot emit to table topic, use SetValue() instead.")
	}

	return ctx.emit(topic, key, value)
}

// Loopback sends a message to another key of the processor.
func (ctx *context) Loopback(key string, value interface{}) error {
	if ctx.loopTopic.null() {
		return fmt.Errorf("No loop topic configured")
	}

	data, err := ctx.loopTopic.codec.Encode(key, value)
	if err != nil {
		return fmt.Errorf("Error encoding message for key %s: %v", key, err)
	}

	return ctx.emit(ctx.loopTopic.Name, key, data)
}

func (ctx *context) emit(topic string, key string, value []byte) error {
	if topic == "" {
		return fmt.Errorf("Cannot emit to emtpy topic (%s)", topic)
	}
	if ctx.tableTopic.is(topic) {
		return fmt.Errorf("Cannot emit to consumer group state (%s)", topic)
	}
	ctx.incCalls()

	ctx.emitter(topic, key, value).Then(func(err error) {
		// first notify our callback-counters so the consumer can
		// acknowledge the message consumption
		ctx.notifyCallDone()
	})
	return nil
}

// Value returns the value of the key in the group table.
func (ctx *context) Value() (interface{}, error) {
	return ctx.valueForKey(string(ctx.msg.Key))
}

// SetValue updates the value of the key in the group table.
func (ctx *context) SetValue(value interface{}) error {
	return ctx.setValueForKey(string(ctx.msg.Key), value)
}

func (ctx *context) Key() string {
	return string(ctx.msg.Key)
}

func (ctx *context) Topic() string {
	return string(ctx.msg.Topic)
}

func (ctx *context) Join(table string) (interface{}, error) {
	if ctx.views == nil {
		return nil, fmt.Errorf("table %s not subscribed", table)
	}
	v, ok := ctx.views[table]
	if !ok {
		return nil, fmt.Errorf("table %s not subscribed", table)
	}
	return v.st.Get(ctx.Key())
}

// Has returns true if key has a value in the processor state, otherwise false.
func (ctx *context) Has(key string) (bool, error) {
	if ctx.storage == nil {
		return false, fmt.Errorf("Cannot access state in stateless processor")
	}

	return ctx.storage.Has(key)
}

// valueForKey returns the value of key in the processor state.
func (ctx *context) valueForKey(key string) (interface{}, error) {
	if ctx.storage == nil {
		return nil, fmt.Errorf("Cannot access state in stateless processor")
	}

	return ctx.storage.Get(key)
}

// setValueForKey sets a value for a key in the processor state.
func (ctx *context) setValueForKey(key string, value interface{}) error {
	if ctx.storage == nil {
		return fmt.Errorf("Cannot access state in stateless processor")
	}

	if value == nil {
		return fmt.Errorf("Cannot set nil as value.")
	}

	err := ctx.storage.Set(key, value)
	if err != nil {
		return fmt.Errorf("Error storing value: %v", err)
	}

	encodedValue, err := ctx.codec.Encode(key, value)
	if err != nil {
		return fmt.Errorf("Error encoding value: %v", err)
	}

	// increment wait counter
	ctx.incCalls()
	// write it to the log.
	ctx.emitter(ctx.tableTopic.Name, key, encodedValue).Then(func(err error) {
		if err != nil {
			return
		}

		// log the new offset if the write call was successful
		if err = ctx.storage.SetOffset(ctx.msg.Offset); err != nil {
			ctx.failer(fmt.Errorf("Error writing the log-offset to local storage for recovery: %v", err))
		}

	}).Then(func(err error) {
		ctx.notifyCallDone()
	})
	return nil
}

func (ctx *context) incCalls() {
	ctx.m.Lock()
	defer ctx.m.Unlock()
	ctx.counters.calls++
}

// mark the context as done and check if calls are done to
// send the ack upstream
func (ctx *context) markDone() bool {
	ctx.m.Lock()
	defer ctx.m.Unlock()

	ctx.done = true
	return ctx.sendAckIfDone()
}

func (ctx *context) notifyCallDone() {
	ctx.m.Lock()
	defer ctx.m.Unlock()

	ctx.counters.callsDone++
	ctx.sendAckIfDone()
}

// sends an ACK to the upstream consumer when all calls have been successful and the
// consumer-handler was finished.
// the caller is responsible for locking the object!
func (ctx *context) sendAckIfDone() bool {
	if !ctx.done {
		return false
	}
	// not all calls are done yet, do not send the ack upstream.
	if ctx.counters.calls > ctx.counters.callsDone {
		return false
	}

	if ctx.ackSent {
		return false
	}
	ctx.ackSent = true

	// finally, if all calls are done we'll send the message-ack back to kafka.
	ctx.commit()

	// no further callback will be called from this context
	ctx.wg.Done()

	return true
}

// Fail stops execution and shuts down the processor
func (ctx *context) Fail(err error) {
	panic(err)
}
