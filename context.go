package goka

import (
	"errors"
	"fmt"
	"sync"
	"time"

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
	Topic() Stream

	// Key returns the key of the input message.
	Key() string

	// Value returns the value of the key in the group table.
	Value() interface{}

	// SetValue updates the value of the key in the group table.
	SetValue(value interface{})

	// Delete deletes a value from the group table. IMPORTANT: this deletes the
	// value associated with the key from both the local cache and the persisted
	// table in Kafka.
	Delete()

	// Timestamp returns the timestamp of the input message. If the timestamp is
	// invalid, a zero time will be returned.
	Timestamp() time.Time

	// Join returns the value of key in the copartitioned table.
	Join(topic Table) interface{}

	// Lookup returns the value of key in the view of table.
	Lookup(topic Table, key string) interface{}

	// Emit asynchronously writes a message into a topic.
	Emit(topic Stream, key string, value interface{})

	// Loopback asynchronously sends a message to another key of the group
	// table. Value passed to loopback is encoded via the codec given in the
	// Loop subscription.
	Loopback(key string, value interface{})

	// Fail stops execution and shuts down the processor
	Fail(err error)
}

type emitter func(topic string, key string, value []byte) *kafka.Promise

type context struct {
	graph *GroupGraph

	commit  func()
	emitter emitter
	failer  func(err error)

	storage storage.Storage
	pviews  map[string]*partition
	views   map[string]*View

	pstats *PartitionStats

	msg      *message
	done     bool
	counters struct {
		emits  int
		dones  int
		stores int
	}
	errors Errors
	m      sync.Mutex
	wg     *sync.WaitGroup
}

// Emit sends a message asynchronously to a topic.
func (ctx *context) Emit(topic Stream, key string, value interface{}) {
	if topic == "" {
		ctx.Fail(errors.New("Cannot emit to empty topic"))
	}
	if loopName(ctx.graph.Group()) == string(topic) {
		ctx.Fail(errors.New("Cannot emit to loop topic, use Loopback() instead."))
	}
	if tableName(ctx.graph.Group()) == string(topic) {
		ctx.Fail(errors.New("Cannot emit to table topic, use SetValue() instead."))
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

	ctx.emit(string(topic), key, data)
}

// Loopback sends a message to another key of the processor.
func (ctx *context) Loopback(key string, value interface{}) {
	l := ctx.graph.LoopStream()
	if l == nil {
		ctx.Fail(errors.New("No loop topic configured"))
	}

	data, err := l.Codec().Encode(value)
	if err != nil {
		ctx.Fail(fmt.Errorf("Error encoding message for key %s: %v", key, err))
	}

	ctx.emit(l.Topic(), key, data)
}

func (ctx *context) emit(topic string, key string, value []byte) {
	ctx.counters.emits++
	ctx.emitter(topic, key, value).Then(func(err error) {
		if err != nil {
			err = fmt.Errorf("error emitting to %s: %v", topic, err)
		}
		ctx.emitDone(err)
	})

	s := ctx.pstats.Output[topic]
	s.Count++
	s.Bytes += len(value)
	ctx.pstats.Output[topic] = s
}

func (ctx *context) Delete() {
	if err := ctx.deleteKey(ctx.Key()); err != nil {
		ctx.Fail(err)
	}
}

// Value returns the value of the key in the group table.
func (ctx *context) Value() interface{} {
	val, err := ctx.valueForKey(ctx.msg.Key)
	if err != nil {
		ctx.Fail(err)
	}
	return val
}

// SetValue updates the value of the key in the group table.
func (ctx *context) SetValue(value interface{}) {
	if err := ctx.setValueForKey(string(ctx.msg.Key), value); err != nil {
		ctx.Fail(err)
	}
}

// Timestamp returns the timestamp of the input message.
func (ctx *context) Timestamp() time.Time {
	return ctx.msg.Timestamp
}

func (ctx *context) Key() string {
	return string(ctx.msg.Key)
}

func (ctx *context) Topic() Stream {
	return Stream(ctx.msg.Topic)
}

func (ctx *context) Join(topic Table) interface{} {
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

func (ctx *context) Lookup(topic Table, key string) interface{} {
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
func (ctx *context) valueForKey(key string) (interface{}, error) {
	if ctx.storage == nil {
		return nil, fmt.Errorf("Cannot access state in stateless processor")
	}

	data, err := ctx.storage.Get(key)
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

func (ctx *context) deleteKey(key string) error {
	if ctx.graph.GroupTable() == nil {
		return fmt.Errorf("Cannot access state in stateless processor")
	}

	ctx.counters.stores++
	if err := ctx.storage.Delete(key); err != nil {
		return fmt.Errorf("error deleting key (%s) from storage: %v", key, err)
	}

	ctx.counters.emits++
	ctx.emitter(ctx.graph.GroupTable().Topic(), key, nil).Then(func(err error) {
		ctx.emitDone(err)
	})

	return nil
}

// setValueForKey sets a value for a key in the processor state.
func (ctx *context) setValueForKey(key string, value interface{}) error {
	if ctx.graph.GroupTable() == nil {
		return fmt.Errorf("Cannot access state in stateless processor")
	}

	if value == nil {
		return fmt.Errorf("Cannot set nil as value.")
	}

	encodedValue, err := ctx.graph.GroupTable().Codec().Encode(value)
	if err != nil {
		return fmt.Errorf("error encoding value: %v", err)
	}

	ctx.counters.stores++
	if err = ctx.storage.Set(key, encodedValue); err != nil {
		return fmt.Errorf("error storing value: %v", err)
	}

	table := ctx.graph.GroupTable().Topic()
	ctx.counters.emits++
	ctx.emitter(table, key, encodedValue).Then(func(err error) {
		ctx.emitDone(err)
	})

	s := ctx.pstats.Output[table]
	s.Count++
	s.Bytes += len(encodedValue)
	ctx.pstats.Output[table] = s

	return nil
}

func (ctx *context) emitDone(err error) {
	ctx.m.Lock()
	defer ctx.m.Unlock()
	ctx.counters.dones++
	ctx.tryCommit(err)
}

// called after all emits
func (ctx *context) finish() {
	ctx.m.Lock()
	defer ctx.m.Unlock()
	ctx.done = true
	ctx.tryCommit(nil)
}

// called before any emit
func (ctx *context) start() {
	ctx.wg.Add(1)
}

// calls ctx.commit once all emits have successfully finished, or fails context
// if some emit failed.
func (ctx *context) tryCommit(err error) {
	if err != nil {
		ctx.errors.collect(err)
	}

	// not all calls are done yet, do not send the ack upstream.
	if !ctx.done || ctx.counters.emits > ctx.counters.dones {
		return
	}

	// commit if no errors, otherwise fail context
	if ctx.errors.hasErrors() {
		ctx.failer(fmt.Errorf("error emitting to %s: %v", ctx.graph.GroupTable().Topic(), ctx.errors.Error()))
	} else {
		ctx.commit()
	}

	// no further callback will be called from this context
	ctx.wg.Done()
}

// Fail stops execution and shuts down the processor
func (ctx *context) Fail(err error) {
	panic(err)
}
