package kafka

import (
	"fmt"
	"time"
)

// Event abstracts different types of events from the kafka consumer like BOF/EOF/Error or an actual message
type Event interface {
	string() string
}

// Assignment represents a partition:offset assignment for the current connection
type Assignment map[int32]int64

func (a *Assignment) string() string {
	var am map[int32]int64 = *a
	return fmt.Sprintf("Assignment %v", am)
}

// EOF marks the end of the log of a topic/partition.
type EOF struct {
	Topic     string
	Partition int32
	Hwm       int64
}

func (e *EOF) string() string {
	return fmt.Sprintf("EOF %s/%d:%d", e.Topic, e.Partition, e.Hwm)
}

// BOF marks the beginning of a topic/partition.
type BOF struct {
	Topic     string
	Partition int32
	Offset    int64
	Hwm       int64
}

func (e *BOF) string() string {
	return fmt.Sprintf("BOF %s/%d:%d->%d", e.Topic, e.Partition, e.Offset, e.Hwm)
}

// Message represents a message from kafka containing
// extra information like topic, partition and offset for convenience
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time

	Key   string
	Value []byte
}

func (m *Message) string() string {
	return fmt.Sprintf("Message %s/%d:%d %s=%v", m.Topic, m.Partition, m.Offset, m.Key, m.Value)
}

// Error from kafka wrapped to be conform with the Event-Interface
type Error struct {
	Err error
}

func (e *Error) string() string {
	return e.Err.Error()
}

// NOP does not carry any information. Useful for debugging.
type NOP struct {
	Topic     string
	Partition int32
}

func (n *NOP) string() string {
	return "nop"
}
