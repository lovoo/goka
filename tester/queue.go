package tester

import (
	"sync"

	"github.com/lovoo/goka"
)

type message struct {
	offset  int64
	key     string
	value   []byte
	headers goka.Headers
}

type queue struct {
	sync.Mutex
	topic    string
	messages []*message
	hwm      int64
}

func newQueue(topic string) *queue {

	return &queue{
		topic: topic,
	}
}

func (q *queue) Hwm() int64 {
	q.Lock()
	defer q.Unlock()

	hwm := q.hwm
	return hwm
}

func (q *queue) push(key string, value []byte, headers goka.Headers) int64 {
	q.Lock()
	defer q.Unlock()
	offset := q.hwm
	q.messages = append(q.messages, &message{
		offset:  offset,
		key:     key,
		value:   value,
		headers: headers,
	})
	q.hwm++
	return offset
}

func (q *queue) message(offset int) *message {
	q.Lock()
	defer q.Unlock()
	return q.messages[offset]
}

func (q *queue) messagesFromOffset(offset int64) []*message {
	q.Lock()
	defer q.Unlock()
	return q.messages[offset:]
}

func (q *queue) size() int {
	q.Lock()
	defer q.Unlock()
	return len(q.messages)
}

// QueueTracker tracks message offsets for each topic for convenient
// 'expect message x to be in topic y' in unit tests
type QueueTracker struct {
	t          T
	topic      string
	nextOffset int64
	tester     *Tester
}

func newQueueTracker(tester *Tester, t T, topic string) *QueueTracker {
	return &QueueTracker{
		t:          t,
		topic:      topic,
		tester:     tester,
		nextOffset: tester.getOrCreateQueue(topic).hwm,
	}
}

// Next returns the next message since the last time this
// function was called (or MoveToEnd)
// It uses the known codec for the topic to decode the message
func (mt *QueueTracker) Next() (string, interface{}, bool) {
	_, key, msg, hasNext := mt.NextWithHeaders()
	return key, msg, hasNext
}

// NextWithHeaders returns the next message since the last time this
// function was called (or MoveToEnd).  This includes headers
// It uses the known codec for the topic to decode the message
func (mt *QueueTracker) NextWithHeaders() (goka.Headers, string, interface{}, bool) {
	headers, key, msgRaw, hasNext := mt.NextRawWithHeaders()

	if !hasNext {
		return headers, key, msgRaw, hasNext
	}

	decoded, err := mt.tester.codecForTopic(mt.topic).Decode(msgRaw)
	if err != nil {
		mt.t.Fatalf("Error decoding message: %v", err)
	}
	return headers, key, decoded, true
}

// NextRaw returns the next message similar to Next(), but without the decoding
func (mt *QueueTracker) NextRaw() (string, []byte, bool) {
	_, key, value, hasNext := mt.NextRawWithHeaders()
	return key, value, hasNext
}

// NextRawWithHeaders returns the next message similar to Next(), but without the decoding
func (mt *QueueTracker) NextRawWithHeaders() (goka.Headers, string, []byte, bool) {
	q := mt.tester.getOrCreateQueue(mt.topic)
	if int(mt.nextOffset) >= q.size() {
		return nil, "", nil, false
	}
	msg := q.message(int(mt.nextOffset))

	mt.nextOffset++
	return msg.headers, msg.key, msg.value, true
}

// Seek moves the index pointer of the queue tracker to passed offset
func (mt *QueueTracker) Seek(offset int64) {
	mt.nextOffset = offset
}

// Hwm returns the tracked queue's hwm value
func (mt *QueueTracker) Hwm() int64 {
	return mt.tester.getOrCreateQueue(mt.topic).Hwm()
}

// NextOffset returns the tracker's next offset
func (mt *QueueTracker) NextOffset() int64 {
	return mt.nextOffset
}
