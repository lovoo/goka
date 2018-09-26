package tester

// MessageTracker tracks message offsets for each topic for convenient
// 'expect message x to be in topic y' in unit tests
type MessageTracker struct {
	t                T
	nextMessageIndex map[string]int
	tester           *Tester
}

func newMessageTracker(tester *Tester, t T) *MessageTracker {
	return &MessageTracker{
		t:                t,
		nextMessageIndex: make(map[string]int),
		tester:           tester,
	}
}

// NextMessage returns the next message from the topic since the last time this
// function was called (or MoveToEnd)
func (mt *MessageTracker) NextMessage(topic string) (string, interface{}, bool) {

	key, msgRaw, hasNext := mt.NextMessageRaw(topic)

	if !hasNext {
		return key, msgRaw, hasNext
	}

	decoded, err := mt.tester.codecForTopic(topic).Decode(msgRaw)
	if err != nil {
		mt.t.Fatalf("Error decoding message: %v", err)
	}
	return key, decoded, true
}

// NextMessageRaw returns the next message in passed topic
func (mt *MessageTracker) NextMessageRaw(topic string) (string, []byte, bool) {
	q := mt.tester.queueForTopic(topic)
	if mt.nextMessageIndex[topic] >= q.size() {
		return "", nil, false
	}
	msg := q.message(mt.nextMessageIndex[topic])

	mt.nextMessageIndex[topic]++
	return msg.key, msg.value, true
}

// ExpectEmpty ensures the topic does not contain more messages
func (mt *MessageTracker) ExpectEmpty(topic string) {
	if mt.nextMessageIndex[topic] == mt.tester.queueForTopic(topic).size() {
		return
	}

	codec := mt.tester.codecForTopic(topic)
	var remaining []interface{}
	for _, msg := range mt.tester.queueForTopic(topic).messagesFrom(mt.nextMessageIndex[topic]) {
		decoded, _ := codec.Decode(msg.value)
		remaining = append(remaining, decoded)
	}
	mt.t.Fatalf("Expected topic %s to be empty, but was not (%#v)", topic, remaining)
}

// MoveToEnd marks the topic to be read regardless of its content
func (mt *MessageTracker) MoveToEnd(topic string) {
	mt.nextMessageIndex[topic] = int(mt.tester.queueForTopic(topic).hwm)
}

// MoveAllToEnd marks all topics to be read
func (mt *MessageTracker) MoveAllToEnd() {
	for topic := range mt.tester.topicQueues {
		mt.nextMessageIndex[topic] = int(mt.tester.queueForTopic(topic).hwm)
	}
}

// ExpectEmit ensures a message exists in passed topic and key. The message may be
// inspected/unmarshalled by a passed expecter function.
// DEPRECATED: This function is only to get some compatibility and should be removed in future
func (mt *MessageTracker) ExpectEmit(topic string, key string, expecter func(value []byte)) {

	for {
		nextKey, value, hasNext := mt.NextMessageRaw(topic)
		if !hasNext {
			break
		}

		if key != nextKey {
			continue
		}

		// found one, stop here
		expecter(value)
		return
	}
	mt.t.Errorf("Did not find expected message in %s for key %s", topic, key)
}
