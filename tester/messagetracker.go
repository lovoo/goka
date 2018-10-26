package tester

// MessageTracker tracks message offsets for each topic for convenient
// 'expect message x to be in topic y' in unit tests
type MessageTracker struct {
	t                T
	topic            string
	nextMessageIndex int
	tester           *Tester
}

func newMessageTracker(tester *Tester, t T, topic string) *MessageTracker {
	return &MessageTracker{
		t:      t,
		topic:  topic,
		tester: tester,
	}
}

// Next returns the next message since the last time this
// function was called (or MoveToEnd)
// It uses the known codec for the topic to decode the message
func (mt *MessageTracker) Next() (string, interface{}, bool) {

	key, msgRaw, hasNext := mt.NextRaw()

	if !hasNext {
		return key, msgRaw, hasNext
	}

	decoded, err := mt.tester.codecForTopic(mt.topic).Decode(msgRaw)
	if err != nil {
		mt.t.Fatalf("Error decoding message: %v", err)
	}
	return key, decoded, true
}

// NextRaw returns the next message similar to Next(), but without the decoding
func (mt *MessageTracker) NextRaw() (string, []byte, bool) {
	q := mt.tester.queueForTopic(mt.topic)
	if mt.nextMessageIndex >= q.size() {
		return "", nil, false
	}
	msg := q.message(mt.nextMessageIndex)

	mt.nextMessageIndex++
	return msg.key, msg.value, true
}

// ExpectLastMessage ensures the message tracker is at the end of the topic
func (mt *MessageTracker) ExpectAtEnd() {
	if mt.nextMessageIndex == mt.tester.queueForTopic(mt.topic).size() {
		return
	}

	codec := mt.tester.codecForTopic(mt.topic)
	var remaining []interface{}
	for _, msg := range mt.tester.queueForTopic(mt.topic).messagesFrom(mt.nextMessageIndex) {
		decoded, _ := codec.Decode(msg.value)
		remaining = append(remaining, decoded)
	}
	mt.t.Fatalf("Expected topic %s to be empty, but was not (%#v)", mt.topic, remaining)
}

// MoveToEnd marks the topic to be read regardless of its content
func (mt *MessageTracker) MoveToEnd(topic string) {
	mt.nextMessageIndex = int(mt.tester.queueForTopic(mt.topic).hwm)
}
