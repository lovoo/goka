package goka

import "sync"

type pendingCommit struct {
	msg  *message
	meta string
	done bool
}

// commitOrderer forwards offset commits to the wrapped commitCallback in offset
// order, separately for every input topic.
//
// Committing message N acknowledges every message of that topic before it, and a
// marked offset only ever moves forward. Completion is out of order though, since
// a callback that finishes asynchronously completes after messages dispatched
// behind it. Committing in completion order would therefore acknowledge earlier
// messages that are still in flight.
//
// Messages of one topic are dispatched in offset order, so a FIFO queue per topic
// whose completed prefix is released keeps the committed offset at or below the
// first unprocessed message.
type commitOrderer struct {
	m       sync.Mutex
	commit  commitCallback
	pending map[string][]*pendingCommit
}

func newCommitOrderer(commit commitCallback) *commitOrderer {
	return &commitOrderer{
		commit:  commit,
		pending: make(map[string][]*pendingCommit),
	}
}

// track registers msg as in flight and returns the function that commits it. The
// returned function may be called from any goroutine, but track must be called in
// offset order, i.e. from the partition processor's run loop.
func (c *commitOrderer) track(msg *message) func(meta string) {
	c.m.Lock()
	defer c.m.Unlock()

	pending := &pendingCommit{msg: msg}
	c.pending[msg.topic] = append(c.pending[msg.topic], pending)

	// a message is committed exactly once, so ignore repeated calls
	var once sync.Once
	return func(meta string) {
		once.Do(func() { c.complete(pending, meta) })
	}
}

// complete marks pending as ready and commits the longest completed prefix of its
// topic's queue.
func (c *commitOrderer) complete(pending *pendingCommit, meta string) {
	c.m.Lock()
	defer c.m.Unlock()

	pending.meta = meta
	pending.done = true

	topic := pending.msg.topic
	queue := c.pending[topic]

	var released int
	for released < len(queue) && queue[released].done {
		c.commit(queue[released].msg, queue[released].meta)
		released++
	}

	switch {
	// an earlier message is still in flight
	case released == 0:
		return
	case released == len(queue):
		delete(c.pending, topic)
	default:
		// copy instead of reslicing, so the released messages and the payloads they
		// reference are not retained by the backing array
		remaining := make([]*pendingCommit, len(queue)-released)
		copy(remaining, queue[released:])
		c.pending[topic] = remaining
	}
}
