package goka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/lovoo/goka/multierr"
)

const (
	// TODO: what is this used for?
	anyOffset int64 = -1000
)

var (
	errOutOfExpectations           = fmt.Errorf("error out of expectations")
	errPartitionConsumerNotStarted = fmt.Errorf("error partition consumer not started")
)

// MockAutoConsumer implements sarama's Consumer interface for testing purposes.
// Before you can start consuming from this consumer, you have to register
// topic/partitions using ExpectConsumePartition, and set expectations on them.
type MockAutoConsumer struct {
	l                  sync.Mutex
	t                  *testing.T
	config             *sarama.Config
	partitionConsumers map[string]map[int32]*MockAutoPartitionConsumer
	metadata           map[string][]int32
}

// NewMockAutoConsumer returns a new mock Consumer instance. The t argument should
// be the *testing.T instance of your test method. An error will be written to it if
// an expectation is violated. The config argument can be set to nil.
func NewMockAutoConsumer(t *testing.T, config *sarama.Config) *MockAutoConsumer {
	if config == nil {
		config = sarama.NewConfig()
	}

	c := &MockAutoConsumer{
		t:                  t,
		config:             config,
		partitionConsumers: make(map[string]map[int32]*MockAutoPartitionConsumer),
	}
	return c
}

///////////////////////////////////////////////////
// Consumer interface implementation
///////////////////////////////////////////////////

// ConsumePartition implements the ConsumePartition method from the sarama.Consumer interface.
// Before you can start consuming a partition, you have to set expectations on it using
// ExpectConsumePartition. You can only consume a partition once per consumer.
func (c *MockAutoConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.partitionConsumers[topic] == nil || c.partitionConsumers[topic][partition] == nil {
		c.t.Errorf("No expectations set for %s/%d", topic, partition)
		return nil, errOutOfExpectations
	}

	pc := c.partitionConsumers[topic][partition]
	if pc.consumed {
		return nil, sarama.ConfigurationError("The topic/partition is already being consumed")
	}

	if pc.offset != anyOffset && pc.offset != offset {
		c.t.Errorf("Unexpected offset when calling ConsumePartition for %s/%d. Expected %d, got %d.", topic, partition, pc.offset, offset)
	}

	pc.consumed = true
	return pc, nil
}

// Topics returns a list of topics, as registered with SetTopicMetadata
func (c *MockAutoConsumer) Topics() ([]string, error) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.metadata == nil {
		c.t.Errorf("Unexpected call to Topics. Initialize the mock's topic metadata with SetTopicMetadata.")
		return nil, sarama.ErrOutOfBrokers
	}

	var result []string
	for topic := range c.metadata {
		result = append(result, topic)
	}
	return result, nil
}

// Partitions returns the list of partitions for the given topic, as registered with SetTopicMetadata
func (c *MockAutoConsumer) Partitions(topic string) ([]int32, error) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.metadata == nil {
		c.t.Errorf("Unexpected call to Partitions. Initialize the mock's topic metadata with SetTopicMetadata.")
		return nil, sarama.ErrOutOfBrokers
	}
	if c.metadata[topic] == nil {
		return nil, sarama.ErrUnknownTopicOrPartition
	}

	return c.metadata[topic], nil
}

// HighWaterMarks returns a map of high watermarks for each topic/partition
func (c *MockAutoConsumer) HighWaterMarks() map[string]map[int32]int64 {
	c.l.Lock()
	defer c.l.Unlock()

	hwms := make(map[string]map[int32]int64, len(c.partitionConsumers))
	for topic, partitionConsumers := range c.partitionConsumers {
		hwm := make(map[int32]int64, len(partitionConsumers))
		for partition, pc := range partitionConsumers {
			hwm[partition] = pc.HighWaterMarkOffset()
		}
		hwms[topic] = hwm
	}

	return hwms
}

// Close implements the Close method from the sarama.Consumer interface. It will close
// all registered PartitionConsumer instances.
func (c *MockAutoConsumer) Close() error {
	c.l.Lock()
	defer c.l.Unlock()

	for _, partitions := range c.partitionConsumers {
		for _, partitionConsumer := range partitions {
			partitionConsumer.Close()
		}
	}

	return nil
}

func (c *MockAutoConsumer) Pause(topicPartitions map[string][]int32) {}

func (c *MockAutoConsumer) Resume(topicPartitions map[string][]int32) {}

func (c *MockAutoConsumer) PauseAll() {}

func (c *MockAutoConsumer) ResumeAll() {}

///////////////////////////////////////////////////
// Expectation API
///////////////////////////////////////////////////

// SetTopicMetadata sets the clusters topic/partition metadata,
// which will be returned by Topics() and Partitions().
func (c *MockAutoConsumer) SetTopicMetadata(metadata map[string][]int32) {
	c.l.Lock()
	defer c.l.Unlock()

	c.metadata = metadata
}

// ExpectConsumePartition will register a topic/partition, so you can set expectations on it.
// The registered PartitionConsumer will be returned, so you can set expectations
// on it using method chaining. Once a topic/partition is registered, you are
// expected to start consuming it using ConsumePartition. If that doesn't happen,
// an error will be written to the error reporter once the mock consumer is closed. It will
// also expect that the
func (c *MockAutoConsumer) ExpectConsumePartition(topic string, partition int32, offset int64) *MockAutoPartitionConsumer {
	c.l.Lock()
	defer c.l.Unlock()

	if c.partitionConsumers[topic] == nil {
		c.partitionConsumers[topic] = make(map[int32]*MockAutoPartitionConsumer)
	}

	if c.partitionConsumers[topic][partition] == nil {
		c.partitionConsumers[topic][partition] = &MockAutoPartitionConsumer{
			t:         c.t,
			topic:     topic,
			partition: partition,
			offset:    offset,
			messages:  make(chan *sarama.ConsumerMessage, c.config.ChannelBufferSize),
			errors:    make(chan *sarama.ConsumerError, c.config.ChannelBufferSize),
		}
	}

	return c.partitionConsumers[topic][partition]
}

///////////////////////////////////////////////////
// PartitionConsumer mock type
///////////////////////////////////////////////////

// MockAutoPartitionConsumer implements sarama's PartitionConsumer interface for testing purposes.
// It is returned by the mock Consumers ConsumePartitionMethod, but only if it is
// registered first using the Consumer's ExpectConsumePartition method. Before consuming the
// Errors and Messages channel, you should specify what values will be provided on these
// channels using YieldMessage and YieldError.
type MockAutoPartitionConsumer struct {
	highWaterMarkOffset     int64 // must be at the top of the struct because https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	l                       sync.Mutex
	t                       *testing.T
	topic                   string
	partition               int32
	offset                  int64
	messages                chan *sarama.ConsumerMessage
	errors                  chan *sarama.ConsumerError
	singleClose             sync.Once
	consumed                bool
	errorsShouldBeDrained   bool
	messagesShouldBeDrained bool
}

///////////////////////////////////////////////////
// PartitionConsumer interface implementation
///////////////////////////////////////////////////

// AsyncClose implements the AsyncClose method from the sarama.PartitionConsumer interface.
func (pc *MockAutoPartitionConsumer) AsyncClose() {
	pc.singleClose.Do(func() {
		close(pc.messages)
		close(pc.errors)
		pc.consumed = false
	})
}

// Close implements the Close method from the sarama.PartitionConsumer interface. It will
// verify whether the partition consumer was actually started.
func (pc *MockAutoPartitionConsumer) Close() error {
	var err error
	pc.singleClose.Do(func() {
		if !pc.consumed {
			pc.t.Errorf("Expectations set on %s/%d, but no partition consumer was started.", pc.topic, pc.partition)
			err = errPartitionConsumerNotStarted
			return
		}

		if pc.errorsShouldBeDrained && len(pc.errors) > 0 {
			pc.t.Errorf("Expected the errors channel for %s/%d to be drained on close, but found %d errors.", pc.topic, pc.partition, len(pc.errors))
		}

		if pc.messagesShouldBeDrained && len(pc.messages) > 0 {
			pc.t.Errorf("Expected the messages channel for %s/%d to be drained on close, but found %d messages.", pc.topic, pc.partition, len(pc.messages))
		}

		pc.AsyncClose()

		var (
			closeErr error
			wg       sync.WaitGroup
		)

		wg.Add(1)
		go func() {
			defer wg.Done()

			errs := make(sarama.ConsumerErrors, 0)
			for err := range pc.errors {
				errs = append(errs, err)
			}

			if len(errs) > 0 {
				closeErr = errs
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for range pc.messages {
				// drain
			}
		}()

		wg.Wait()
		err = closeErr
	})
	return err
}

// Errors implements the Errors method from the sarama.PartitionConsumer interface.
func (pc *MockAutoPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return pc.errors
}

// Messages implements the Messages method from the sarama.PartitionConsumer interface.
func (pc *MockAutoPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return pc.messages
}

// HighWaterMarkOffset returns the highwatermark for the partition
func (pc *MockAutoPartitionConsumer) HighWaterMarkOffset() int64 {
	return atomic.LoadInt64(&pc.highWaterMarkOffset)
}

///////////////////////////////////////////////////
// Expectation API
///////////////////////////////////////////////////

// YieldMessage will yield a messages Messages channel of this partition consumer
// when it is consumed. By default, the mock consumer will not verify whether this
// message was consumed from the Messages channel, because there are legitimate
// reasons forthis not to happen. ou can call ExpectMessagesDrainedOnClose so it will
// verify that the channel is empty on close.
func (pc *MockAutoPartitionConsumer) YieldMessage(msg *sarama.ConsumerMessage) {
	pc.l.Lock()
	defer pc.l.Unlock()

	msg.Topic = pc.topic
	msg.Partition = pc.partition
	msg.Offset = atomic.LoadInt64(&pc.highWaterMarkOffset)
	atomic.AddInt64(&pc.highWaterMarkOffset, 1)

	pc.messages <- msg
}

// YieldError will yield an error on the Errors channel of this partition consumer
// when it is consumed. By default, the mock consumer will not verify whether this error was
// consumed from the Errors channel, because there are legitimate reasons for this
// not to happen. You can call ExpectErrorsDrainedOnClose so it will verify that
// the channel is empty on close.
func (pc *MockAutoPartitionConsumer) YieldError(err error) {
	pc.errors <- &sarama.ConsumerError{
		Topic:     pc.topic,
		Partition: pc.partition,
		Err:       err,
	}
}

// ExpectMessagesDrainedOnClose sets an expectation on the partition consumer
// that the messages channel will be fully drained when Close is called. If this
// expectation is not met, an error is reported to the error reporter.
func (pc *MockAutoPartitionConsumer) ExpectMessagesDrainedOnClose() {
	pc.messagesShouldBeDrained = true
}

// ExpectErrorsDrainedOnClose sets an expectation on the partition consumer
// that the errors channel will be fully drained when Close is called. If this
// expectation is not met, an error is reported to the error reporter.
func (pc *MockAutoPartitionConsumer) ExpectErrorsDrainedOnClose() {
	pc.errorsShouldBeDrained = true
}

// Pause suspends fetching from this partition. Future calls to the broker will not return
// any records from these partition until it have been resumed using Resume().
// Note that this method does not affect partition subscription.
// In particular, it does not cause a group rebalance when automatic assignment is used.
func (pc *MockAutoPartitionConsumer) Pause() {
}

// Resume resumes this partition which have been paused with Pause().
// New calls to the broker will return records from these partitions if there are any to be fetched.
// If the partition was not previously paused, this method is a no-op.
func (pc *MockAutoPartitionConsumer) Resume() {
}

// IsPaused indicates if this partition consumer is paused or not
func (pc *MockAutoPartitionConsumer) IsPaused() bool {
	return false
}

// MockConsumerGroupSession mocks the consumer group session used for testing
type MockConsumerGroupSession struct {
	ctx        context.Context
	generation int32
	topics     []string
	claims     map[string]*MockConsumerGroupClaim

	mu sync.RWMutex

	consumerGroup *MockConsumerGroup
}

// MockConsumerGroupClaim mocks the consumergroupclaim
type MockConsumerGroupClaim struct {
	topic     string
	partition int32
	msgs      chan *sarama.ConsumerMessage
}

// NewMockConsumerGroupClaim creates a new mocksconsumergroupclaim
func NewMockConsumerGroupClaim(topic string, partition int32) *MockConsumerGroupClaim {
	return &MockConsumerGroupClaim{
		topic:     topic,
		partition: partition,
		msgs:      make(chan *sarama.ConsumerMessage),
	}
}

// Topic returns the current topic of the claim
func (cgc *MockConsumerGroupClaim) Topic() string {
	return cgc.topic
}

// Partition returns the partition
func (cgc *MockConsumerGroupClaim) Partition() int32 {
	return cgc.partition
}

// InitialOffset returns the initial offset
func (cgc *MockConsumerGroupClaim) InitialOffset() int64 {
	return 0
}

// HighWaterMarkOffset returns the hwm offset
func (cgc *MockConsumerGroupClaim) HighWaterMarkOffset() int64 {
	return 0
}

// Messages returns the message channel that must be
func (cgc *MockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return cgc.msgs
}

func newConsumerGroupSession(ctx context.Context, generation int32, cg *MockConsumerGroup, topics []string) *MockConsumerGroupSession {
	return &MockConsumerGroupSession{
		ctx:           ctx,
		generation:    generation,
		consumerGroup: cg,
		topics:        topics,
		claims:        make(map[string]*MockConsumerGroupClaim),
	}
}

// Claims returns the number of partitions assigned in the group session for each topic
func (cgs *MockConsumerGroupSession) Claims() map[string][]int32 {
	claims := make(map[string][]int32)
	for _, topic := range cgs.topics {
		claims[topic] = []int32{0}
	}
	return claims
}

func (cgs *MockConsumerGroupSession) createGroupClaim(topic string, partition int32) *MockConsumerGroupClaim {
	cgs.mu.Lock()
	defer cgs.mu.Unlock()

	cgs.claims[topic] = NewMockConsumerGroupClaim(topic, 0)

	return cgs.claims[topic]
}

// SendMessage sends a message to the consumer
func (cgs *MockConsumerGroupSession) SendMessage(msg *sarama.ConsumerMessage) {
	cgs.mu.RLock()
	defer cgs.mu.RUnlock()

	for topic, claim := range cgs.claims {
		if topic == msg.Topic {
			claim.msgs <- msg
		}
	}
}

// MemberID returns the member ID
// TODO: clarify what that actually means and whether we need to mock that somehow
func (cgs *MockConsumerGroupSession) MemberID() string {
	panic("MemberID not provided by mock")
}

// GenerationID returns the generation ID of the group consumer
func (cgs *MockConsumerGroupSession) GenerationID() int32 {
	return cgs.generation
}

// MarkOffset marks the passed offset consumed in topic/partition
func (cgs *MockConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	cgs.consumerGroup.markMessage(topic, partition, offset, metadata)
}

// Commit the offset to the backend
func (cgs *MockConsumerGroupSession) Commit() {
	panic("not implemented")
}

// ResetOffset resets the offset to be consumed from
func (cgs *MockConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	panic("reset offset is not implemented by the mock")
}

// MarkMessage marks the passed message as consumed
func (cgs *MockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	panic("not implemented")
}

// Context returns the consumer group's context
func (cgs *MockConsumerGroupSession) Context() context.Context {
	return cgs.ctx
}

// MockConsumerGroup mocks the consumergroup
type MockConsumerGroup struct {
	errs chan error

	mu sync.RWMutex

	// setting this makes the consume call fail with this error for testing
	failOnConsume error

	// use the same offset counter for all topics
	offset            int64
	currentGeneration int32

	// messages we sent to the consumergroup and need to wait for
	mMessages  sync.Mutex
	messages   map[int64]int64
	wgMessages sync.WaitGroup

	sessions map[string]*MockConsumerGroupSession
}

// NewMockConsumerGroup creates a new consumer group
func NewMockConsumerGroup(t *testing.T) *MockConsumerGroup {
	return &MockConsumerGroup{
		errs:     make(chan error, 1),
		sessions: make(map[string]*MockConsumerGroupSession),
		messages: make(map[int64]int64),
	}
}

// FailOnConsume marks the consumer to fail on consume
func (cg *MockConsumerGroup) FailOnConsume(err error) {
	cg.failOnConsume = err
}

func (cg *MockConsumerGroup) nextOffset() int64 {
	return atomic.AddInt64(&cg.offset, 1)
}

func (cg *MockConsumerGroup) topicKey(topics []string) string {
	return strings.Join(topics, ",")
}

func (cg *MockConsumerGroup) markMessage(topic string, partition int32, offset int64, metadata string) {
	cg.mMessages.Lock()
	defer cg.mMessages.Unlock()

	cnt := cg.messages[offset-1]

	if cnt == 0 {
		panic(fmt.Errorf("Cannot mark message with offset %d, it's not a valid offset or was already marked", offset))
	}

	cg.messages[offset] = cnt - 1

	cg.wgMessages.Done()
}

// Consume starts consuming from the consumergroup
func (cg *MockConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	if cg.failOnConsume != nil {
		return cg.failOnConsume
	}

	key := cg.topicKey(topics)
	for {
		cg.currentGeneration++
		session := newConsumerGroupSession(ctx, cg.currentGeneration, cg, topics)

		cg.sessions[key] = session

		err := handler.Setup(session)
		if err != nil {
			return fmt.Errorf("Error setting up: %v", err)
		}
		errg, _ := multierr.NewErrGroup(ctx)
		for _, topic := range topics {
			claim := session.createGroupClaim(topic, 0)
			errg.Go(func() error {
				<-ctx.Done()
				close(claim.msgs)
				return nil
			})
			errg.Go(func() error {
				err := handler.ConsumeClaim(session, claim)
				return err
			})
		}

		err = multierror.Append(
			// wait for runner errors and collect error
			errg.Wait(),
			// cleanup and collect errors
			handler.Cleanup(session),
		).ErrorOrNil()

		// remove current sessions
		delete(cg.sessions, key)

		if err != nil {
			return fmt.Errorf("Error running or cleaning: %w", err)
		}

		select {
		// if the session was terminated because of a cancelled context,
		// stop the loop
		case <-ctx.Done():
			return nil

			// otherwise just continue with the next generation
		default:
		}
	}
}

// SendError sends an error the consumergroup
func (cg *MockConsumerGroup) SendError(err error) {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	cg.errs <- err
}

// SendMessage sends a message to the consumergroup
// returns a channel that will be closed when the message has been committed
// by the group
func (cg *MockConsumerGroup) SendMessage(message *sarama.ConsumerMessage) <-chan struct{} {
	cg.mMessages.Lock()
	defer cg.mMessages.Unlock()

	message.Offset = cg.nextOffset()

	var messages int
	for _, session := range cg.sessions {
		session.SendMessage(message)
		messages++
	}

	cg.messages[message.Offset] += int64(messages)
	cg.wgMessages.Add(messages)

	done := make(chan struct{})
	go func() {
		defer close(done)
		cg.wgMessages.Wait()
	}()

	return done
}

// SendMessageWait sends a message to the consumergroup waiting for the message for being committed
func (cg *MockConsumerGroup) SendMessageWait(message *sarama.ConsumerMessage) {
	<-cg.SendMessage(message)
}

// Errors returns the errors channel
func (cg *MockConsumerGroup) Errors() <-chan error {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return cg.errs
}

// Close closes the consumergroup
func (cg *MockConsumerGroup) Close() error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	cg.messages = make(map[int64]int64)

	// close old errs chan and create new one
	close(cg.errs)
	return nil
}

func (cg *MockConsumerGroup) Pause(partitions map[string][]int32) {}

func (cg *MockConsumerGroup) Resume(partitions map[string][]int32) {}

func (cg *MockConsumerGroup) PauseAll() {}

func (cg *MockConsumerGroup) ResumeAll() {}
