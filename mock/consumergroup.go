package mock

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/multierr"
)

// ConsumerGroupSession mocks the consumer group session used for testing
type ConsumerGroupSession struct {
	ctx        context.Context
	generation int32
	topics     []string
	claims     map[string]*ConsumerGroupClaim

	consumerGroup *ConsumerGroup
}

// ConsumerGroupClaim mocks the claim...
type ConsumerGroupClaim struct {
	topic     string
	partition int32
	msgs      chan *sarama.ConsumerMessage
}

// NewConsumerGroupClaim creates a new mock
func NewConsumerGroupClaim(topic string, partition int32) *ConsumerGroupClaim {
	return &ConsumerGroupClaim{
		topic:     topic,
		partition: partition,
		msgs:      make(chan *sarama.ConsumerMessage),
	}
}

// Topic returns the current topic of the claim
func (cgc *ConsumerGroupClaim) Topic() string {
	return cgc.topic
}

// Partition returns the partition
func (cgc *ConsumerGroupClaim) Partition() int32 {
	return cgc.partition
}

// InitialOffset returns the initial offset
func (cgc *ConsumerGroupClaim) InitialOffset() int64 {
	return 0
}

// HighWaterMarkOffset returns the hwm offset
func (cgc *ConsumerGroupClaim) HighWaterMarkOffset() int64 {
	return 0
}

// Messages returns the message channel that must be
func (cgc *ConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return cgc.msgs
}

func newConsumerGroupSession(ctx context.Context, generation int32, cg *ConsumerGroup, topics []string) *ConsumerGroupSession {
	return &ConsumerGroupSession{
		ctx:           ctx,
		generation:    generation,
		consumerGroup: cg,
		topics:        topics,
		claims:        make(map[string]*ConsumerGroupClaim),
	}
}

// Claims returns the number of partitions assigned in the group session for each topic
func (cgs *ConsumerGroupSession) Claims() map[string][]int32 {
	claims := make(map[string][]int32)
	for _, topic := range cgs.topics {
		claims[topic] = []int32{0}
	}
	return claims
}

func (cgs *ConsumerGroupSession) createGroupClaim(topic string, partition int32) *ConsumerGroupClaim {
	cgs.claims[topic] = NewConsumerGroupClaim(topic, 0)

	return cgs.claims[topic]
}

// SendMessage sends a message to the consumer
func (cgs *ConsumerGroupSession) SendMessage(msg *sarama.ConsumerMessage) {

	for topic, claim := range cgs.claims {
		if topic == msg.Topic {
			claim.msgs <- msg
		}
	}
}

// MemberID returns the member ID
// TOOD: clarify what that actually means and whether we need to mock taht somehow
func (cgs *ConsumerGroupSession) MemberID() string {
	panic("MemberID not provided by mock")
}

// GenerationID returns the generation ID of the group consumer
func (cgs *ConsumerGroupSession) GenerationID() int32 {
	return cgs.generation
}

// MarkOffset marks the passed offset consumed in topic/partition
func (cgs *ConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}

// ResetOffset resets the offset to be consumed from
func (cgs *ConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	panic("reset offset is not implemented by the mock")
}

// MarkMessage marks the passed message as consumed
func (cgs *ConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	cgs.consumerGroup.markMessage(msg)
}

// Context returns the consumer group's context
func (cgs *ConsumerGroupSession) Context() context.Context {
	return cgs.ctx
}

// ConsumerGroup mocks the consumergroup
type ConsumerGroup struct {
	errs chan error

	// use the same offset counter for all topics
	offset            int64
	currentGeneration int32

	// messages we sent to the consumergroup and need to wait for
	mMessages  sync.Mutex
	messages   map[int64]int64
	wgMessages sync.WaitGroup

	sessions map[string]*ConsumerGroupSession
}

// NewConsumerGroup creates a new consumer group
func NewConsumerGroup(t *testing.T) *ConsumerGroup {
	return &ConsumerGroup{
		errs:     make(chan error, 1),
		sessions: make(map[string]*ConsumerGroupSession),
		messages: make(map[int64]int64),
	}
}

func (cg *ConsumerGroup) nextOffset() int64 {
	return atomic.AddInt64(&cg.offset, 1)
}

func (cg *ConsumerGroup) topicKey(topics []string) string {
	return strings.Join(topics, ",")
}

func (cg *ConsumerGroup) markMessage(msg *sarama.ConsumerMessage) {
	cg.mMessages.Lock()
	defer cg.mMessages.Unlock()

	cnt := cg.messages[msg.Offset]

	if cnt == 0 {
		panic(fmt.Errorf("Cannot mark message with offest %d, it's not a valid offset or was already marked", msg.Offset))
	}

	cg.messages[msg.Offset] = cnt - 1

	cg.wgMessages.Done()
}

// Consume starts consuming from the consumergroup
func (cg *ConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {

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

		errs := new(multierr.Errors)

		// wait for runner errors and collect error
		errs.Collect(errg.Wait().NilOrError())

		// cleanup and collect errors
		errs.Collect(handler.Cleanup(session))

		// remove current sessions
		delete(cg.sessions, key)

		err = errs.NilOrError()
		if err != nil {
			return fmt.Errorf("Error running or cleaning: %v", err)
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
func (cg *ConsumerGroup) SendError(err error) {
	cg.errs <- err
}

// SendMessage sends a message to the consumergroup
// returns a channel that will be closed when the message has been committed
// by the group
func (cg *ConsumerGroup) SendMessage(message *sarama.ConsumerMessage) <-chan struct{} {
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
func (cg *ConsumerGroup) SendMessageWait(message *sarama.ConsumerMessage) {
	<-cg.SendMessage(message)
}

// Errors returns the errors channel
func (cg *ConsumerGroup) Errors() <-chan error {
	return cg.errs
}

// Close closes the consumergroup
func (cg *ConsumerGroup) Close() error {
	return nil
}
