package mock

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/multierr"
)

type ConsumerGroupSession struct {
	ctx    context.Context
	topics []string
	claims map[string]*ConsumerGroupClaim
}

type ConsumerGroupClaim struct {
	topic     string
	partition int32
	msgs      chan *sarama.ConsumerMessage
}

func NewConsumerGroupClaim(topic string, partition int32) *ConsumerGroupClaim {
	return &ConsumerGroupClaim{
		topic:     topic,
		partition: partition,
		msgs:      make(chan *sarama.ConsumerMessage),
	}
}

func (cgc *ConsumerGroupClaim) Topic() string {
	return cgc.topic
}
func (cgc *ConsumerGroupClaim) Partition() int32 {
	return cgc.partition
}
func (cgc *ConsumerGroupClaim) InitialOffset() int64 {
	return 0
}
func (cgc *ConsumerGroupClaim) HighWaterMarkOffset() int64 {
	return 0
}
func (cgc *ConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return cgc.msgs
}

func NewConsumerGroupSession(ctx context.Context, topics []string) *ConsumerGroupSession {
	return &ConsumerGroupSession{
		ctx:    ctx,
		topics: topics,
		claims: make(map[string]*ConsumerGroupClaim),
	}
}

func (cgs *ConsumerGroupSession) Claims() map[string][]int32 {
	claims := make(map[string][]int32)
	for _, topic := range cgs.topics {
		claims[topic] = []int32{0}
	}
	return claims
}

func (cgs *ConsumerGroupSession) CreateGroupClaim(topic string, partition int32) *ConsumerGroupClaim {
	cgs.claims[topic] = NewConsumerGroupClaim(topic, 0)

	return cgs.claims[topic]
}

func (cgs *ConsumerGroupSession) SendMessage(msg *sarama.ConsumerMessage) {
	for topic, claim := range cgs.claims {
		if topic == msg.Topic {
			claim.msgs <- msg
		}
	}
}

func (cgs *ConsumerGroupSession) MemberID() string {
	return "test"
}
func (cgs *ConsumerGroupSession) GenerationID() int32 {
	return 0
}
func (cgs *ConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}
func (cgs *ConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}
func (cgs *ConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
}

func (cgs *ConsumerGroupSession) Context() context.Context {
	return cgs.ctx
}

type ConsumerGroup struct {
	errs chan error

	sessions map[string]*ConsumerGroupSession
}

func NewConsumerGroup(t *testing.T) *ConsumerGroup {
	return &ConsumerGroup{
		errs:     make(chan error, 1),
		sessions: make(map[string]*ConsumerGroupSession),
	}
}

func (cg *ConsumerGroup) topicKey(topics []string) string {
	return strings.Join(topics, ",")
}

func (cg *ConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	session := NewConsumerGroupSession(ctx, topics)
	key := cg.topicKey(topics)
	cg.sessions[key] = session

	err := handler.Setup(session)
	if err != nil {
		return fmt.Errorf("Error setting up: %v", err)
	}
	errg, _ := multierr.NewErrGroup(ctx)
	for _, topic := range topics {
		claim := session.CreateGroupClaim(topic, 0)
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

	// return collected errors
	return errs.NilOrError()
}

func (cg *ConsumerGroup) SendError(err error) {
	cg.errs <- err
}

func (cg *ConsumerGroup) SendMessage(message *sarama.ConsumerMessage) {
	for _, session := range cg.sessions {
		session.SendMessage(message)
	}
}

func (cg *ConsumerGroup) Errors() <-chan error {
	return cg.errs
}
func (cg *ConsumerGroup) Close() error {
	return nil
}
