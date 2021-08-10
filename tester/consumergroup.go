package tester

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/multierr"
)

// consumerGroup mocks the consumergroup
type consumerGroup struct {
	errs chan error

	// use the same offset counter for all topics
	offset            int64
	currentGeneration int32

	state *goka.Signal

	currentSession *cgSession

	mu sync.RWMutex

	tt *Tester
}

const (
	cgStateStopped goka.State = iota
	cgStateRebalancing
	cgStateSetup
	cgStateConsuming
	cgStateCleaning
)

func newConsumerGroup(t T, tt *Tester) *consumerGroup {
	return &consumerGroup{
		errs:  make(chan error, 1),
		state: goka.NewSignal(cgStateStopped, cgStateRebalancing, cgStateSetup, cgStateConsuming, cgStateCleaning).SetState(cgStateStopped),
		tt:    tt,
	}
}

func (cg *consumerGroup) catchupAndWait() int {
	if cg.currentSession == nil {
		panic("There is currently no session. Cannot catchup, but we shouldn't be at this point")
	}
	return cg.currentSession.catchupAndWait()
}

// Consume starts consuming from the consumergroup
func (cg *consumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	if !cg.state.IsState(cgStateStopped) {
		return fmt.Errorf("Tried to double-consume this consumer-group, which is not supported by the mock")
	}
	logger.Printf("consuming consumergroup with topics %v", topics)
	defer cg.state.SetState(cgStateStopped)
	if len(topics) == 0 {
		return fmt.Errorf("no topics specified")
	}

	for {
		cg.state.SetState(cgStateRebalancing)
		cg.currentGeneration++
		session := newCgSession(ctx, cg.currentGeneration, cg, topics)

		cg.currentSession = session

		cg.state.SetState(cgStateSetup)
		err := handler.Setup(session)
		if err != nil {
			return fmt.Errorf("Error setting up: %v", err)
		}
		errg, innerCtx := multierr.NewErrGroup(ctx)
		for _, claim := range session.claims {
			claim := claim
			errg.Go(func() error {
				<-innerCtx.Done()
				claim.close()
				return nil
			})
			errg.Go(func() error {
				err := handler.ConsumeClaim(session, claim)
				if err != nil {
					cg.errs <- err
				}
				return nil
			})
		}
		cg.state.SetState(cgStateConsuming)

		errs := new(multierr.Errors)

		// wait for runner errors and collect error
		errs.Collect(errg.Wait().NilOrError())
		cg.state.SetState(cgStateCleaning)

		// cleanup and collect errors
		errs.Collect(handler.Cleanup(session))

		// remove current sessions
		cg.currentSession = nil

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
func (cg *consumerGroup) SendError(err error) {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	cg.errs <- err
}

// Errors returns the errors channel
func (cg *consumerGroup) Errors() <-chan error {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return cg.errs
}

func (cg *consumerGroup) waitRunning() {
	<-cg.state.WaitForState(cgStateConsuming)
}

func (cg *consumerGroup) nextOffset() int64 {
	return atomic.AddInt64(&cg.offset, 1)
}

// Close closes the consumergroup
func (cg *consumerGroup) Close() error {
	// close old errs chan and create new one
	cg.mu.Lock()
	defer cg.mu.Unlock()
	close(cg.errs)
	cg.errs = make(chan error)

	cg.offset = 0
	cg.currentGeneration = 0
	return nil
}

type cgSession struct {
	ctx        context.Context
	generation int32
	claims     map[string]*cgClaim
	queues     map[string]*queueSession

	mCatchup sync.Mutex

	waitingMessages map[string]bool
	mMessages       sync.Mutex
	wgMessages      sync.WaitGroup
	consumerGroup   *consumerGroup
}

func newCgSession(ctx context.Context, generation int32, cg *consumerGroup, topics []string) *cgSession {

	cgs := &cgSession{
		ctx:             ctx,
		generation:      generation,
		consumerGroup:   cg,
		waitingMessages: make(map[string]bool),
		queues:          make(map[string]*queueSession),
		claims:          make(map[string]*cgClaim),
	}

	for _, topic := range topics {
		cgs.queues[topic] = &queueSession{
			queue: cg.tt.getOrCreateQueue(topic),
		}
		cgs.claims[topic] = newCgClaim(topic, 0)
	}

	return cgs
}

// queue session pairs a queue and an offset for a consumerGroupClaim.
// This allows to consume messages continuing from the last marked offset.
type queueSession struct {
	sync.Mutex
	queue *queue
	hwm   int64
}

func (qs *queueSession) setHwmIfNewer(hwm int64) {
	qs.Lock()
	defer qs.Unlock()
	if qs.hwm < hwm {
		qs.hwm = hwm
	}
}

func (qs *queueSession) getHwm() int64 {
	qs.Lock()
	defer qs.Unlock()
	return qs.hwm
}

// Claims returns the number of partitions assigned in the group session for each topic
func (cgs *cgSession) Claims() map[string][]int32 {
	claims := make(map[string][]int32)
	for topic := range cgs.claims {
		claims[topic] = []int32{0}
	}
	return claims
}

// MemberID returns the member ID
// TOOD: clarify what that actually means and whether we need to mock taht somehow
func (cgs *cgSession) MemberID() string {
	panic("MemberID not provided by mock")
}

// GenerationID returns the generation ID of the group consumer
func (cgs *cgSession) GenerationID() int32 {
	return cgs.generation
}

// MarkOffset marks the passed offset consumed in topic/partition
func (cgs *cgSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	cgs.mMessages.Lock()
	defer cgs.mMessages.Unlock()

	msgKey := cgs.makeMsgKey(topic, offset-1)
	if !cgs.waitingMessages[msgKey] {
		logger.Printf("Message topic/partition/offset %s/%d/%d was already marked as consumed. We should only mark the message once", topic, partition, offset-1)
	} else {
		cgs.wgMessages.Done()
		delete(cgs.waitingMessages, msgKey)
	}

	cgs.queues[topic].setHwmIfNewer(offset)
}

func (cgs *cgSession) Commit() {
	panic("commit offset is not implemented by the mock")
}

// ResetOffset resets the offset to be consumed from
func (cgs *cgSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	panic("reset offset is not implemented by the mock")
}

// MarkMessage marks the passed message as consumed
func (cgs *cgSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	cgs.MarkOffset(msg.Topic, msg.Partition, msg.Offset+1, metadata)
}

// Context returns the consumer group's context
func (cgs *cgSession) Context() context.Context {
	return cgs.ctx
}

func (cgs *cgSession) catchupAndWait() int {
	cgs.mCatchup.Lock()
	defer cgs.mCatchup.Unlock()

	var msgPushed int
	for _, queue := range cgs.queues {

		cgs.mMessages.Lock()
		queueHwm := queue.getHwm()
		cgs.mMessages.Unlock()

		for _, msg := range queue.queue.messagesFromOffset(queueHwm) {
			cgs.pushMessageToClaim(cgs.claims[queue.queue.topic], msg)
			msgPushed++
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		cgs.wgMessages.Wait()
	}()

	select {
	case <-cgs.ctx.Done():
		return 0
	case <-done:
		return msgPushed
	}
}

func (cgs *cgSession) makeMsgKey(topic string, offset int64) string {
	return fmt.Sprintf("%s-%d", topic, offset)
}

func (cgs *cgSession) pushMessageToClaim(claim *cgClaim, msg *message) {
	cgs.mMessages.Lock()

	msgKey := cgs.makeMsgKey(claim.Topic(), msg.offset)

	if cgs.waitingMessages[msgKey] {
		cgs.mMessages.Unlock()
		panic(fmt.Sprintf("There's a duplicate message offset in the same topic/partition %s/%d: %d. The tester has a bug!", claim.Topic(), 0, msg.offset))
	}

	cgs.waitingMessages[msgKey] = true
	cgs.mMessages.Unlock()

	cgs.wgMessages.Add(1)

	select {
	case claim.msgs <- &sarama.ConsumerMessage{
		Headers: msg.headers.ToSaramaPtr(),
		Key:     []byte(msg.key),
		Value:   msg.value,
		Topic:   claim.Topic(),
		Offset:  msg.offset,
	}:
	// context closed already, so don't push as no consumer will be listening
	case <-cgs.ctx.Done():
		// decrement wg count as we couldn'T push the message
		cgs.wgMessages.Done()
		return
	}
}

type cgClaim struct {
	topic     string
	partition int32
	msgs      chan *sarama.ConsumerMessage
}

func newCgClaim(topic string, partition int32) *cgClaim {
	return &cgClaim{
		topic:     topic,
		partition: partition,
		msgs:      make(chan *sarama.ConsumerMessage),
	}
}

// Topic returns the current topic of the claim
func (cgc *cgClaim) Topic() string {
	return cgc.topic
}

// Partition returns the partition
func (cgc *cgClaim) Partition() int32 {
	return cgc.partition
}

// InitialOffset returns the initial offset
func (cgc *cgClaim) InitialOffset() int64 {
	return 0
}

// HighWaterMarkOffset returns the hwm offset
func (cgc *cgClaim) HighWaterMarkOffset() int64 {
	return 0
}

// Messages returns the message channel that must be
func (cgc *cgClaim) Messages() <-chan *sarama.ConsumerMessage {
	return cgc.msgs
}

func (cgc *cgClaim) close() {
	close(cgc.msgs)
}
