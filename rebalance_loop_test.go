package goka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka/codec"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// stubConsumerGroup returns from Consume after session, without calling the
// handler, and records when each Consume call started and returned.
type stubConsumerGroup struct {
	session time.Duration
	errs    chan error
	// fail is what successive Consume calls return, at once and without a
	// session; once it is used up, Consume runs a session and returns nil.
	fail []error

	mu      sync.Mutex
	starts  []time.Time
	returns []time.Time
}

func (s *stubConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	s.mu.Lock()
	s.starts = append(s.starts, time.Now())
	if len(s.fail) > 0 {
		err := s.fail[0]
		s.fail = s.fail[1:]
		s.returns = append(s.returns, time.Now())
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	select {
	case <-time.After(s.session):
	case <-ctx.Done():
	}
	s.mu.Lock()
	s.returns = append(s.returns, time.Now())
	s.mu.Unlock()
	return nil
}
func (s *stubConsumerGroup) Errors() <-chan error      { return s.errs }
func (s *stubConsumerGroup) Close() error              { return nil }
func (s *stubConsumerGroup) Pause(map[string][]int32)  {}
func (s *stubConsumerGroup) Resume(map[string][]int32) {}
func (s *stubConsumerGroup) PauseAll()                 {}
func (s *stubConsumerGroup) ResumeAll()                {}

func (s *stubConsumerGroup) waitForStarts(t *testing.T, n int, within time.Duration) ([]time.Time, []time.Time) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		if len(s.starts) >= n {
			st, rt := append([]time.Time(nil), s.starts...), append([]time.Time(nil), s.returns...)
			s.mu.Unlock()
			return st, rt
		}
		s.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Consume was called fewer than %d times within %v", n, within)
	return nil, nil
}

func runWithStubGroup(t *testing.T, stub *stubConsumerGroup) (context.CancelFunc, *Processor) {
	cancel, proc, _ := runWithStubGroupErr(t, stub)
	return cancel, proc
}

// runWithStubGroupErr is runWithStubGroup, also returning what Run returns.
func runWithStubGroupErr(t *testing.T, stub *stubConsumerGroup) (context.CancelFunc, *Processor, <-chan error) {
	ctrl, bm := createMockBuilder(t)
	t.Cleanup(ctrl.Finish)
	bm.producer.EXPECT().Close().Return(nil).AnyTimes()
	bm.tmgr.EXPECT().Close().Return(nil).AnyTimes()
	bm.tmgr.EXPECT().Partitions(gomock.Any()).Return([]int32{0}, nil).AnyTimes()
	bm.tmgr.EXPECT().EnsureStreamExists(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	consBuilder, _ := createTestConsumerBuilder(t)
	groupBuilder := func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) { return stub, nil }

	graph := DefineGroup("test", Input("input", new(codec.Int64), func(ctx Context, msg interface{}) {}))
	proc, err := NewProcessor([]string{"localhost:9092"}, graph, bm.createProcessorOptions(consBuilder, groupBuilder)...)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- proc.Run(ctx) }()
	return cancel, proc, done
}

// A session that lasts — an ordinary rebalance — must rejoin at once rather
// than wait a fixed delay.
func TestRebalanceLoop_RejoinsAtOnceAfterALastingSession(t *testing.T) {
	stub := &stubConsumerGroup{session: rebalanceMinSession + 200*time.Millisecond, errs: make(chan error)}
	cancel, proc := runWithStubGroup(t, stub)
	defer func() { cancel(); <-proc.Done() }()

	starts, returns := stub.waitForStarts(t, 3, 6*time.Second)
	for i := 1; i < 3; i++ {
		gap := starts[i].Sub(returns[i-1])
		require.Lessf(t, gap, 200*time.Millisecond, "rejoin %d waited %v after a %v session", i, gap, stub.session)
	}
}

// A group that keeps returning immediately must back off, waiting longer
// each time, rather than spin.
func TestRebalanceLoop_BacksOffWhenSessionsEndAtOnce(t *testing.T) {
	stub := &stubConsumerGroup{session: 0, errs: make(chan error)}
	cancel, proc := runWithStubGroup(t, stub)
	defer func() { cancel(); <-proc.Done() }()

	starts, returns := stub.waitForStarts(t, 4, 6*time.Second)
	gaps := make([]time.Duration, 3)
	for i := 1; i < 4; i++ {
		gaps[i-1] = starts[i].Sub(returns[i-1])
	}
	require.Less(t, gaps[0], 200*time.Millisecond, "the first quick return rejoins at once: %v", gaps)
	require.GreaterOrEqual(t, gaps[1], rebalanceBackoffStep-50*time.Millisecond, "the second waits a step: %v", gaps)
	require.GreaterOrEqual(t, gaps[2], 2*rebalanceBackoffStep-50*time.Millisecond, "the third waits two: %v", gaps)
}

// A coordinator that is still loading the group's offsets, as one is just
// after its broker restarts, is waited out: the processor backs off and
// rejoins, longer each time, and runs once the coordinator answers.
func TestRebalanceLoop_RejoinsAfterACoordinatorThatIsNotReady(t *testing.T) {
	stub := &stubConsumerGroup{session: time.Hour, errs: make(chan error), fail: []error{
		sarama.ErrOffsetsLoadInProgress,
		fmt.Errorf("joining: %w", sarama.ErrOffsetsLoadInProgress),
		sarama.ErrNotCoordinatorForConsumer,
		sarama.ErrConsumerCoordinatorNotAvailable,
		sarama.ErrRebalanceInProgress,
	}}
	cancel, _, done := runWithStubGroupErr(t, stub)

	starts, returns := stub.waitForStarts(t, 6, 15*time.Second)
	for i := 1; i < 6; i++ {
		gap := starts[i].Sub(returns[i-1])
		want := time.Duration(i-1) * rebalanceBackoffStep
		require.GreaterOrEqualf(t, gap, want-50*time.Millisecond, "rejoin %d waited %v, want at least %v", i, gap, want)
	}
	select {
	case err := <-done:
		t.Fatalf("the processor stopped on a retriable error: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the processor did not stop once canceled")
	}
}

// Any other error from Consume still stops the processor, with the error.
func TestRebalanceLoop_StopsOnAnErrorThatIsNotRetriable(t *testing.T) {
	stub := &stubConsumerGroup{session: time.Hour, errs: make(chan error), fail: []error{sarama.ErrGroupAuthorizationFailed}}
	cancel, _, done := runWithStubGroupErr(t, stub)
	defer cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, sarama.ErrGroupAuthorizationFailed)
		require.ErrorContains(t, err, "error consuming from group consumer")
	case <-time.After(5 * time.Second):
		t.Fatal("the processor kept running after a non-retriable error")
	}
	_, _ = stub.waitForStarts(t, 1, time.Second)
	stub.mu.Lock()
	defer stub.mu.Unlock()
	require.Len(t, stub.starts, 1, "a non-retriable error was retried")
}

// A processor canceled while it waits to rejoin stops at once.
func TestRebalanceLoop_StopsWhenCanceledWhileWaitingToRejoin(t *testing.T) {
	stub := &stubConsumerGroup{session: time.Hour, errs: make(chan error), fail: []error{
		sarama.ErrOffsetsLoadInProgress, sarama.ErrOffsetsLoadInProgress, sarama.ErrOffsetsLoadInProgress,
		sarama.ErrOffsetsLoadInProgress, sarama.ErrOffsetsLoadInProgress, sarama.ErrOffsetsLoadInProgress,
	}}
	cancel, _, done := runWithStubGroupErr(t, stub)
	_, _ = stub.waitForStarts(t, 3, 5*time.Second)
	canceled := time.Now()
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
		require.Less(t, time.Since(canceled), 2*rebalanceBackoffStep, "the processor finished its wait before stopping")
	case <-time.After(5 * time.Second):
		t.Fatal("the processor did not stop once canceled")
	}
}
