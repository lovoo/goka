package goka

import (
	"context"
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

	mu      sync.Mutex
	starts  []time.Time
	returns []time.Time
}

func (s *stubConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	s.mu.Lock()
	s.starts = append(s.starts, time.Now())
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
	go func() { _ = proc.Run(ctx) }()
	return cancel, proc
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
