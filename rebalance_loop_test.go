package goka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/storage"
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

// A client that lost its broker, as one does while the broker restarts,
// is waited out like a coordinator that is not ready: a read that timed
// out in the join, and a partition table that could not ask a broker
// for its offsets while the processor set up its partitions.
func TestRebalanceLoop_RejoinsAfterABrokerConnectionIsLost(t *testing.T) {
	timedOut := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	stub := &stubConsumerGroup{session: time.Hour, errs: make(chan error), fail: []error{
		timedOut,
		setupFailure(sarama.ErrNotConnected),
		sarama.ErrOutOfBrokers,
	}}
	cancel, _, done := runWithStubGroupErr(t, stub)
	_, _ = stub.waitForStarts(t, 4, 10*time.Second)
	select {
	case err := <-done:
		t.Fatalf("the processor stopped on a lost broker: %v", err)
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

// setupFailure is err as Consume returns it from Setup when a partition
// table cannot find its offsets: wrapped as Processor.Setup,
// PartitionProcessor.Start and PartitionTable.findOffsetToLoad wrap it.
func setupFailure(err error) error {
	table := fmt.Errorf("Error getting oldest offset for topic/partition table/3: %w", err)
	start := fmt.Errorf("Setup failed. Cannot start processor for partition 3: %w", multierror.Append(nil, table))
	return multierror.Append(nil, newErrSetup(3, start))
}

// Which errors from Consume rejoin and which stop the processor.
func TestIsRetriableConsumeErr(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"loading offsets", sarama.ErrOffsetsLoadInProgress, true},
		{"coordinator unavailable", sarama.ErrConsumerCoordinatorNotAvailable, true},
		{"not coordinator", sarama.ErrNotCoordinatorForConsumer, true},
		{"rebalance in progress", sarama.ErrRebalanceInProgress, true},
		{"leader not available", sarama.ErrLeaderNotAvailable, true},
		{"not leader", sarama.ErrNotLeaderForPartition, true},
		{"request timed out", sarama.ErrRequestTimedOut, true},
		{"broker not available", sarama.ErrBrokerNotAvailable, true},
		{"network exception", sarama.ErrNetworkException, true},
		{"not connected", sarama.ErrNotConnected, true},
		{"out of brokers", sarama.ErrOutOfBrokers, true},
		{"connection closed", fmt.Errorf("reading: %w", io.EOF), true},
		{"connection cut short", io.ErrUnexpectedEOF, true},
		{"connection refused", &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}, true},
		{"connection reset", &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}, true},
		{"broken pipe", &net.OpError{Op: "write", Net: "tcp", Err: os.NewSyscallError("write", syscall.EPIPE)}, true},
		{"read timed out", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, true},
		{"table setup without a broker", setupFailure(sarama.ErrNotConnected), true},
		{"table setup that cannot recover", setupFailure(errors.New("the table might be gone")), false},
		{"group not authorized", sarama.ErrGroupAuthorizationFailed, false},
		{"topic not authorized", setupFailure(sarama.ErrTopicAuthorizationFailed), false},
		{"closed client", sarama.ErrClosedClient, false},
		{"TLS alert", &net.OpError{Op: "remote error", Err: errors.New("tls: bad certificate")}, false},
		{"unknown host", &net.DNSError{Err: "no such host", Name: "kafka-9"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isRetriableConsumeErr(tc.err), "%v", tc.err)
		})
	}
}

// A partition processor whose table cannot ask a broker for its offsets
// fails to start with an error that still names the lost broker, so the
// processor's Setup error rejoins rather than stops it.
func TestPartitionProcessorStart_KeepsALostBrokerInItsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	bm := newBuilderMock(ctrl)
	graph := DefineGroup("lost-broker",
		Input("input", new(codec.String), func(Context, any) {}),
		Persist(new(codec.String)),
	)
	opts := new(poptions)
	require.NoError(t, opts.applyOptions(graph, WithStorageBuilder(storage.MemoryBuilder())))
	bm.tmgr.EXPECT().GetOffset(graph.GroupTable().Topic(), int32(3), sarama.OffsetOldest).Return(int64(0), sarama.ErrNotConnected)

	pp := newPartitionProcessor(3, graph, func(*message, string) {}, defaultLogger, opts, runModeActive,
		nil, nil, nil, bm.tmgr, NewSimpleBackoff(defaultBackoffStep, defaultBackoffMax), time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := pp.Start(ctx, ctx)
	require.ErrorIs(t, err, sarama.ErrNotConnected)
	require.True(t, isRetriableConsumeErr(newErrSetup(3, err)), "%v", err)
}
