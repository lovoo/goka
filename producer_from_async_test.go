package goka

import (
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

type mockAsyncProducer struct {
	input     chan *sarama.ProducerMessage
	successes chan *sarama.ProducerMessage
	errors    chan *sarama.ProducerError
	closeOnce sync.Once
}

func newMockAsyncProducer() *mockAsyncProducer {
	return &mockAsyncProducer{
		input:     make(chan *sarama.ProducerMessage, 16),
		successes: make(chan *sarama.ProducerMessage, 16),
		errors:    make(chan *sarama.ProducerError, 16),
	}
}

func (m *mockAsyncProducer) AsyncClose() {
	m.closeOnce.Do(func() {
		close(m.input)
	})
}

func (m *mockAsyncProducer) Close() error {
	m.AsyncClose()
	return nil
}

func (m *mockAsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return m.input
}

func (m *mockAsyncProducer) Successes() <-chan *sarama.ProducerMessage {
	return m.successes
}

func (m *mockAsyncProducer) Errors() <-chan *sarama.ProducerError {
	return m.errors
}

func (m *mockAsyncProducer) IsTransactional() bool                   { return false }
func (m *mockAsyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag { return 0 }
func (m *mockAsyncProducer) BeginTxn() error                         { return nil }
func (m *mockAsyncProducer) CommitTxn() error                        { return nil }
func (m *mockAsyncProducer) AbortTxn() error                         { return nil }
func (m *mockAsyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}
func (m *mockAsyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

func TestNewProducerFromAsyncProducer_ResolvesPromise(t *testing.T) {
	ap := newMockAsyncProducer()
	p := NewProducerFromAsyncProducer(ap)

	done := make(chan error, 1)
	go func() {
		msg := <-ap.input
		ap.successes <- msg
	}()

	promise := p.Emit("topic", "key", []byte("value"))
	promise.Then(func(err error) {
		done <- err
	})

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("promise was not resolved")
	}

	close(ap.successes)
	close(ap.errors)
	require.NoError(t, p.Close())
}

func TestNewProducerFromAsyncProducer_UsesWrappedProducer(t *testing.T) {
	ap := newMockAsyncProducer()
	cfg := DefaultConfig()
	var wrapped bool
	wrap := AsyncProducerWrapper(func(p sarama.AsyncProducer, c *sarama.Config) sarama.AsyncProducer {
		wrapped = true
		require.Equal(t, cfg, c)
		return p
	})

	prod := NewProducerFromAsyncProducer(wrap(ap, cfg))
	require.True(t, wrapped)
	require.NotNil(t, prod)

	close(ap.successes)
	close(ap.errors)
	require.NoError(t, prod.Close())
}

func TestProducerBuilderWithAsyncProducerWrapper_NilWrap(t *testing.T) {
	cfg := DefaultConfig()
	builder := ProducerBuilderWithAsyncProducerWrapper(cfg, nil)
	require.NotNil(t, builder)
}
