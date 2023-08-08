package tester

import (
	"fmt"

	"github.com/IBM/sarama"
)

// MockTopicManager mimicks the behavior of the real topic manager
type MockTopicManager struct {
	tt *Tester
}

// NewMockTopicManager creates a new topic manager mock
func NewMockTopicManager(tt *Tester, defaultNumPartitions int, defaultReplFactor int) *MockTopicManager {
	return &MockTopicManager{
		tt: tt,
	}
}

// EnsureTableExists ensures a table exists
func (tm *MockTopicManager) EnsureTableExists(topic string, npar int) error {
	if npar != 1 {
		return fmt.Errorf("Mock only supports 1 partition")
	}
	tm.tt.getOrCreateQueue(topic)
	return nil
}

// EnsureStreamExists ensures a stream exists
func (tm *MockTopicManager) EnsureStreamExists(topic string, npar int) error {
	tm.tt.getOrCreateQueue(topic)
	return nil
}

// EnsureTopicExists ensures a topic exists
func (tm *MockTopicManager) EnsureTopicExists(topic string, npar, rfactor int, config map[string]string) error {
	tm.tt.getOrCreateQueue(topic)
	return nil
}

// Partitions returns all partitions for a topic
func (tm *MockTopicManager) Partitions(topic string) ([]int32, error) {
	return []int32{0}, nil
}

// GetOffset returns the offset closest to the passed time (or exactly time, if the offsets are empty)
func (tm *MockTopicManager) GetOffset(topicName string, partitionID int32, time int64) (int64, error) {
	topic := tm.tt.getOrCreateQueue(topicName)

	switch time {
	case sarama.OffsetOldest:
		return 0, nil
	case sarama.OffsetNewest:
		return topic.Hwm(), nil
	default:
		// always return from oldest, because the tester does not store
		// timestamps
		return 0, nil
	}
}

// Close has no action on the mock
func (tm *MockTopicManager) Close() error {
	return nil
}
