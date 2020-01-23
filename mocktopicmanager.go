package goka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// Topic holds all infos about a topic
type MockTopic struct {
	Topic             string
	NumPartitions     int
	ReplicationFactor int
	Config            map[string]string
	Table             bool // false -> stream, true -> table
	OldestOffset      int64
	Hwm               int64
}

// MockTopicManager mimicks the behavior of the real topic manager
type MockTopicManager struct {
	DefaultNumPartitions     int
	DefaultReplicationFactor int
	Topics                   map[string]*MockTopic
}

// NewMockTopicManagerMock creates a new topic manager mock
func NewMockTopicManager(defaultNumPartitions int, defaultReplFactor int) *MockTopicManager {
	return &MockTopicManager{
		DefaultNumPartitions:     defaultNumPartitions,
		DefaultReplicationFactor: defaultReplFactor,
		Topics:                   make(map[string]*MockTopic),
	}
}

// EnsureTableExists ensures a table exists
func (tm *MockTopicManager) EnsureTableExists(topic string, npar int) error {
	tm.Topics[topic] = &MockTopic{
		Topic:             topic,
		NumPartitions:     npar,
		ReplicationFactor: tm.DefaultReplicationFactor,
		Table:             true,
	}
	return nil
}

// EnsureStreamExists ensures a stream exists
func (tm *MockTopicManager) EnsureStreamExists(topic string, npar int) error {
	tm.Topics[topic] = &MockTopic{
		Topic:             topic,
		NumPartitions:     npar,
		ReplicationFactor: tm.DefaultReplicationFactor,
	}
	return nil
}

// EnsureMockTopicExists ensures a topic exists
func (tm *MockTopicManager) EnsureTopicExists(topic string, npar, rfactor int, config map[string]string) error {
	tm.Topics[topic] = &MockTopic{
		Topic:             topic,
		NumPartitions:     npar,
		ReplicationFactor: rfactor,
		Config:            config,
	}
	return nil
}

// Partitions returns all partitions for a topic
func (tm *MockTopicManager) Partitions(topic string) ([]int32, error) {
	numParts := tm.DefaultNumPartitions
	if t, exists := tm.Topics[topic]; exists {
		numParts = t.NumPartitions
	}

	var parts []int32
	for i := 0; i < numParts; i++ {
		parts = append(parts, int32(i))
	}
	return parts, nil
}

func (tm *MockTopicManager) SetOffset(topicName string, oldest, hwm int64) {
	topic, ok := tm.Topics[topicName]
	if !ok {
		topic = &MockTopic{
			Topic:             topicName,
			NumPartitions:     tm.DefaultNumPartitions,
			ReplicationFactor: tm.DefaultReplicationFactor,
			Config:            map[string]string{},
		}
		tm.Topics[topicName] = topic
	}
	topic.OldestOffset = oldest
	topic.Hwm = hwm
}

// GetOffset returns the offset closest to the passed time (or exactly time, if the offsets are empty)
func (tm *MockTopicManager) GetOffset(topicName string, partitionID int32, time int64) (int64, error) {
	topic, ok := tm.Topics[topicName]
	if !ok {
		return time, nil
	}

	if time == sarama.OffsetOldest {
		return topic.OldestOffset, nil
	}
	if time == sarama.OffsetNewest {
		return topic.Hwm, nil
	}

	return 0, fmt.Errorf("only oldest and newest are supported in the mock")
}

// Close has no action on the mock
func (tm *MockTopicManager) Close() error {
	return nil
}
