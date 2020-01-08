package mock

// Topic holds all infos about a topic
type Topic struct {
	Topic             string
	NumPartitions     int
	ReplicationFactor int
	Config            map[string]string
	Table             bool // false -> stream, true -> table
}

// TopicManager mimicks the behavior of the real topic manager
type TopicManager struct {
	DefaultNumPartitions     int
	DefaultReplicationFactor int
	Topics                   map[string]*Topic
}

// NewTopicManagerMock creates a new topic manager mock
func NewTopicManager(defaultNumPartitions int, defaultReplFactor int) *TopicManager {
	return &TopicManager{
		DefaultNumPartitions:     defaultNumPartitions,
		DefaultReplicationFactor: defaultReplFactor,
		Topics:                   make(map[string]*Topic),
	}
}

// EnsureTableExists ensures a table exists
func (tm *TopicManager) EnsureTableExists(topic string, npar int) error {
	tm.Topics[topic] = &Topic{
		Topic:             topic,
		NumPartitions:     npar,
		ReplicationFactor: tm.DefaultReplicationFactor,
		Table:             true,
	}
	return nil
}

// EnsureStreamExists ensures a stream exists
func (tm *TopicManager) EnsureStreamExists(topic string, npar int) error {
	tm.Topics[topic] = &Topic{
		Topic:             topic,
		NumPartitions:     npar,
		ReplicationFactor: tm.DefaultReplicationFactor,
	}
	return nil
}

// EnsureTopicExists ensures a topic exists
func (tm *TopicManager) EnsureTopicExists(topic string, npar, rfactor int, config map[string]string) error {
	tm.Topics[topic] = &Topic{
		Topic:             topic,
		NumPartitions:     npar,
		ReplicationFactor: rfactor,
		Config:            config,
	}
	return nil
}

// Partitions returns all partitions for a topic
func (tm *TopicManager) Partitions(topic string) ([]int32, error) {
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

// Close has no action on the mock
func (tm *TopicManager) Close() error {
	return nil
}
