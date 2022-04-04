package goka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

// TopicManager provides an interface to create/check topics and their partitions
type TopicManager interface {
	// EnsureTableExists checks that a table (log-compacted topic) exists, or create one if possible
	EnsureTableExists(topic string, npar int) error
	// EnsureStreamExists checks that a stream topic exists, or create one if possible
	EnsureStreamExists(topic string, npar int) error
	// EnsureTopicExists checks that a topic exists, or create one if possible,
	// enforcing the given configuration
	EnsureTopicExists(topic string, npar, rfactor int, config map[string]string) error

	// Partitions returns the number of partitions of a topic, that are assigned to the running
	// instance, i.e. it doesn't represent all partitions of a topic.
	Partitions(topic string) ([]int32, error)

	GetOffset(topic string, partitionID int32, time int64) (int64, error)

	// Close closes the topic manager
	Close() error
}

type topicManager struct {
	admin              sarama.ClusterAdmin
	client             sarama.Client
	topicManagerConfig *TopicManagerConfig
}

// NewTopicManager creates a new topic manager using the sarama library
func NewTopicManager(brokers []string, saramaConfig *sarama.Config, topicManagerConfig *TopicManagerConfig) (TopicManager, error) {

	if !saramaConfig.Version.IsAtLeast(sarama.V0_10_0_0) {
		return nil, fmt.Errorf("goka's topic manager needs kafka version v0.10.0.0 or higher to function. Version is %s", saramaConfig.Version.String())
	}

	client, err := sarama.NewClient(brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("Error creating the kafka client: %v", err)
	}
	return newTopicManager(brokers, saramaConfig, topicManagerConfig, client, checkBroker)
}

func newTopicManager(brokers []string, saramaConfig *sarama.Config, topicManagerConfig *TopicManagerConfig, client sarama.Client, check checkFunc) (*topicManager, error) {
	if client == nil {
		return nil, errors.New("cannot create topic manager with nil client")
	}

	if topicManagerConfig == nil {
		return nil, errors.New("cannot create topic manager with nil config")
	}

	activeBrokers := client.Brokers()
	if len(activeBrokers) == 0 {
		return nil, errors.New("no brokers active in current client")
	}

	broker := activeBrokers[0]
	err := check(broker, saramaConfig)
	if err != nil {
		return nil, err
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("error creating cluster admin: %v", err)
	}

	return &topicManager{
		admin:              admin,
		client:             client,
		topicManagerConfig: topicManagerConfig,
	}, nil
}

type checkFunc func(broker Broker, config *sarama.Config) error

func checkBroker(broker Broker, config *sarama.Config) error {
	if config == nil {
		config = DefaultConfig()
	}

	err := broker.Open(config)
	if err != nil {
		return fmt.Errorf("error opening broker connection: %v", err)
	}
	connected, err := broker.Connected()
	if err != nil {
		return fmt.Errorf("cannot connect to broker %s: %v", broker.Addr(), err)
	}

	if !connected {
		return fmt.Errorf("cannot connect to broker %s: not connected", broker.Addr())
	}

	return nil
}

func (m *topicManager) Close() error {
	return m.client.Close()
}

func (m *topicManager) Partitions(topic string) ([]int32, error) {
	// refresh metadata, otherwise we might get an outdated number of partitions.
	// we cannot call it for that specific topic,
	// otherwise we'd create it if auto.create.topics.enable==true, which we want to avoid
	if err := m.client.RefreshMetadata(); err != nil {
		return nil, fmt.Errorf("error refreshing metadata %v", err)
	}
	topics, err := m.client.Topics()
	if err != nil {
		return nil, err
	}
	for _, tpc := range topics {
		// topic exists, let's list the partitions.
		if tpc == topic {
			return m.client.Partitions(topic)
		}
	}
	return nil, errTopicNotFound
}

func (m *topicManager) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	return m.client.GetOffset(topic, partitionID, time)
}

func (m *topicManager) createTopic(topic string, npar, rfactor int, config map[string]string) error {
	m.topicManagerConfig.Logger.Debugf("creating topic %s with npar=%d, rfactor=%d, config=%#v", topic, npar, rfactor, config)
	topicDetail := &sarama.TopicDetail{}
	topicDetail.NumPartitions = int32(npar)
	topicDetail.ReplicationFactor = int16(rfactor)
	topicDetail.ConfigEntries = make(map[string]*string)

	for k, v := range config {
		// configEntries is a map to `*string`, so we have to make a copy of the value
		// here or end up having the same value for all, since `v` has the same address everywhere
		value := v
		topicDetail.ConfigEntries[k] = &value
	}

	err := m.admin.CreateTopic(topic, topicDetail, false)
	if err != nil {
		return fmt.Errorf("error creating topic %s, npar=%d, rfactor=%d, config=%#v: %v",
			topic, npar, rfactor, config, err)
	}

	return m.waitForCreated(topic)
}

func (m *topicManager) handleConfigMismatch(message string) error {
	switch m.topicManagerConfig.MismatchBehavior {
	case TMConfigMismatchBehaviorWarn:
		m.topicManagerConfig.Logger.Printf("Warning: %s", message)
		return nil
	case TMConfigMismatchBehaviorFail:
		return fmt.Errorf("%s", message)
		// ignores per default
	default:
		return nil
	}
}

func (m *topicManager) ensureExists(topic string, npar, rfactor int, config map[string]string) error {

	partitions, err := m.Partitions(topic)

	if err != nil {
		if err != errTopicNotFound {
			return fmt.Errorf("error checking topic: %v", err)
		}
	}
	// no topic yet, let's create it
	if len(partitions) == 0 {
		return m.createTopic(topic,
			npar,
			rfactor,
			config)
	}

	// we have a topic, let's check their values

	// partitions do not match
	if len(partitions) != npar {
		return m.handleConfigMismatch(fmt.Sprintf("partition count mismatch for topic %s. Need %d, but existing topic has %d", topic, npar, len(partitions)))
	}

	// check additional config values via the cluster admin if our current version supports it
	if m.adminSupported() {
		cfgMap, err := m.getTopicConfigMap(topic)
		if err != nil {
			return err
		}

		// check for all user-passed config values whether they're as expected
		for key, value := range config {
			entry, ok := cfgMap[key]
			if !ok {
				return m.handleConfigMismatch(fmt.Sprintf("config for topic %s did not contain requested key %s", topic, key))
			}
			if entry.Value != value {
				return m.handleConfigMismatch(fmt.Sprintf("unexpected config value for topic %s. Expected %s=%s. Got %s=%s", topic, key, value, key, entry.Value))
			}
		}

		// check if the number of replicas match what we expect
		topicMinReplicas, err := m.getTopicMinReplicas(topic)
		if err != nil {
			return err
		}
		if topicMinReplicas != rfactor {
			return m.handleConfigMismatch(fmt.Sprintf("unexpected replication factor for topic %s. Expected %d, got %d",
				topic,
				rfactor,
				topicMinReplicas))
		}
	}

	return nil
}

func (m *topicManager) waitForCreated(topic string) error {
	// no timeout defined -> no check
	if m.topicManagerConfig.CreateTopicTimeout == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), m.topicManagerConfig.CreateTopicTimeout)
	defer cancel()

	for ctx.Err() == nil {
		_, err := m.Partitions(topic)
		switch err {
		case nil:
			return nil
		case errTopicNotFound:
			time.Sleep(time.Second)
		default:
			return fmt.Errorf("error checking topic: %w", err)
		}
	}
	return fmt.Errorf("waiting for topic %s to be created timed out", topic)
}

func (m *topicManager) adminSupported() bool {
	return m.client.Config().Version.IsAtLeast(sarama.V0_11_0_0)
}

func (m *topicManager) getTopicConfigMap(topic string) (map[string]sarama.ConfigEntry, error) {
	cfg, err := m.admin.DescribeConfig(sarama.ConfigResource{
		Type: sarama.TopicResource,
		Name: topic,
	})

	// now it does not exist anymore -- this means the cluster is somehow unstable
	if err != nil {
		return nil, fmt.Errorf("Error getting config for topic %s: %w", topic, err)
	}

	// remap the config values to a map
	cfgMap := make(map[string]sarama.ConfigEntry, len(cfg))
	for _, cfgEntry := range cfg {
		cfgMap[cfgEntry.Name] = cfgEntry
	}
	return cfgMap, nil
}

func (m *topicManager) getTopicMinReplicas(topic string) (int, error) {
	topicsMeta, err := m.admin.DescribeTopics([]string{topic})
	if err != nil {
		return 0, fmt.Errorf("Error describing topic %s: %w", topic, err)
	}
	if len(topicsMeta) != 1 {
		return 0, fmt.Errorf("cannot find meta data for topic %s", topic)
	}

	topicMeta := topicsMeta[0]
	var replicasMin int
	for _, part := range topicMeta.Partitions {
		if replicasMin == 0 || len(part.Replicas) < replicasMin {
			replicasMin = len(part.Replicas)
		}
	}
	return replicasMin, nil
}

func (m *topicManager) EnsureStreamExists(topic string, npar int) error {
	return m.ensureExists(
		topic,
		npar,
		m.topicManagerConfig.Stream.Replication,
		map[string]string{
			"cleanup.policy": m.topicManagerConfig.streamCleanupPolicy(),
			"retention.ms":   fmt.Sprintf("%d", m.topicManagerConfig.Stream.Retention.Milliseconds()),
		})
}

func (m *topicManager) EnsureTopicExists(topic string, npar, rfactor int, config map[string]string) error {
	return m.ensureExists(
		topic,
		npar,
		rfactor,
		config)
}

func (m *topicManager) EnsureTableExists(topic string, npar int) error {

	return m.ensureExists(
		topic,
		npar,
		m.topicManagerConfig.Table.Replication,
		map[string]string{
			"cleanup.policy": m.topicManagerConfig.tableCleanupPolicy(),
		})
}

// TMConfigMismatchBehavior configures how configuration mismatches of a topic (replication, num partitions, compaction) should be
// treated
type TMConfigMismatchBehavior int

const (
	// TMConfigMismatchBehaviorIgnore ignore wrong config values
	TMConfigMismatchBehaviorIgnore TMConfigMismatchBehavior = 0

	// TMConfigMismatchBehaviorWarn warns if the topic is configured differently than requested
	TMConfigMismatchBehaviorWarn TMConfigMismatchBehavior = 1

	// TMConfigMismatchBehaviorFail makes checking the topic fail, if the configuration different than requested
	TMConfigMismatchBehaviorFail TMConfigMismatchBehavior = 2
)

// TopicManagerConfig contains options of to create tables and stream topics.
type TopicManagerConfig struct {
	Logger logger
	Table  struct {
		Replication int
		// CleanupPolicy allows to overwrite the default cleanup policy for streams.
		// Defaults to 'compact' if not set
		CleanupPolicy string
	}
	Stream struct {
		Replication int
		Retention   time.Duration
		// CleanupPolicy allows to overwrite the default cleanup policy for streams.
		// Defaults to 'delete' if not set
		CleanupPolicy string
	}

	// CreateTopicTimeout timeout for the topic manager to wait for the topic being created.
	// Set to 0 to turn off checking topic creation.
	// Defaults to 10 seconds
	CreateTopicTimeout time.Duration

	// TMConfigMismatchBehavior configures how configuration mismatches of a topic (replication, num partitions, compaction) should be
	// treated
	MismatchBehavior TMConfigMismatchBehavior
}

func (tmc *TopicManagerConfig) streamCleanupPolicy() string {

	if tmc.Stream.CleanupPolicy != "" {
		return tmc.Stream.CleanupPolicy
	}
	return "delete"
}

func (tmc *TopicManagerConfig) tableCleanupPolicy() string {
	if tmc.Table.CleanupPolicy != "" {
		return tmc.Table.CleanupPolicy
	}
	return "compact"
}

// NewTopicManagerConfig provides a default configuration for auto-creation
// with replication factor of 2 and rentention time of 1 hour.
// Use this function rather than creating TopicManagerConfig from scratch to
// initialize the config with reasonable defaults
func NewTopicManagerConfig() *TopicManagerConfig {
	cfg := new(TopicManagerConfig)
	cfg.Table.Replication = 2
	cfg.Stream.Replication = 2
	cfg.Stream.Retention = 1 * time.Hour

	cfg.MismatchBehavior = TMConfigMismatchBehaviorIgnore
	cfg.Logger = defaultLogger.Prefix("topic_manager")
	cfg.CreateTopicTimeout = 10 * time.Second
	return cfg
}
