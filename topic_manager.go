package goka

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/multierr"
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
	brokers            []string
	broker             Broker
	client             sarama.Client
	topicManagerConfig *TopicManagerConfig
}

// NewTopicManager creates a new topic manager using the sarama library
func NewTopicManager(brokers []string, saramaConfig *sarama.Config, topicManagerConfig *TopicManagerConfig) (TopicManager, error) {
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

	return &topicManager{
		brokers:            brokers,
		client:             client,
		broker:             broker,
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
	errs := new(multierr.Errors)
	errs.Collect(m.client.Close())

	return errs.NilOrError()
}

func (m *topicManager) Partitions(topic string) ([]int32, error) {
	return m.client.Partitions(topic)
}

func (m *topicManager) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	return m.client.GetOffset(topic, partitionID, time)
}

func (m *topicManager) checkTopicExistsWithPartitions(topic string, npar int) (bool, error) {
	par, err := m.client.Partitions(topic)
	if err != nil {
		if err == sarama.ErrUnknownTopicOrPartition {
			return false, nil
		}
		return false, fmt.Errorf("Error checking partitions for topic %s: %v", topic, err)
	}
	if len(par) != npar {
		return false, fmt.Errorf("topic %s has %d partitions instead of %d", topic, len(par), npar)
	}
	return true, nil
}

func (m *topicManager) createTopic(topic string, npar, rfactor int, config map[string]string) error {
	topicDetail := &sarama.TopicDetail{}
	topicDetail.NumPartitions = int32(npar)
	topicDetail.ReplicationFactor = int16(rfactor)
	topicDetail.ConfigEntries = make(map[string]*string)

	for k, v := range config {
		topicDetail.ConfigEntries[k] = &v
	}

	topicDetails := make(map[string]*sarama.TopicDetail)
	topicDetails[topic] = topicDetail

	request := sarama.CreateTopicsRequest{
		Timeout:      time.Second * 15,
		TopicDetails: topicDetails,
	}
	response, err := m.broker.CreateTopics(&request)

	if err != nil {
		var errs []string
		for k, topicErr := range response.TopicErrors {
			errs = append(errs, fmt.Sprintf("%s: %s (%v)", k, topicErr.Err.Error(), topicErr.ErrMsg))
		}
		return fmt.Errorf("error creating topic %s, npar=%d, rfactor=%d, config=%#v: %v\ntopic errors:\n%s",
			topic, npar, rfactor, config, err, strings.Join(errs, "\n"))
	}

	return nil
}

func (m *topicManager) ensureExists(topic string, npar, rfactor int, config map[string]string) error {
	exists, err := m.checkTopicExistsWithPartitions(topic, npar)
	if err != nil {
		return fmt.Errorf("error checking topic exists: %v", err)
	}
	if exists {
		return nil
	}
	return m.createTopic(topic,
		npar,
		rfactor,
		config)
}

func (m *topicManager) EnsureStreamExists(topic string, npar int) error {
	return m.ensureExists(
		topic,
		npar,
		m.topicManagerConfig.Stream.Replication,
		map[string]string{
			"retention.ms": fmt.Sprintf("%d", m.topicManagerConfig.Stream.Retention),
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
			"cleanup.policy": "compact",
		})
}

// TopicManagerConfig contains the configuration to access the Zookeeper servers
// as well as the desired options of to create tables and stream topics.
type TopicManagerConfig struct {
	Table struct {
		Replication int
	}
	Stream struct {
		Replication int
		Retention   time.Duration
	}
}

// NewTopicManagerConfig provides a default configuration for auto-creation
// with replication factor of 1 and rentention time of 1 hour.
func NewTopicManagerConfig() *TopicManagerConfig {
	cfg := new(TopicManagerConfig)
	cfg.Table.Replication = 2
	cfg.Stream.Replication = 2
	cfg.Stream.Retention = 1 * time.Hour
	return cfg
}
