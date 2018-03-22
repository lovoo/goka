package kafka

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	kazoo "github.com/db7/kazoo-go"
)

// TopicManager provides an interface to create/check topics and their partitions
type TopicManager interface {
	// EnsureTableExists checks that a table (log-compacted topic) exists, or create one if possible
	EnsureTableExists(topic string, npar int) error
	// EnsureStreamExists checks that a stream topic exists, or create one if possible
	EnsureStreamExists(topic string, npar int) error

	// Partitions returns the number of partitions of a topic, that are assigned to the running
	// instance, i.e. it doesn't represent all partitions of a topic.
	Partitions(topic string) ([]int32, error)

	// Close closes the topic manager
	Close() error
}

type saramaTopicManager struct {
	brokers []string
	client  sarama.Client
}

// NewSaramaTopicManager creates a new topic manager using the sarama library
func NewSaramaTopicManager(brokers []string, config *sarama.Config) (TopicManager, error) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("Error creating the kafka client: %v", err)
	}

	return &saramaTopicManager{
		brokers: brokers,
		client:  client,
	}, nil
}

func (m *saramaTopicManager) Close() error {
	return m.client.Close()
}

func (m *saramaTopicManager) Partitions(topic string) ([]int32, error) {
	return m.client.Partitions(topic)
}

func (m *saramaTopicManager) EnsureStreamExists(topic string, npar int) error {
	return m.EnsureTableExists(topic, npar)
}

func (m *saramaTopicManager) EnsureTableExists(topic string, npar int) error {
	par, err := m.client.Partitions(topic)
	if err != nil {
		return fmt.Errorf("could not ensure %s exists: %v", topic, err)
	}
	if len(par) != npar {
		return fmt.Errorf("topic %s has %d partitions instead of %d", topic, len(par), npar)
	}
	return nil
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

type topicManager struct {
	zk      kzoo
	servers []string
	config  *TopicManagerConfig
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

// NewTopicManager creates a new topic manager for managing topics with zookeeper
func NewTopicManager(servers []string, config *TopicManagerConfig) (TopicManager, error) {
	servers, chroot, err := updateChroot(servers)
	if err != nil {
		return nil, err
	}

	cfg := kazoo.NewConfig()
	cfg.Chroot = chroot

	if config == nil {
		config = NewTopicManagerConfig()
	}

	kzoo, err := kazoo.NewKazoo(servers, cfg)
	if err != nil {
		return nil, fmt.Errorf("could not connect to ZooKeeper: %v", err)
	}

	return &topicManager{
		zk:     kzoo,
		config: config,
	}, nil
}

func (m *topicManager) Close() error {
	return m.zk.Close()
}

func (m *topicManager) EnsureTableExists(topic string, npar int) error {
	err := checkTopic(
		m.zk, topic, npar,
		m.config.Table.Replication,
		map[string]string{"cleanup.policy": "compact"},
	)
	if err != nil {
		return err
	}
	// check number of partitions
	return m.checkPartitions(topic, npar)
}

func (m *topicManager) EnsureStreamExists(topic string, npar int) error {
	retention := int(m.config.Stream.Retention.Nanoseconds() / time.Millisecond.Nanoseconds())
	err := checkTopic(
		m.zk, topic, npar,
		m.config.Stream.Replication,
		map[string]string{"retention.ms": strconv.Itoa(retention)},
	)
	if err != nil {
		return err
	}
	return m.checkPartitions(topic, npar)
}

func (m *topicManager) Partitions(topic string) ([]int32, error) {
	tl, err := m.zk.Topics()
	if err != nil {
		return nil, err
	}
	t := tl.Find(topic)
	if t == nil {
		return nil, nil
	}

	pl, err := t.Partitions()
	if err != nil {
		return nil, err
	}
	var partitions []int32
	for _, p := range pl {
		partitions = append(partitions, p.ID)
	}
	return partitions, nil
}

// ensure topic exists
func checkTopic(kz kzoo, topic string, npar int, rfactor int, cfg map[string]string) error {
	ok, err := hasTopic(kz, topic)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	err = kz.CreateTopic(topic, npar, rfactor, cfg)
	if err != nil {
		return err
	}

	return nil
}

// returns true if topic exists, false otherwise
func hasTopic(kz kzoo, topic string) (bool, error) {
	topics, err := kz.Topics()
	if err != nil {
		return false, err
	}
	for _, t := range topics {
		if t.Name == topic {
			return true, nil
		}
	}
	return false, nil
}

// check that the number of paritions match npar using kazoo library
func (m *topicManager) checkPartitions(topic string, npar int) error {
	t := m.zk.Topic(topic)

	partitions, err := t.Partitions()
	if err != nil {
		return fmt.Errorf("Error fetching partitions for topic %s: %v", topic, err)
	}
	if len(partitions) != npar {
		return fmt.Errorf("Topic %s does not have %d partitions", topic, npar)
	}
	return nil
}

// check that the number of paritions match
func checkPartitions(client sarama.Client, topic string, npar int) error {
	// check if topic has npar partitions
	partitions, err := client.Partitions(topic)
	if err != nil {
		return fmt.Errorf("Error fetching partitions for topic %s: %v", topic, err)
	}
	if len(partitions) != npar {
		return fmt.Errorf("Topic %s has %d partitions instead of %d", topic, len(partitions), npar)
	}
	return nil
}

func updateChroot(servers []string) (servs []string, chroot string, err error) {
	// find chroot in server addresses
	for _, server := range servers {
		splt := strings.Split(server, "/")
		if len(splt) == 1 {
			// no chroot in address
			servs = append(servs, server)
			continue
		}
		if len(splt) > 2 {
			err = fmt.Errorf("Could not parse %s properly", server)
			return
		}
		servs = append(servs, splt[0])
		c := fmt.Sprintf("/%s", splt[1])
		if chroot == "" {
			chroot = c
		} else if c != chroot {
			err = fmt.Errorf("Multiple chroot set (%s != %s)", c, chroot)
			return
		}
	}
	return
}

//go:generate mockgen -package mock -destination mock/kazoo.go -source=topic_manager.go kzoo
type kzoo interface {
	Topic(topic string) *kazoo.Topic
	Topics() (kazoo.TopicList, error)
	CreateTopic(topic string, npar int, rep int, config map[string]string) error
	Close() error
}
