package goka

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/multierr"
	kazoo "github.com/wvanbergen/kazoo-go"
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

type saramaTopicManager struct {
	brokers            []string
	broker             *sarama.Broker
	client             sarama.Client
	topicManagerConfig *TopicManagerConfig
}

// NewSaramaTopicManager creates a new topic manager using the sarama library
func NewSaramaTopicManager(brokers []string, saramaConfig *sarama.Config, topicManagerConfig *TopicManagerConfig) (TopicManager, error) {
	client, err := sarama.NewClient(brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("Error creating the kafka client: %v", err)
	}

	if topicManagerConfig == nil {
		return nil, fmt.Errorf("Cannot create topic manager with nil config")
	}

	activeBrokers := client.Brokers()
	if len(activeBrokers) == 0 {
		return nil, fmt.Errorf("No brokers active in current client")
	}

	broker := activeBrokers[0]
	err = broker.Open(saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("error opening broker connection: %v", err)
	}
	connected, err := broker.Connected()
	if err != nil {
		return nil, fmt.Errorf("cannot connect to broker %s: %v", brokers[0], err)
	}

	if !connected {
		return nil, fmt.Errorf("cannot connect to broker %s: not connected", brokers[0])
	}

	return &saramaTopicManager{
		brokers:            brokers,
		client:             client,
		broker:             broker,
		topicManagerConfig: topicManagerConfig,
	}, nil
}

func (m *saramaTopicManager) Close() error {
	errs := new(multierr.Errors)
	errs.Collect(m.client.Close())

	return errs.NilOrError()
}

func (m *saramaTopicManager) Partitions(topic string) ([]int32, error) {
	return m.client.Partitions(topic)
}

func (m *saramaTopicManager) EnsureStreamExists(topic string, npar int) error {
	exists, err := m.checkTopicExistsWithPartitions(topic, npar)
	if err != nil {
		return fmt.Errorf("error checking topic exists: %v", err)
	}
	if exists {
		return nil
	}
	return m.createTopic(topic,
		npar,
		m.topicManagerConfig.Stream.Replication,
		map[string]string{
			"retention.ms": fmt.Sprintf("%d", m.topicManagerConfig.Stream.Retention),
		})
}

func (m *saramaTopicManager) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	return m.client.GetOffset(topic, partitionID, time)
}

func (m *saramaTopicManager) checkTopicExistsWithPartitions(topic string, npar int) (bool, error) {
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

func (m *saramaTopicManager) createTopic(topic string, npar, rfactor int, config map[string]string) error {
	topicDetail := &sarama.TopicDetail{}
	topicDetail.NumPartitions = int32(npar)
	topicDetail.ReplicationFactor = int16(rfactor)
	topicDetail.ConfigEntries = make(map[string]*string)

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

func (m *saramaTopicManager) EnsureTopicExists(topic string, npar, rfactor int, config map[string]string) error {
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
		map[string]string{})
}

func (m *saramaTopicManager) EnsureTableExists(topic string, npar int) error {
	exists, err := m.checkTopicExistsWithPartitions(topic, npar)
	if err != nil {
		return fmt.Errorf("error checking topic exists: %v", err)
	}
	if exists {
		return nil
	}
	return m.createTopic(topic,
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

type zkTopicManager struct {
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

	return &zkTopicManager{
		zk:     kzoo,
		config: config,
	}, nil
}

func (m *zkTopicManager) Close() error {
	return m.zk.Close()
}

func (m *zkTopicManager) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	return 0, fmt.Errorf("Not implemented")
}

func (m *zkTopicManager) EnsureTableExists(topic string, npar int) error {
	err := checkTopic(
		m.zk, topic, npar,
		m.config.Table.Replication,
		map[string]string{"cleanup.policy": "compact"},
		false,
	)
	if err != nil {
		return err
	}
	// check number of partitions
	return m.checkPartitions(topic, npar)
}

func (m *zkTopicManager) EnsureStreamExists(topic string, npar int) error {
	retention := int(m.config.Stream.Retention.Nanoseconds() / time.Millisecond.Nanoseconds())
	err := checkTopic(
		m.zk, topic, npar,
		m.config.Stream.Replication,
		map[string]string{"retention.ms": strconv.Itoa(retention)},
		false,
	)
	if err != nil {
		return err
	}

	return m.checkPartitions(topic, npar)
}

func (m *zkTopicManager) EnsureTopicExists(topic string, npar, rfactor int, config map[string]string) error {
	if err := checkTopic(m.zk, topic, npar, rfactor, config, true); err != nil {
		return err
	}
	return m.checkPartitions(topic, npar)
}

func (m *zkTopicManager) Partitions(topic string) ([]int32, error) {
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
func checkTopic(kz kzoo, topic string, npar int, rfactor int, cfg map[string]string, ensureConfig bool) error {
	ok, err := hasTopic(kz, topic)
	if err != nil {
		return err
	}
	if !ok {
		err = kz.CreateTopic(topic, npar, rfactor, cfg)
		if err != nil {
			return err
		}
	}
	if !ensureConfig {
		return nil
	}
	// topic exists, check if config the same
	c, err := kz.Topic(topic).Config()
	if err != nil {
		return err
	}
	for k, v := range cfg {
		if c[k] != v {
			return fmt.Errorf("expected %s=%s, but found %s", k, cfg[k], c[k])
		}
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
func (m *zkTopicManager) checkPartitions(topic string, npar int) error {
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

func updateChroot(servers []string) (servs []string, chroot string, err error) {
	// find chroot in server addresses
	for _, server := range servers {
		for strings.HasSuffix(server, "/") {
			server = server[:len(server)-1]
		}
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
