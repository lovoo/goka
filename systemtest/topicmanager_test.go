package systemtest

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/internal/test"
)

func TestTopicManagerCreate(t *testing.T) {
	brokers := initSystemTest(t)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_11_0_0

	tm, err := goka.TopicManagerBuilderWithConfig(cfg, goka.NewTopicManagerConfig())(brokers)
	test.AssertNil(t, err)

	err = tm.EnsureTopicExists("test10", 4, 2, nil)
	test.AssertNil(t, err)

}

// Tests the topic manager with sarama version v11 --> so it will test topic configuration using
// the sarama.ClusterAdmin
func TestTopicManager_v11(t *testing.T) {
	brokers := initSystemTest(t)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_11_0_0
	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.MismatchBehavior = goka.TMConfigMismatchBehaviorFail

	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	test.AssertNil(t, err)

	client, _ := sarama.NewClient(brokers, cfg)
	admin, _ := sarama.NewClusterAdminFromClient(client)

	t.Run("ensure-new-stream", func(t *testing.T) {
		topic := newTopicName()

		// delete topic, ignore error if it does not exist
		admin.DeleteTopic(topic)

		err := tm.EnsureStreamExists(topic, 10)
		test.AssertNil(t, err)
		time.Sleep(1 * time.Second)
		// trying to create the same is fine
		err = tm.EnsureStreamExists(topic, 10)
		test.AssertNil(t, err)
		time.Sleep(1 * time.Second)
		// partitions changed - error
		err = tm.EnsureStreamExists(topic, 11)
		test.AssertNotNil(t, err)
	})

	t.Run("list-partitions", func(t *testing.T) {

		var (
			topic      = newTopicName()
			partitions []int32
			err        error
		)
		_, err = tm.Partitions(topic)
		test.AssertNotNil(t, err)
		test.AssertTrue(t, strings.Contains(err.Error(), "requested topic was not found"))
		test.AssertEqual(t, len(partitions), 0)

		tm.EnsureTableExists(topic, 123)
		time.Sleep(1 * time.Second)
		partitions, err = tm.Partitions(topic)
		test.AssertNil(t, err)
		test.AssertEqual(t, len(partitions), 123)

	})

	t.Run("non-existent", func(t *testing.T) {
		// topic does not exist
		partitions, err := tm.Partitions("non-existent-topic")
		test.AssertTrue(t, len(partitions) == 0, "expected no partitions, was", partitions)
		test.AssertNotNil(t, err)
	})

}

func newTopicName() string {
	topicBytes := make([]byte, 4)
	rand.Read(topicBytes)
	return hex.EncodeToString(topicBytes)
}
