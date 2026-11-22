package systemtest

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/stretchr/testify/require"
)

func TestTopicManagerCreate(t *testing.T) {
	brokers := initSystemTest(t)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_11_0_0

	tm, err := goka.TopicManagerBuilderWithConfig(cfg, goka.NewTopicManagerConfig())(brokers)
	require.NoError(t, err)

	err = tm.EnsureTopicExists("test10", 4, 2, nil)
	require.NoError(t, err)
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
	require.NoError(t, err)

	client, _ := sarama.NewClient(brokers, cfg)
	admin, _ := sarama.NewClusterAdminFromClient(client)

	t.Run("ensure-new-stream", func(t *testing.T) {
		topic := newTopicName()

		// delete topic, ignore error if it does not exist
		admin.DeleteTopic(topic)

		err := tm.EnsureStreamExists(topic, 10)
		require.NoError(t, err)
		time.Sleep(1 * time.Second)
		// trying to create the same is fine
		err = tm.EnsureStreamExists(topic, 10)
		require.NoError(t, err)
		time.Sleep(1 * time.Second)
		// partitions changed - error
		err = tm.EnsureStreamExists(topic, 11)
		require.Error(t, err)
	})

	t.Run("list-partitions", func(t *testing.T) {
		var (
			topic      = newTopicName()
			partitions []int32
			err        error
		)
		_, err = tm.Partitions(topic)
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "requested topic was not found"))
		require.Equal(t, 0, len(partitions))

		tm.EnsureTableExists(topic, 123)
		time.Sleep(1 * time.Second)
		partitions, err = tm.Partitions(topic)
		require.NoError(t, err)
		require.Equal(t, 123, len(partitions))
	})

	t.Run("non-existent", func(t *testing.T) {
		// topic does not exist
		partitions, err := tm.Partitions("non-existent-topic")
		require.True(t, len(partitions) == 0, "expected no partitions, was", partitions)
		require.Error(t, err)
	})
}

func newTopicName() string {
	topicBytes := make([]byte, 4)
	rand.Read(topicBytes)
	return hex.EncodeToString(topicBytes)
}

func TestProcessorGroupTablePartitions(t *testing.T) {
	brokers := initSystemTest(t)
	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), goka.NewTopicManagerConfig())
	require.NoError(t, err)
	defer tm.Close()

	input := newTopicName()
	require.NoError(t, tm.EnsureStreamExists(input, 2))
	for _, tc := range []struct {
		name       string
		partitions int
	}{
		{"fewer", 1},
		{"matching", 2},
		{"more", 3},
		{"create", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			group := goka.Group(newTopicName())
			table := string(goka.GroupTable(group))
			if tc.partitions != 0 {
				require.NoError(t, tm.EnsureTableExists(table, tc.partitions))
			}
			graph := goka.DefineGroup(group,
				goka.Input(goka.Stream(input), new(codec.String), func(goka.Context, any) {}),
				goka.Persist(new(codec.String)),
			)
			_, err := goka.NewProcessor(brokers, graph)
			if tc.partitions == 1 || tc.partitions == 3 {
				require.ErrorContains(t, err, table)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
