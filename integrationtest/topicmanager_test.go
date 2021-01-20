package integrationtest

import (
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/internal/test"
)

var (
	systemtest = flag.Bool("systemtest", false, "set to run systemtests that require a running kafka-version")
)

func TestTopicManagerCreate(t *testing.T) {
	if !*systemtest {
		t.Skipf("Ignoring systemtest. pass '-args -systemtest' to `go test` to include them")
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_10_0_0

	tm, err := goka.TopicManagerBuilderWithConfig(cfg, goka.NewTopicManagerConfig())([]string{"localhost:9092"})
	test.AssertNil(t, err)

	err = tm.EnsureTopicExists("test10", 4, 2, nil)
	test.AssertNil(t, err)

}

func TestTopicManagerv10(t *testing.T) {
	if !*systemtest {
		t.Skipf("Ignoring systemtest. pass '-args -systemtest' to `go test` to include them")
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_11_0_0
	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.MismatchBehavior = goka.TMConfigMismatchBehaviorFail

	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)([]string{"localhost:9092"})
	test.AssertNil(t, err)

	client, _ := sarama.NewClient([]string{"localhost:9092"}, cfg)
	admin, _ := sarama.NewClusterAdminFromClient(client)

	t.Run("ensure-new-stream", func(t *testing.T) {
		// delete topic, ignore error if it does not exist
		admin.DeleteTopic("tm_test_v10")

		tm.EnsureStreamExists("tm_test_v10", 10)

		// partitions changed
		err := tm.EnsureStreamExists("tm_test_v10", 11)
		test.AssertNotNil(t, err)
	})

	t.Run("list-partitions", func(t *testing.T) {
		admin.DeleteTopic("test")

		var (
			partitions []int32
			err        error
		)
		for i := 0; i < 10; i++ {
			time.Sleep(500 * time.Millisecond)
			partitions, err = tm.Partitions("test")
			if len(partitions) == 0 {
				break
			}
		}

		test.AssertNotNil(t, err)
		test.AssertTrue(t, strings.Contains(err.Error(), "requested topic was not found"))
		test.AssertEqual(t, len(partitions), 0)

		// tm.EnsureTableExists("test", 123)
		// partitions, err = tm.Partitions("test")
		// test.AssertNil(t, err)
		// test.AssertEqual(t, len(partitions), 123)

	})
	// topic does not exist
	partitions, err := tm.Partitions("non-existent-topic")
	test.AssertTrue(t, len(partitions) == 0, "expected no partitions, was", partitions)
	test.AssertNotNil(t, err)

}
