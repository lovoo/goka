package integrationtest

import (
	"flag"
	"testing"

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

func TestTopicManagerPartitions(t *testing.T) {
	if !*systemtest {
		t.Skipf("Ignoring systemtest. pass '-args -systemtest' to `go test` to include them")
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_11_0_0
	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.MismatchBehavior = goka.TMConfigMismatchBehaviorWarn

	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)([]string{"localhost:9092"})
	test.AssertNil(t, err)

	// client, _ := sarama.NewClient([]string{"localhost:9092"}, cfg)
	// admin, _ := sarama.NewClusterAdminFromClient(client)
	// topics, _ := admin.DescribeTopics([]string{"test"})
	// log.Printf("topics %+v", topics[0].Partitions)

	// topic does not exist
	partitions, err := tm.Partitions("non-existent-topic")
	test.AssertTrue(t, len(partitions) == 0, "expected no partitions, was", partitions)
	test.AssertNotNil(t, err)

	tm.EnsureTableExists("test", 123)
}
