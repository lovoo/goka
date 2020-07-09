package goka

import (
	"fmt"
	"math"
	"sort"

	"github.com/Shopify/sarama"
)

// CopartitioningStrategy is the rebalance strategy necessary to guarantee the copartitioning
// when consuming multiple input topics with multiple processor instances
var CopartitioningStrategy = new(copartitioningStrategy)

type copartitioningStrategy struct {
}

// Name implements BalanceStrategy.
func (s *copartitioningStrategy) Name() string {
	return "copartition"
}

// Plan implements BalanceStrategy.
func (s *copartitioningStrategy) Plan(members map[string]sarama.ConsumerGroupMemberMetadata, topics map[string][]int32) (sarama.BalanceStrategyPlan, error) {
	var (
		allPartitions []int32
		allTopics     []string
		allMembers    []string
	)
	// (1) collect all topics and check they're copartitioned
	for topic, topicPartitions := range topics {
		allTopics = append(allTopics, topic)
		if len(allPartitions) == 0 {
			allPartitions = topicPartitions
		} else {
			if !s.partitionsEqual(allPartitions, topicPartitions) {
				return nil, fmt.Errorf("Error balancing. Not all topics are copartitioned. For goka, all topics need to have the same number of partitions: %#v", topics)
			}
		}
	}

	// (2) collect all members and check they consume the same topics
	for memberID, meta := range members {
		if !s.topicsEqual(allTopics, meta.Topics) {
			return nil, fmt.Errorf("Error balancing. Not all members request the same list of topics. A group-name clash might be the reason: %#v", members)
		}
		allMembers = append(allMembers, memberID)
	}

	// (3) sort the data structures for deterministic distribution
	sort.Strings(allMembers)
	sort.Strings(allTopics)
	sort.Sort(partitionSlice(allPartitions))

	// (4) create a plan and assign the same set of partitions to the members
	// in a range-like configuration (like `sarama.BalanceStrategyRange`)
	plan := make(sarama.BalanceStrategyPlan, len(allMembers))
	step := float64(len(allPartitions)) / float64(len(allMembers))
	for idx, memberID := range allMembers {
		pos := float64(idx)
		min := int(math.Floor(pos*step + 0.5))
		max := int(math.Floor((pos+1)*step + 0.5))
		for _, topic := range allTopics {
			plan.Add(memberID, topic, allPartitions[min:max]...)
		}
	}

	return plan, nil
}

// AssignmentData copartitioning strategy does not require data
func (s *copartitioningStrategy) AssignmentData(memberID string, topics map[string][]int32, generationID int32) ([]byte, error) {
	return nil, nil
}

func (s *copartitioningStrategy) partitionsEqual(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	partSet := make(map[int32]bool, len(a))
	for _, p := range a {
		partSet[p] = true
	}
	for _, p := range b {
		if !partSet[p] {
			return false
		}
	}
	return true
}
func (s *copartitioningStrategy) topicsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	topicSet := make(map[string]bool, len(a))
	for _, p := range a {
		topicSet[p] = true
	}
	for _, p := range b {
		if !topicSet[p] {
			return false
		}
	}
	return true
}

type partitionSlice []int32

func (p partitionSlice) Len() int           { return len(p) }
func (p partitionSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p partitionSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
