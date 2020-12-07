package goka

import (
	"fmt"
	"math"
	"sort"

	"github.com/Shopify/sarama"
)

var (
	// CopartitioningStrategy is the rebalance strategy necessary to guarantee the copartitioning
	// when consuming multiple input topics with multiple processor instances.
	// This strategy tolerates different sets of topics per member of consumer group to allow
	// rolling upgrades of processors.
	//
	// Note that the topic inconcistency needs to be only temporarily, otherwise not all topic partitions will be consumed as in
	// the following example:
	// Assume having topics X and Y, each with partitions [0,1,2]
	// MemberA requests topic X
	// MemberB requests topics X and Y, because topic Y was newly added to the processor.
	//
	// Then the strategy will plan as follows:
	// MemberA: X: [0,1]
	// MemberB: X: [2], Y:[2]
	//
	// That means partitions [0,1] from topic Y are not being consumed.
	// So the assumption is that memberA will be redeployed so that after a second rebalance
	// both members consume both topics and all partitions.
	//
	// If you do not use rolling upgrades, i.e. replace all members of a group simultaneously, it is
	// safe to use the StrictCopartitioningStrategy
	CopartitioningStrategy = new(copartitioningStrategy)

	// StrictCopartitioningStrategy behaves like the copartitioning strategy but it will fail if two members of a consumer group
	// request a different set of topics, which might indicate a bug or a reused consumer group name.
	StrictCopartitioningStrategy = &copartitioningStrategy{
		failOnInconsistentTopics: true,
	}
)

type copartitioningStrategy struct {
	failOnInconsistentTopics bool
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
			if s.failOnInconsistentTopics {
				return nil, fmt.Errorf("Error balancing. Not all members request the same list of topics. A group-name clash might be the reason: %#v", members)
			}
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
		for _, topic := range members[memberID].Topics {
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
