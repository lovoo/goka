package goka

import (
	"reflect"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

func TestCopartitioningStrategy(t *testing.T) {
	t.Run("name", func(t *testing.T) {
		require.Equal(t, "copartition", CopartitioningStrategy.Name())
	})

	for _, ttest := range []struct {
		name      string
		members   map[string]sarama.ConsumerGroupMemberMetadata
		topics    map[string][]int32
		hasError  bool
		useStrict bool
		expected  sarama.BalanceStrategyPlan
	}{
		{
			name: "inconsistent-topic-members",
			members: map[string]sarama.ConsumerGroupMemberMetadata{
				"M1": {Topics: []string{"T1"}},
			},
			// topics are inconsistent with members
			topics: map[string][]int32{
				"T2": {0, 1, 2},
			},
			hasError:  true,
			useStrict: true,
		},
		{
			name: "not-copartitioned",
			members: map[string]sarama.ConsumerGroupMemberMetadata{
				"M1": {Topics: []string{"T1", "T2"}},
			},
			// topics are inconsistent with members
			topics: map[string][]int32{
				"T1": {0, 1, 2},
				"T2": {0, 1},
			},
			hasError: true,
		},
		{
			name: "inconsistent-members",
			members: map[string]sarama.ConsumerGroupMemberMetadata{
				"M1": {Topics: []string{"T1", "T2"}},
				"M2": {Topics: []string{"T2"}},
			},
			// topics are inconsistent with members
			topics: map[string][]int32{
				"T1": {0, 1, 2},
				"T2": {0, 1, 2},
			},
			hasError:  true,
			useStrict: true,
		},
		{
			name: "tolerate-inconsistent-members",
			members: map[string]sarama.ConsumerGroupMemberMetadata{
				"M1": {Topics: []string{"T1", "T2"}},
				"M2": {Topics: []string{"T2"}},
			},
			// topics are inconsistent with members
			topics: map[string][]int32{
				"T1": {0, 1, 2},
				"T2": {0, 1, 2},
			},
			expected: sarama.BalanceStrategyPlan{
				"M1": map[string][]int32{
					"T1": {0, 1},
					"T2": {0, 1},
				},
				"M2": map[string][]int32{
					"T2": {2},
				},
			},
		},
		{
			name: "single-member",
			members: map[string]sarama.ConsumerGroupMemberMetadata{
				"M1": {Topics: []string{"T1"}},
			},
			// topics are inconsistent with members
			topics: map[string][]int32{
				"T1": {0, 1, 2},
			},
			expected: sarama.BalanceStrategyPlan{
				"M1": map[string][]int32{
					"T1": {0, 1, 2},
				},
			},
		},
		{
			name: "multi-member",
			members: map[string]sarama.ConsumerGroupMemberMetadata{
				"M1": {Topics: []string{"T1"}},
				"M2": {Topics: []string{"T1"}},
			},
			// topics are inconsistent with members
			topics: map[string][]int32{
				"T1": {0, 1, 2},
			},
			expected: sarama.BalanceStrategyPlan{
				"M1": map[string][]int32{
					"T1": {0, 1},
				},
				"M2": map[string][]int32{
					"T1": {2},
				},
			},
		},
		{
			name: "multi-member-multitopic",
			members: map[string]sarama.ConsumerGroupMemberMetadata{
				"M1": {Topics: []string{"T1", "T2", "T3"}},
				"M2": {Topics: []string{"T2", "T3", "T1"}},
				"M3": {Topics: []string{"T2", "T3", "T1"}},
			},
			// topics are inconsistent with members
			topics: map[string][]int32{
				"T1": {0, 1, 2, 3, 4, 5},
				"T2": {0, 1, 2, 3, 4, 5},
				"T3": {0, 1, 2, 3, 4, 5},
			},
			expected: sarama.BalanceStrategyPlan{
				"M1": map[string][]int32{
					"T1": {0, 1},
					"T2": {0, 1},
					"T3": {0, 1},
				},
				"M2": map[string][]int32{
					"T1": {2, 3},
					"T2": {2, 3},
					"T3": {2, 3},
				},
				"M3": map[string][]int32{
					"T1": {4, 5},
					"T2": {4, 5},
					"T3": {4, 5},
				},
			},
		},
	} {
		t.Run(ttest.name, func(t *testing.T) {
			var strategy sarama.BalanceStrategy = CopartitioningStrategy
			if ttest.useStrict {
				strategy = StrictCopartitioningStrategy
			}

			plan, err := strategy.Plan(ttest.members, ttest.topics)
			require.Equal(t, ttest.hasError, err != nil)
			if err == nil {
				require.True(t, reflect.DeepEqual(ttest.expected, plan), "expected", ttest.expected, "actual", plan)
			}
		})
	}
}
