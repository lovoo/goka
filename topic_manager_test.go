package goka

import (
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var tmTestBrokers = []string{"0"}

func trueCheckFunc(broker Broker, config *sarama.Config) error {
	return nil
}

func falseCheckFunc(broker Broker, config *sarama.Config) error {
	return errors.New("broker check error")
}

func createTopicManager(t *testing.T) (*topicManager, *builderMock, *gomock.Controller) {
	ctrl := NewMockController(t)
	bm := newBuilderMock(ctrl)
	return &topicManager{
		admin:              bm.admin,
		client:             bm.client,
		topicManagerConfig: NewTopicManagerConfig(),
	}, bm, ctrl
}

func TestTM_checkBroker(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		ctrl := NewMockController(t)
		broker := NewMockBroker(ctrl)
		defer ctrl.Finish()

		var (
			config    = DefaultConfig()
			connected = true
		)
		broker.EXPECT().Open(config).Return(nil)
		broker.EXPECT().Connected().Return(connected, nil)

		err := checkBroker(broker, config)
		require.NoError(t, err)
	})
	t.Run("fail_open", func(t *testing.T) {
		ctrl := NewMockController(t)
		broker := NewMockBroker(ctrl)
		defer ctrl.Finish()

		var (
			config *sarama.Config = DefaultConfig()
			errRet error          = errors.New("some-error")
		)
		broker.EXPECT().Open(config).Return(errRet)

		err := checkBroker(broker, config)
		require.Error(t, err)
	})
	t.Run("fail_connected", func(t *testing.T) {
		ctrl := NewMockController(t)
		broker := NewMockBroker(ctrl)
		defer ctrl.Finish()

		var (
			config    = DefaultConfig()
			connected = false
		)
		broker.EXPECT().Open(config).Return(nil)
		broker.EXPECT().Connected().Return(connected, nil)
		broker.EXPECT().Addr().Return("127.0.0.1")

		err := checkBroker(broker, config)
		require.Error(t, err)
	})
	t.Run("fail_not_connected", func(t *testing.T) {
		ctrl := NewMockController(t)
		broker := NewMockBroker(ctrl)
		defer ctrl.Finish()

		var (
			config    = DefaultConfig()
			connected = false
			errRet    = errors.New("some-error")
		)
		broker.EXPECT().Open(config).Return(nil)
		broker.EXPECT().Connected().Return(connected, errRet)
		broker.EXPECT().Addr().Return("127.0.0.1")

		err := checkBroker(broker, config)
		require.Error(t, err)
	})
}

func TestTM_newTopicManager(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		ctrl := NewMockController(t)
		defer ctrl.Finish()
		bm := newBuilderMock(ctrl)

		// expect some calls to properly set up the topic manager
		bm.client.EXPECT().Controller().Return(nil, nil)
		bm.client.EXPECT().Config().Return(nil)
		bm.client.EXPECT().Brokers().Return([]*sarama.Broker{
			new(sarama.Broker),
		})
		tm, err := newTopicManager(DefaultConfig(), NewTopicManagerConfig(), bm.client, trueCheckFunc)
		require.NoError(t, err)
		require.Equal(t, tm.client, bm.client)
		require.NotNil(t, tm.admin)
	})
	t.Run("fail_missing_stuff", func(t *testing.T) {
		ctrl := NewMockController(t)
		defer ctrl.Finish()
		bm := newBuilderMock(ctrl)

		_, err := newTopicManager(nil, nil, bm.client, trueCheckFunc)
		require.Error(t, err)

		_, err = newTopicManager(nil, NewTopicManagerConfig(), nil, trueCheckFunc)
		require.Error(t, err)
	})
	t.Run("fail_check", func(t *testing.T) {
		ctrl := NewMockController(t)
		defer ctrl.Finish()
		bm := newBuilderMock(ctrl)

		// expect to return one broker
		bm.client.EXPECT().Brokers().Return([]*sarama.Broker{
			new(sarama.Broker),
		})

		_, err := newTopicManager(DefaultConfig(), NewTopicManagerConfig(), bm.client, falseCheckFunc)
		require.Equal(t, err.Error(), "broker check error")
	})
}

func TestTM_Close(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		bm.client.EXPECT().Close().Return(nil)
		err := tm.Close()
		require.NoError(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		bm.client.EXPECT().Close().Return(errors.New("some-error"))
		err := tm.Close()
		require.Error(t, err)
	})
}

func TestTM_Partitions(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		topic := "some-topic"
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Topics().Return([]string{topic}, nil)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)
		_, err := tm.Partitions(topic)
		require.NoError(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		topic := "some-topic"
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Topics().Return([]string{topic}, nil)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, errors.New("some-error"))
		_, err := tm.Partitions(topic)
		require.Error(t, err)
	})
}

func TestTM_GetOffset(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic     = "some-topic"
			partition int32
			offset    = sarama.OffsetNewest
		)
		bm.client.EXPECT().GetOffset(topic, partition, offset).Return(sarama.OffsetNewest, nil)
		_, err := tm.GetOffset(topic, partition, offset)
		require.NoError(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic     = "some-topic"
			partition int32
			offset    = sarama.OffsetNewest
		)
		bm.client.EXPECT().GetOffset(topic, partition, offset).Return(sarama.OffsetNewest, errors.New("some-error"))
		_, err := tm.GetOffset(topic, partition, offset)
		require.Error(t, err)
	})
}

func TestTM_EnsureStreamExists(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()

		tm.topicManagerConfig.MismatchBehavior = TMConfigMismatchBehaviorFail
		var (
			topic = "some-topic"
			npar  = 1
		)

		cfg := sarama.NewConfig()
		cfg.Version = sarama.V0_10_0_0
		bm.client.EXPECT().Config().Return(cfg)
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Topics().Return([]string{topic}, nil)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)

		err := tm.EnsureStreamExists(topic, npar)
		require.NoError(t, err)
	})
	t.Run("create", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic   = "some-topic"
			npar    = 1
			rfactor = 1
		)

		tm.topicManagerConfig.Stream.Replication = rfactor
		tm.topicManagerConfig.Stream.Retention = time.Second
		bm.client.EXPECT().RefreshMetadata().Return(nil).AnyTimes()

		gomock.InOrder(
			bm.client.EXPECT().Topics().Return(nil, nil),
			bm.client.EXPECT().Topics().Return([]string{"some-topic"}, nil),
			bm.client.EXPECT().Partitions("some-topic").Return([]int32{0}, nil),
		)
		bm.admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), false).Return(nil)

		err := tm.EnsureStreamExists(topic, npar)
		require.NoError(t, err)
	})
	t.Run("no-create", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic   = "some-topic"
			npar    = 1
			rfactor = 1
		)

		tm.topicManagerConfig.Stream.Replication = rfactor
		tm.topicManagerConfig.Stream.Retention = time.Second
		tm.topicManagerConfig.NoCreate = true

		bm.client.EXPECT().RefreshMetadata().Return(nil).AnyTimes()

		gomock.InOrder(
			bm.client.EXPECT().Topics().Return(nil, nil),
		)

		err := tm.EnsureStreamExists(topic, npar)
		require.Equal(t, err, errTopicNoCreate(topic))
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic  = "some-topic"
			npar   = 1
			retErr = errors.New("some-error")
		)

		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Topics().Return(nil, retErr)

		err := tm.EnsureStreamExists(topic, npar)
		require.Error(t, err)
	})
}

func TestTM_createTopic(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic   = "some-topic"
			npar    = 1
			rfactor = 1
			config  = map[string]string{
				"a": "a",
			}
		)

		bm.client.EXPECT().RefreshMetadata().Return(nil).AnyTimes()
		gomock.InOrder(
			bm.client.EXPECT().Topics().Return(nil, nil),
			bm.client.EXPECT().Topics().Return([]string{"some-topic"}, nil),
			bm.client.EXPECT().Partitions("some-topic").Return([]int32{0}, nil),
		)

		bm.admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), false).Return(nil)
		err := tm.createTopic(topic, npar, rfactor, config)
		require.NoError(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic   = "some-topic"
			npar    = 1
			rfactor = 1
			config  = map[string]string{
				"a": "a",
			}
			retErr error = errors.New("some-error")
		)
		bm.admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), false).Return(retErr)
		err := tm.createTopic(topic, npar, rfactor, config)
		require.Error(t, err)
	})
}

func TestTM_EnsureTopicExists(t *testing.T) {
	t.Run("exists_nocheck", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic   = "some-topic"
			npar    = 1
			rfactor = 1
			config  = map[string]string{
				"a": "a",
			}
		)

		cfg := sarama.NewConfig()
		cfg.Version = sarama.V0_10_0_0
		bm.client.EXPECT().Config().Return(cfg)
		bm.client.EXPECT().Topics().Return([]string{topic}, nil)
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)

		err := tm.EnsureTopicExists(topic, npar, rfactor, config)
		require.NoError(t, err)
	})
	t.Run("exists_diff_partitions_ignore", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic = "some-topic"
			npar  = 1
		)

		tm.topicManagerConfig.MismatchBehavior = TMConfigMismatchBehaviorIgnore

		bm.client.EXPECT().Topics().Return([]string{topic}, nil)
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)

		err := tm.EnsureStreamExists(topic, npar)
		require.NoError(t, err)
	})
	t.Run("exists_diff_partitions_fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic = "some-topic"
			npar  = 1
		)

		tm.topicManagerConfig.MismatchBehavior = TMConfigMismatchBehaviorFail

		bm.client.EXPECT().Topics().Return([]string{topic}, nil)
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)

		err := tm.EnsureTopicExists(topic, npar, 1, map[string]string{})
		require.Error(t, err)
	})
	t.Run("exists_diff_config", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic = "some-topic"
			npar  = 1
		)
		// make the tm fail on mismatch:
		tm.topicManagerConfig.MismatchBehavior = TMConfigMismatchBehaviorFail

		cfg := sarama.NewConfig()
		cfg.Version = sarama.V0_11_0_0
		bm.client.EXPECT().Config().Return(cfg)
		bm.client.EXPECT().Topics().Return([]string{topic}, nil)
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)
		bm.admin.EXPECT().DescribeConfig(sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: topic,
		}).Return([]sarama.ConfigEntry{{Name: "a", Value: "b"}}, nil)
		err := tm.EnsureTopicExists(topic, npar, 1, map[string]string{"a": "diff-value"})
		require.Error(t, err)
	})
	t.Run("exists_diff_rfactor", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic = "some-topic"
			npar  = 1
		)
		// make the tm fail on mismatch:
		tm.topicManagerConfig.MismatchBehavior = TMConfigMismatchBehaviorFail

		cfg := sarama.NewConfig()
		cfg.Version = sarama.V0_11_0_0
		bm.client.EXPECT().Config().Return(cfg)
		bm.client.EXPECT().Topics().Return([]string{topic}, nil)
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)
		bm.admin.EXPECT().DescribeConfig(sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: topic,
		}).Return(nil, nil)
		bm.admin.EXPECT().DescribeTopics([]string{topic}).Return([]*sarama.TopicMetadata{
			{
				Name: topic,
				Partitions: []*sarama.PartitionMetadata{
					// two topics with different replicas,
					// TM will select the one with the fewest replicas and
					// compare with what is requested
					{Replicas: []int32{0}},
					{Replicas: []int32{0, 1}},
				},
			},
		}, nil)

		// fails because rfactor is requested as 2, but the smallest one is 1
		err := tm.EnsureTopicExists(topic, npar, 2, map[string]string{})
		require.Error(t, err)
	})
	t.Run("exists_same", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic = "some-topic"
			npar  = 1
		)
		// make the tm fail on mismatch:
		tm.topicManagerConfig.MismatchBehavior = TMConfigMismatchBehaviorFail

		cfg := sarama.NewConfig()
		cfg.Version = sarama.V0_11_0_0
		bm.client.EXPECT().Config().Return(cfg)
		bm.client.EXPECT().Topics().Return([]string{topic}, nil)
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)
		bm.admin.EXPECT().DescribeConfig(sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: topic,
		}).Return([]sarama.ConfigEntry{{Name: "a", Value: "b"}}, nil)
		bm.admin.EXPECT().DescribeTopics([]string{topic}).Return([]*sarama.TopicMetadata{
			{
				Name: topic,
				Partitions: []*sarama.PartitionMetadata{
					// two topics with different replicas,
					// TM will select the one with the fewest replicas and
					// compare with what is requested
					{Replicas: []int32{0, 1}},
				},
			},
		}, nil)

		// fails because rfactor is requested as 2, but the smallest one is 1
		err := tm.EnsureTopicExists(topic, npar, 2, map[string]string{"a": "b"})
		require.NoError(t, err)
	})
	t.Run("create", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic   = "some-topic"
			npar    = 1
			rfactor = 1
			config  = map[string]string{
				"a": "a",
			}
		)

		bm.client.EXPECT().RefreshMetadata().Return(nil).AnyTimes()
		gomock.InOrder(
			bm.client.EXPECT().Topics().Return(nil, nil),
			bm.client.EXPECT().Topics().Return([]string{"some-topic"}, nil),
			bm.client.EXPECT().Partitions("some-topic").Return([]int32{0}, nil),
		)

		bm.admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), false).Return(nil)

		err := tm.EnsureTopicExists(topic, npar, rfactor, config)
		require.NoError(t, err)
	})
	t.Run("create-nowait", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		tm.topicManagerConfig.CreateTopicTimeout = 0
		defer ctrl.Finish()
		var (
			topic   = "some-topic"
			npar    = 1
			rfactor = 1
			config  = map[string]string{
				"a": "a",
			}
		)

		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Topics().Return(nil, nil)

		bm.admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), false).Return(nil)

		err := tm.EnsureTopicExists(topic, npar, rfactor, config)
		require.NoError(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic   = "some-topic"
			npar    = 1
			rfactor = 1
			config  = map[string]string{
				"a": "a",
			}
			retErr error = errors.New("some-error")
		)

		// client.Topics() fails
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Topics().Return(nil, retErr)
		err := tm.EnsureTopicExists(topic, npar, rfactor, config)
		require.Error(t, err)

		// client.Partitions() fails
		bm.client.EXPECT().Topics().Return([]string{topic}, nil)
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Partitions(topic).Return(nil, retErr)
		err = tm.EnsureTopicExists(topic, npar, rfactor, config)
		require.Error(t, err)

		// client.DescribeConfig() fails
		bm.client.EXPECT().Topics().Return([]string{topic}, nil)
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)
		cfg := sarama.NewConfig()
		cfg.Version = sarama.V0_11_0_0
		bm.client.EXPECT().Config().Return(cfg)
		bm.admin.EXPECT().DescribeConfig(gomock.Any()).Return(nil, retErr)
		err = tm.EnsureTopicExists(topic, npar, rfactor, config)
		require.Error(t, err)

		// client.DescribeTopics() fails
		bm.client.EXPECT().Topics().Return([]string{topic}, nil)
		bm.client.EXPECT().RefreshMetadata().Return(nil)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)
		bm.client.EXPECT().Config().Return(cfg)
		bm.admin.EXPECT().DescribeConfig(gomock.Any()).Return([]sarama.ConfigEntry{{Name: "a", Value: "a"}}, nil)
		bm.admin.EXPECT().DescribeTopics(gomock.Any()).Return(nil, retErr)
		err = tm.EnsureTopicExists(topic, npar, rfactor, config)
		require.Error(t, err)
	})
}
