package goka

import (
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/lovoo/goka/internal/test"
)

var (
	tmTestBrokers = []string{"0"}
)

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
		test.AssertNil(t, err)
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
		test.AssertNotNil(t, err)
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
		test.AssertNotNil(t, err)
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
		test.AssertNotNil(t, err)
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
		tm, err := newTopicManager(tmTestBrokers, DefaultConfig(), NewTopicManagerConfig(), bm.client, trueCheckFunc)
		test.AssertNil(t, err)
		test.AssertEqual(t, tm.client, bm.client)
		test.AssertNotNil(t, tm.admin)
	})
	t.Run("fail_missing_stuff", func(t *testing.T) {
		ctrl := NewMockController(t)
		defer ctrl.Finish()
		bm := newBuilderMock(ctrl)

		_, err := newTopicManager(tmTestBrokers, nil, nil, bm.client, trueCheckFunc)
		test.AssertNotNil(t, err)

		_, err = newTopicManager(tmTestBrokers, nil, NewTopicManagerConfig(), nil, trueCheckFunc)
		test.AssertNotNil(t, err)
	})
	t.Run("fail_check", func(t *testing.T) {
		ctrl := NewMockController(t)
		defer ctrl.Finish()
		bm := newBuilderMock(ctrl)

		// expect to return one broker
		bm.client.EXPECT().Brokers().Return([]*sarama.Broker{
			new(sarama.Broker),
		})

		_, err := newTopicManager(tmTestBrokers, DefaultConfig(), NewTopicManagerConfig(), bm.client, falseCheckFunc)
		test.AssertEqual(t, err.Error(), "broker check error")
	})
}

func TestTM_Close(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		bm.client.EXPECT().Close().Return(nil)
		err := tm.Close()
		test.AssertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		bm.client.EXPECT().Close().Return(errors.New("some-error"))
		err := tm.Close()
		test.AssertNotNil(t, err)
	})
}

func TestTM_Partitions(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic = "some-topic"
		)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)
		_, err := tm.Partitions(topic)
		test.AssertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic = "some-topic"
		)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, errors.New("some-error"))
		_, err := tm.Partitions(topic)
		test.AssertNotNil(t, err)
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
		test.AssertNil(t, err)
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
		test.AssertNotNil(t, err)
	})
}

func TestTM_checkTopicExistsWithPartitions(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic = "some-topic"
			npar  = 1
		)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)
		correct, err := tm.checkTopicExistsWithPartitions(topic, npar)
		test.AssertNil(t, err)
		test.AssertTrue(t, correct)
	})
	t.Run("unknown", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic = "some-topic"
			npar  = 1
		)
		bm.client.EXPECT().Partitions(topic).Return(nil, sarama.ErrUnknownTopicOrPartition)
		correct, err := tm.checkTopicExistsWithPartitions(topic, npar)
		test.AssertNil(t, err)
		test.AssertTrue(t, !correct)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic     = "some-topic"
			npar      = 1
			falseNPar = 2
		)
		bm.client.EXPECT().Partitions(topic).Return(nil, errors.New("some-error"))
		correct, err := tm.checkTopicExistsWithPartitions(topic, npar)
		test.AssertNotNil(t, err)
		test.AssertTrue(t, !correct)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)
		correct, err = tm.checkTopicExistsWithPartitions(topic, falseNPar)
		test.AssertNotNil(t, err)
		test.AssertTrue(t, !correct)
	})
}

func TestTM_EnsureStreamExists(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic = "some-topic"
			npar  = 1
		)

		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)

		err := tm.EnsureStreamExists(topic, npar)
		test.AssertNil(t, err)
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
		bm.client.EXPECT().Partitions(topic).Return(nil, sarama.ErrUnknownTopicOrPartition)
		bm.admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), false).Return(nil)

		err := tm.EnsureStreamExists(topic, npar)
		test.AssertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic  = "some-topic"
			npar   = 1
			retErr = errors.New("some-error")
		)

		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, retErr)

		err := tm.EnsureStreamExists(topic, npar)
		test.AssertNotNil(t, err)
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
		bm.admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), false).Return(nil)
		err := tm.createTopic(topic, npar, rfactor, config)
		test.AssertNil(t, err)
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
		test.AssertNotNil(t, err)
	})
}

func TestTM_EnsureTopicExists(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
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

		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)

		err := tm.EnsureTopicExists(topic, npar, rfactor, config)
		test.AssertNil(t, err)
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

		bm.client.EXPECT().Partitions(topic).Return(nil, sarama.ErrUnknownTopicOrPartition)
		bm.admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), false).Return(nil)

		err := tm.EnsureTopicExists(topic, npar, rfactor, config)
		test.AssertNil(t, err)
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

		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, retErr)

		err := tm.EnsureTopicExists(topic, npar, rfactor, config)
		test.AssertNotNil(t, err)
	})
}
