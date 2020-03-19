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
	tmTestBrokers []string = []string{"0"}
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
		brokers:            tmTestBrokers,
		broker:             bm.broker,
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
			config    *sarama.Config = DefaultConfig()
			connected bool           = true
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
			config    *sarama.Config = DefaultConfig()
			connected bool           = false
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
			config    *sarama.Config = DefaultConfig()
			connected bool           = false
			errRet    error          = errors.New("some-error")
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

		var (
			broker *sarama.Broker = &sarama.Broker{}
		)

		bm.client.EXPECT().Brokers().Return([]*sarama.Broker{
			broker,
		})

		tm, err := newTopicManager(tmTestBrokers, DefaultConfig(), NewTopicManagerConfig(), bm.client, trueCheckFunc)
		test.AssertNil(t, err)
		test.AssertEqual(t, tm.brokers, tmTestBrokers)
		test.AssertEqual(t, tm.client, bm.client)
		test.AssertEqual(t, tm.broker, broker)
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

		var (
			broker *sarama.Broker = &sarama.Broker{}
		)

		bm.client.EXPECT().Brokers().Return([]*sarama.Broker{
			broker,
		})

		_, err := newTopicManager(tmTestBrokers, DefaultConfig(), NewTopicManagerConfig(), bm.client, falseCheckFunc)
		test.AssertNotNil(t, err)
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
			topic string = "some-topic"
		)
		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)
		_, err := tm.Partitions(topic)
		test.AssertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic string = "some-topic"
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
			topic     string = "some-topic"
			partition int32  = 0
			offset    int64  = sarama.OffsetNewest
		)
		bm.client.EXPECT().GetOffset(topic, partition, offset).Return(sarama.OffsetNewest, nil)
		_, err := tm.GetOffset(topic, partition, offset)
		test.AssertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic     string = "some-topic"
			partition int32  = 0
			offset    int64  = sarama.OffsetNewest
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
			topic string = "some-topic"
			npar  int    = 1
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
			topic string = "some-topic"
			npar  int    = 1
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
			topic     string = "some-topic"
			npar      int    = 1
			falseNPar int    = 2
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
			topic string = "some-topic"
			npar  int    = 1
		)

		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, nil)

		err := tm.EnsureStreamExists(topic, npar)
		test.AssertNil(t, err)
	})
	t.Run("create", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic   string = "some-topic"
			npar    int    = 1
			rfactor int    = 1
		)

		tm.topicManagerConfig.Stream.Replication = rfactor
		tm.topicManagerConfig.Stream.Retention = time.Second
		bm.client.EXPECT().Partitions(topic).Return(nil, sarama.ErrUnknownTopicOrPartition)
		bm.broker.EXPECT().CreateTopics(gomock.Any()).Return(nil, nil)

		err := tm.EnsureStreamExists(topic, npar)
		test.AssertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic  string = "some-topic"
			npar   int    = 1
			retErr error  = errors.New("some-error")
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
			topic   string            = "some-topic"
			npar    int               = 1
			rfactor int               = 1
			config  map[string]string = map[string]string{
				"a": "a",
			}
		)
		bm.broker.EXPECT().CreateTopics(gomock.Any()).Return(nil, nil)
		err := tm.createTopic(topic, npar, rfactor, config)
		test.AssertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic   string            = "some-topic"
			npar    int               = 1
			rfactor int               = 1
			config  map[string]string = map[string]string{
				"a": "a",
			}
			retErr error  = errors.New("some-error")
			errMsg string = "some-error-msg"
		)
		bm.broker.EXPECT().CreateTopics(gomock.Any()).Return(&sarama.CreateTopicsResponse{
			TopicErrors: map[string]*sarama.TopicError{
				"a": &sarama.TopicError{
					Err:    sarama.KError(0),
					ErrMsg: &errMsg,
				},
			},
		}, retErr)
		err := tm.createTopic(topic, npar, rfactor, config)
		test.AssertNotNil(t, err)
	})
}

func TestTM_EnsureTopicExists(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic   string            = "some-topic"
			npar    int               = 1
			rfactor int               = 1
			config  map[string]string = map[string]string{
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
			topic   string            = "some-topic"
			npar    int               = 1
			rfactor int               = 1
			config  map[string]string = map[string]string{
				"a": "a",
			}
		)

		bm.client.EXPECT().Partitions(topic).Return(nil, sarama.ErrUnknownTopicOrPartition)
		bm.broker.EXPECT().CreateTopics(gomock.Any()).Return(nil, nil)

		err := tm.EnsureTopicExists(topic, npar, rfactor, config)
		test.AssertNil(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		tm, bm, ctrl := createTopicManager(t)
		defer ctrl.Finish()
		var (
			topic   string            = "some-topic"
			npar    int               = 1
			rfactor int               = 1
			config  map[string]string = map[string]string{
				"a": "a",
			}
			retErr error = errors.New("some-error")
		)

		bm.client.EXPECT().Partitions(topic).Return([]int32{0}, retErr)

		err := tm.EnsureTopicExists(topic, npar, rfactor, config)
		test.AssertNotNil(t, err)
	})
}
