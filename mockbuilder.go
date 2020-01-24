package goka

import (
	"fmt"
	"hash"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/lovoo/goka/storage"
)

// func createMockTopicManagerBuilder(t *testing.T) (TopicManagerBuilder, *MockTopicManager) {
// 	tm := NewMockTopicManager(1, 1)

// 	return func(broker []string) (TopicManager, error) {
// 		return tm, nil
// 	}, tm
// }

// func createMockProducer(t *testing.T) (ProducerBuilder, *MockProducer) {
// 	pb := NewMockProducer(t)

// 	return func(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error) {
// 		return pb, nil
// 	}, pb
// }

type builderMock struct {
	ctrl          *gomock.Controller
	st            *MockStorage
	tmgr          *MockTopicManager
	consumerGroup *MockConsumerGroup
	producer      *MockProducer
}

func newBuilderMock(ctrl *gomock.Controller) *builderMock {
	return &builderMock{
		ctrl:     ctrl,
		st:       NewMockStorage(ctrl),
		tmgr:     NewMockTopicManager(ctrl),
		producer: NewMockProducer(ctrl),
	}
}

func (bm *builderMock) createProcessorOptions(consBuilder SaramaConsumerBuilder, groupBuilder ConsumerGroupBuilder) []ProcessorOption {
	return []ProcessorOption{
		WithStorageBuilder(bm.getStorageBuilder()),
		WithTopicManagerBuilder(bm.getTopicManagerBuilder()),
		WithProducerBuilder(bm.getProducerBuilder()),
		WithConsumerGroupBuilder(groupBuilder),
		WithConsumerSaramaBuilder(consBuilder),
	}
}

func (bm *builderMock) getStorageBuilder() storage.Builder {
	return func(topic string, partition int32) (storage.Storage, error) {
		return bm.st, nil
	}
}

func (bm *builderMock) getTopicManagerBuilder() TopicManagerBuilder {
	return func([]string) (TopicManager, error) {
		return bm.tmgr, nil
	}
}

func (bm *builderMock) getProducerBuilder() ProducerBuilder {
	return func(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error) {
		return bm.producer, nil
	}
}

// func (bm *builderMock) BuildStorage(topic string, partition int32) (storage.Storage, error) {
//  return bm.st, nil
// }
// func (bm *builderMock) InitTopics(topics []string){
//  for ..
//  bm.tpmgr.EXPECT().
// }

func errStorageBuilder() storage.Builder {
	return func(topic string, partition int32) (storage.Storage, error) {
		return nil, fmt.Errorf("error returned by errStorageBuilder")
	}
}

func defaultSaramaConsumerMock(t *testing.T) *MockConsumer {
	return NewMockConsumer(t, DefaultConfig())
}
