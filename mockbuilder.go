package goka

import (
	"errors"
	"fmt"
	"hash"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/lovoo/goka/storage"
)

var (
	errProducerBuilder error = errors.New("building producer failed on purpose")
)

type builderMock struct {
	ctrl          *gomock.Controller
	st            storage.Storage
	mst           *MockStorage
	tmgr          *MockTopicManager
	consumerGroup *MockConsumerGroup
	producer      *MockProducer
	client        *MockClient
	admin         *MockClusterAdmin
}

func newBuilderMock(ctrl *gomock.Controller) *builderMock {
	return &builderMock{
		ctrl:     ctrl,
		mst:      NewMockStorage(ctrl),
		tmgr:     NewMockTopicManager(ctrl),
		producer: NewMockProducer(ctrl),
		client:   NewMockClient(ctrl),
		admin:    NewMockClusterAdmin(ctrl),
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
		if bm.st != nil {
			return bm.st, nil
		}
		return bm.mst, nil
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

func (bm *builderMock) getErrorProducerBuilder() ProducerBuilder {
	return func(brokers []string, clientID string, hasher func() hash.Hash32) (Producer, error) {
		return nil, errProducerBuilder
	}
}

func (bm *builderMock) useMemoryStorage() {
	bm.st = storage.NewMemory()
}

func errStorageBuilder() storage.Builder {
	return func(topic string, partition int32) (storage.Storage, error) {
		return nil, fmt.Errorf("error returned by errStorageBuilder")
	}
}

func defaultSaramaAutoConsumerMock(t *testing.T) *MockAutoConsumer {
	return NewMockAutoConsumer(t, DefaultConfig())
}
