package goka

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/lovoo/goka/storage"
)

type builderMock struct {
	ctrl *gomock.Controller
	st   *MockStorage
	tmgr *MockTopicManager
	// consumer sarama.Consumer
}

func newBuilderMock(ctrl *gomock.Controller) *builderMock {
	return &builderMock{
		ctrl: ctrl,
		st:   NewMockStorage(ctrl),
		tmgr: NewMockTopicManager(ctrl),
	}
}

func (bm *builderMock) createProcessorOptions() []ProcessorOption {
	return []ProcessorOption{
		WithStorageBuilder(bm.getStorageBuilder()),
		WithTopicManagerBuilder(bm.getTopicManagerBuilder()),
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
