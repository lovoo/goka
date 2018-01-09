package confluent

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/lovoo/goka/kafka"
)

func newMockConfluent(consumer confluentConsumer) *confluent {
	return &confluent{
		consumer: consumer,
		events:   make(chan kafka.Event),
		stop:     make(chan bool),
		done:     make(chan bool),
	}
}

func TestConfluent1(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	consumer := NewMockconfluentConsumer(ctrl)
	c := newMockConfluent(consumer)

	consumer.EXPECT().SubscribeTopics([]string{"t1"}, nil).Return(nil)
	c.Subscribe(map[string]int64{"t1": -1})
}
