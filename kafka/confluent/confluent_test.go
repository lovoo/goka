package confluent

import (
	"testing"

	"github.com/golang/mock/gomock"
	"stash.lvint.de/lab/goka/mock"
)

func newMockConfluent(consumer confluentConsumer) *confluent {
	return &confluent{
		consumer: consumer,
		events:   make(chan Event),
		stop:     make(chan bool),
		done:     make(chan bool),
	}
}

func TestConfluent1(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	consumer := mock.NewMockconfluentConsumer(ctrl)
	c := newMockConfluent(consumer)

	consumer.EXPECT().SubscribeTopics([]string{"t1"}, nil).Return(nil)
	c.ConnectGroup(map[Topic]Offset{"t1": -1})
}
