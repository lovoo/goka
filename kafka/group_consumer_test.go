package kafka

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lovoo/goka/kafka/mock"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/facebookgo/ensure"
	"github.com/golang/mock/gomock"
)

var (
	brokers = []string{"localhost:9092"}
	group   = "group"
	topic1  = "topic1"
	topic2  = "topic2"
	topics  = map[string]int64{topic1: -1, topic2: -2}
)

func TestGroupConsumer_Subscribe(t *testing.T) {
	events := make(chan Event)
	c, err := newGroupConsumer(brokers, group, events, nil)
	ensure.Nil(t, err)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	consumer := mock.NewMockclusterConsumer(ctrl)
	c.consumer = consumer

	notifications := make(chan *cluster.Notification)
	errs := make(chan error)
	consumer.EXPECT().Notifications().Return(notifications)
	consumer.EXPECT().Errors().Return(errs)

	wait := make(chan bool)
	go func() {
		c.run()
		close(wait)
	}()

	// wait for running
	for {
		if atomic.LoadInt64(&c.running) != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	consumer.EXPECT().Close().Return(nil)
	err = doTimed(t, func() {
		err = c.Close()
		ensure.Nil(t, err)
		<-wait
	})
	ensure.Nil(t, err)
}

func TestGroupConsumer_Group(t *testing.T) {
	events := make(chan Event)
	c, err := newGroupConsumer(brokers, group, events, nil)
	ensure.Nil(t, err)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	consumer := mock.NewMockclusterConsumer(ctrl)
	c.consumer = consumer

	notifications := make(chan *cluster.Notification, 1)
	errs := make(chan error, 1)
	consumer.EXPECT().Notifications().Return(notifications).Times(2)
	consumer.EXPECT().Errors().Return(errs)

	wait := make(chan bool)
	go func() {
		c.run()
		close(wait)
	}()

	// test an error
	err = errors.New("some error")
	errs <- err
	consumer.EXPECT().Notifications().Return(notifications)
	consumer.EXPECT().Errors().Return(errs)
	e := <-events
	ensure.DeepEqual(t, e.(*Error).Err, err)

	// notification arrives
	notifications <- &cluster.Notification{Type: cluster.RebalanceStart}
	notifications <- &cluster.Notification{
		Type: cluster.RebalanceOK,
		Current: map[string][]int32{
			topic1: []int32{0, 1},
		}}
	n := <-events

	ensure.DeepEqual(t, n, &Assignment{
		0: -1,
		1: -1,
	})
	ensure.DeepEqual(t, c.partitionMap, map[int32]bool{
		0: false, 1: false,
	})
	// add a partition
	c.AddGroupPartition(0)

	consumer.EXPECT().Close().Return(nil)
	err = doTimed(t, func() {
		err := c.Close()
		ensure.Nil(t, err)
		<-wait
	})
	ensure.Nil(t, err)

	ensure.DeepEqual(t, c.partitionMap, map[int32]bool{
		0: true, 1: false,
	})
}

func TestGroupConsumer_CloseOnNotifications(t *testing.T) {
	events := make(chan Event)
	c, err := newGroupConsumer(brokers, group, events, nil)
	ensure.Nil(t, err)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	consumer := mock.NewMockclusterConsumer(ctrl)
	c.consumer = consumer

	notifications := make(chan *cluster.Notification)
	errs := make(chan error)
	consumer.EXPECT().Notifications().Return(notifications).AnyTimes()
	consumer.EXPECT().Errors().Return(errs).AnyTimes()

	wait := make(chan bool)
	go func() {
		c.run()
		close(wait)
	}()

	// close on error
	err = errors.New("some error")
	errs <- err

	consumer.EXPECT().Close().Return(nil)
	err = doTimed(t, func() {
		err = c.Close()
		ensure.Nil(t, err)
		<-wait
	})
	ensure.Nil(t, err)

	// close on notification
	c, err = newGroupConsumer(brokers, group, events, nil)
	ensure.Nil(t, err)
	c.consumer = consumer

	wait = make(chan bool)
	go func() {
		c.run()
		close(wait)
	}()

	notifications <- &cluster.Notification{Type: cluster.RebalanceStart}
	notifications <- &cluster.Notification{
		Type: cluster.RebalanceOK,
		Current: map[string][]int32{
			topic1: []int32{0, 1},
		}}

	consumer.EXPECT().Close().Return(nil)
	err = doTimed(t, func() {
		err = c.Close()
		ensure.Nil(t, err)
		<-wait
	})
	ensure.Nil(t, err)

}

func TestGroupConsumer_GroupConsumeMessages(t *testing.T) {
	events := make(chan Event)
	c, err := newGroupConsumer(brokers, group, events, nil)
	ensure.Nil(t, err)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	consumer := mock.NewMockclusterConsumer(ctrl)
	c.consumer = consumer

	notifications := make(chan *cluster.Notification, 1)
	errs := make(chan error, 1)
	consumer.EXPECT().Notifications().Return(notifications).Times(2)
	consumer.EXPECT().Errors().Return(errs)

	wait := make(chan bool)
	go func() {
		c.run()
		close(wait)
	}()

	notifications <- &cluster.Notification{Type: cluster.RebalanceStart}
	notifications <- &cluster.Notification{
		Type: cluster.RebalanceOK,
		Current: map[string][]int32{
			topic1: []int32{0, 1},
		}}
	n := <-events
	ensure.DeepEqual(t, n, &Assignment{
		0: -1,
		1: -1,
	})
	ensure.DeepEqual(t, c.partitionMap, map[int32]bool{
		0: false, 1: false,
	})

	// add partitions (it will start consuming messages channel)
	messages := make(chan *sarama.ConsumerMessage)
	consumer.EXPECT().Notifications().Return(notifications)
	consumer.EXPECT().Messages().Return(messages)
	consumer.EXPECT().Errors().Return(errs)
	c.AddGroupPartition(0)
	c.AddGroupPartition(1)

	// test an error
	err = errors.New("some error")
	errs <- err
	consumer.EXPECT().Notifications().Return(notifications)
	consumer.EXPECT().Messages().Return(messages)
	consumer.EXPECT().Errors().Return(errs)
	e := <-events
	ensure.DeepEqual(t, e.(*Error).Err, err)

	// process a message
	var (
		key   = []byte("key")
		value = []byte("value")
	)
	messages <- &sarama.ConsumerMessage{
		Topic:     topic1,
		Partition: 0,
		Offset:    0,
		Key:       key,
		Value:     value,
	}

	// goroutine will loop after we dequeue Events
	consumer.EXPECT().Notifications().Return(notifications)
	consumer.EXPECT().Messages().Return(messages)
	consumer.EXPECT().Errors().Return(errs)
	m := <-events
	ensure.DeepEqual(t, m, &Message{
		Topic:     topic1,
		Partition: 0,
		Offset:    0,
		Key:       string(key),
		Value:     value,
	})

	ensure.DeepEqual(t, c.partitionMap, map[int32]bool{
		0: true, 1: true,
	})

	consumer.EXPECT().Close().Return(errors.New("some error"))
	err = doTimed(t, func() {
		err := c.Close()
		ensure.NotNil(t, err)
		<-wait
	})
	ensure.Nil(t, err)

}

func TestGroupConsumer_CloseOnMessages(t *testing.T) {
	events := make(chan Event)
	c, err := newGroupConsumer(brokers, group, events, nil)
	ensure.Nil(t, err)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	consumer := mock.NewMockclusterConsumer(ctrl)
	c.consumer = consumer

	notifications := make(chan *cluster.Notification)
	errs := make(chan error)
	consumer.EXPECT().Notifications().Return(notifications).Times(2)
	consumer.EXPECT().Errors().Return(errs)

	wait := make(chan bool)
	go func() {
		c.run()
		close(wait)
	}()

	notifications <- &cluster.Notification{Type: cluster.RebalanceStart}
	notifications <- &cluster.Notification{
		Type: cluster.RebalanceOK,
		Current: map[string][]int32{
			topic1: []int32{0, 1},
		}}
	n := <-events
	ensure.DeepEqual(t, n, &Assignment{
		0: -1,
		1: -1,
	})
	ensure.DeepEqual(t, c.partitionMap, map[int32]bool{
		0: false, 1: false,
	})

	// add partitions (it will start consuming messages channel)
	messages := make(chan *sarama.ConsumerMessage)
	consumer.EXPECT().Notifications().Return(notifications)
	consumer.EXPECT().Messages().Return(messages)
	consumer.EXPECT().Errors().Return(errs)
	c.AddGroupPartition(0)
	c.AddGroupPartition(1)

	// process a message
	var (
		key   = []byte("key")
		value = []byte("value")
	)
	messages <- &sarama.ConsumerMessage{
		Topic:     topic1,
		Partition: 0,
		Offset:    0,
		Key:       key,
		Value:     value,
	}

	consumer.EXPECT().Close().Return(nil)
	err = doTimed(t, func() {
		err = c.Close()
		ensure.Nil(t, err)
		<-wait
	})
	ensure.Nil(t, err)
}

func TestGroupConsumer_CloseOnMessageErrors(t *testing.T) {
	events := make(chan Event)
	c, err := newGroupConsumer(brokers, group, events, nil)
	ensure.Nil(t, err)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	consumer := mock.NewMockclusterConsumer(ctrl)
	c.consumer = consumer

	notifications := make(chan *cluster.Notification)
	errs := make(chan error)
	consumer.EXPECT().Notifications().Return(notifications).Times(2)
	consumer.EXPECT().Errors().Return(errs)

	wait := make(chan bool)
	go func() {
		c.run()
		close(wait)
	}()

	notifications <- &cluster.Notification{Type: cluster.RebalanceStart}
	notifications <- &cluster.Notification{
		Type: cluster.RebalanceOK,
		Current: map[string][]int32{
			topic1: []int32{0, 1},
		}}
	n := <-events
	ensure.DeepEqual(t, n, &Assignment{
		0: -1,
		1: -1,
	})
	ensure.DeepEqual(t, c.partitionMap, map[int32]bool{
		0: false, 1: false,
	})

	// add partitions (it will start consuming messages channel)
	messages := make(chan *sarama.ConsumerMessage)
	consumer.EXPECT().Notifications().Return(notifications)
	consumer.EXPECT().Messages().Return(messages)
	consumer.EXPECT().Errors().Return(errs)
	c.AddGroupPartition(0)
	c.AddGroupPartition(1)

	err = errors.New("some error")
	errs <- err

	consumer.EXPECT().Close().Return(nil)
	err = doTimed(t, func() {
		err = c.Close()
		ensure.Nil(t, err)
		<-wait
	})
	ensure.Nil(t, err)
}

func TestGroupConsumer_GroupNotificationsAfterMessages(t *testing.T) {
	events := make(chan Event)
	c, err := newGroupConsumer(brokers, group, events, nil)
	ensure.Nil(t, err)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	consumer := mock.NewMockclusterConsumer(ctrl)
	c.consumer = consumer

	notifications := make(chan *cluster.Notification, 1)
	errs := make(chan error, 1)
	consumer.EXPECT().Notifications().Return(notifications).Times(2)
	consumer.EXPECT().Errors().Return(errs)

	wait := make(chan bool)
	go func() {
		c.run()
		close(wait)
	}()

	notifications <- &cluster.Notification{Type: cluster.RebalanceStart}
	notifications <- &cluster.Notification{
		Type: cluster.RebalanceOK,
		Current: map[string][]int32{
			topic1: []int32{0, 1},
		}}
	n := <-events
	ensure.DeepEqual(t, n, &Assignment{
		0: -1,
		1: -1,
	})
	ensure.DeepEqual(t, c.partitionMap, map[int32]bool{
		0: false, 1: false,
	})

	// add partitions (it will start consuming messages channel)
	messages := make(chan *sarama.ConsumerMessage)
	consumer.EXPECT().Notifications().Return(notifications)
	consumer.EXPECT().Messages().Return(messages)
	consumer.EXPECT().Errors().Return(errs)
	c.AddGroupPartition(0)
	c.AddGroupPartition(1)

	// process a message
	var (
		key   = []byte("key")
		value = []byte("value")
	)
	messages <- &sarama.ConsumerMessage{
		Topic:     topic1,
		Partition: 0,
		Offset:    0,
		Key:       key,
		Value:     value,
	}

	// goroutine will loop after we dequeue Events
	consumer.EXPECT().Notifications().Return(notifications).Times(2)
	consumer.EXPECT().Messages().Return(messages)
	consumer.EXPECT().Errors().Return(errs)
	m := <-events
	ensure.DeepEqual(t, m, &Message{
		Topic:     topic1,
		Partition: 0,
		Offset:    0,
		Key:       string(key),
		Value:     value,
	})

	ensure.DeepEqual(t, c.partitionMap, map[int32]bool{
		0: true, 1: true,
	})

	// new notification
	notifications <- &cluster.Notification{Type: cluster.RebalanceStart}
	notifications <- &cluster.Notification{
		Type: cluster.RebalanceOK,
		Current: map[string][]int32{
			topic1: []int32{0, 1, 2},
		}}
	n = <-events
	ensure.DeepEqual(t, n, &Assignment{
		0: -1,
		1: -1,
		2: -1,
	})
	ensure.DeepEqual(t, c.partitionMap, map[int32]bool{
		0: true, 1: true, 2: false,
	})

	consumer.EXPECT().Close().Return(nil)
	err = doTimed(t, func() {
		err := c.Close()
		ensure.Nil(t, err)
		<-wait
	})
	ensure.Nil(t, err)

}

func TestGroupConsumer_GroupEmptyNotifications(t *testing.T) {
	events := make(chan Event)
	c, err := newGroupConsumer(brokers, group, events, nil)
	ensure.Nil(t, err)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	consumer := mock.NewMockclusterConsumer(ctrl)
	c.consumer = consumer

	notifications := make(chan *cluster.Notification)
	messages := make(chan *sarama.ConsumerMessage)
	errs := make(chan error)
	consumer.EXPECT().Notifications().Return(notifications).AnyTimes()
	consumer.EXPECT().Messages().Return(messages).AnyTimes()
	consumer.EXPECT().Errors().Return(errs).AnyTimes()

	wait := make(chan bool)
	go func() {
		c.run()
		close(wait)
	}()

	notifications <- &cluster.Notification{Type: cluster.RebalanceStart}
	notifications <- &cluster.Notification{
		Type:    cluster.RebalanceOK,
		Current: map[string][]int32{},
	}
	n := <-events
	ensure.DeepEqual(t, n, &Assignment{})

	err = doTimed(t, func() {
		notifications <- &cluster.Notification{Type: cluster.RebalanceStart}
		notifications <- &cluster.Notification{
			Type: cluster.RebalanceOK,
			Current: map[string][]int32{
				topic1: []int32{0, 1},
			}}
		n = <-events
	})
	ensure.Nil(t, err)
	ensure.DeepEqual(t, n, &Assignment{
		0: -1,
		1: -1,
	})
	ensure.DeepEqual(t, c.partitionMap, map[int32]bool{
		0: false, 1: false,
	})

	// add partitions (it will start consuming messages channel)
	c.AddGroupPartition(0)
	c.AddGroupPartition(1)

	// process a message
	var (
		key   = []byte("key")
		value = []byte("value")
	)
	messages <- &sarama.ConsumerMessage{
		Topic:     topic1,
		Partition: 0,
		Offset:    0,
		Key:       key,
		Value:     value,
	}

	// goroutine will loop after we dequeue Events
	m := <-events
	ensure.DeepEqual(t, m, &Message{
		Topic:     topic1,
		Partition: 0,
		Offset:    0,
		Key:       string(key),
		Value:     value,
	})

	ensure.DeepEqual(t, c.partitionMap, map[int32]bool{
		0: true, 1: true,
	})

	// new notification
	notifications <- &cluster.Notification{Type: cluster.RebalanceStart}
	notifications <- &cluster.Notification{
		Type: cluster.RebalanceOK,
		Current: map[string][]int32{
			topic1: []int32{0, 1, 2},
		}}
	n = <-events
	ensure.DeepEqual(t, n, &Assignment{
		0: -1,
		1: -1,
		2: -1,
	})
	ensure.DeepEqual(t, c.partitionMap, map[int32]bool{
		0: true, 1: true, 2: false,
	})

	consumer.EXPECT().Close().Return(nil)
	err = doTimed(t, func() {
		err := c.Close()
		ensure.Nil(t, err)
		<-wait
	})
	ensure.Nil(t, err)

}

func doTimed(t *testing.T, do func()) error {
	ch := make(chan bool)
	go func() {
		do()
		close(ch)
	}()

	select {
	case <-time.After(1 * time.Second):
		t.Fail()
		return errors.New("function took too long to complete")
	case <-ch:
	}

	return nil
}
