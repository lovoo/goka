//go:generate mockgen -package mock -destination=mock/sarama.go github.com/Shopify/sarama Client,Consumer,PartitionConsumer

package kafka

import (
	"errors"
	"testing"

	"github.com/lovoo/goka/kafka/mock"

	"github.com/Shopify/sarama"
	"github.com/facebookgo/ensure"
	"github.com/golang/mock/gomock"
)

func TestSimpleConsumer_GetOffsets(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		client          = mock.NewMockClient(ctrl)
		topic           = "topic"
		partition int32 = 123
		offset    int64 = 1234
		oldest    int64 = 1233
		newest    int64 = 1237
		start     int64
		hwm       int64
		err       error
	)

	c := &simpleConsumer{
		client: client,
	}

	// errors in GetOffset
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, errors.New("some error"))
	_, _, err = c.getOffsets(topic, partition, offset)
	ensure.NotNil(t, err)

	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, nil)
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetNewest).Return(newest, errors.New("some error"))
	_, _, err = c.getOffsets(topic, partition, offset)
	ensure.NotNil(t, err)

	// oldest < offset < newest
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, nil)
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetNewest).Return(newest, nil)
	start, hwm, err = c.getOffsets(topic, partition, offset)
	ensure.Nil(t, err)
	ensure.True(t, start == offset)
	ensure.True(t, hwm == newest)

	// offset < oldest < newest
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, nil)
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetNewest).Return(newest, nil)
	start, hwm, err = c.getOffsets(topic, partition, oldest-1)
	ensure.Nil(t, err)
	ensure.True(t, start == oldest)
	ensure.True(t, hwm == newest)

	// oldest < newest < offset
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, nil)
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetNewest).Return(newest, nil)
	start, hwm, err = c.getOffsets(topic, partition, newest+1)
	ensure.Nil(t, err)
	ensure.True(t, start == newest)
	ensure.True(t, hwm == newest)

	// sarama.OffsetOldest
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, nil)
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetNewest).Return(newest, nil)
	start, hwm, err = c.getOffsets(topic, partition, sarama.OffsetOldest)
	ensure.Nil(t, err)
	ensure.True(t, start == oldest)
	ensure.True(t, hwm == newest)

	// sarama.OffsetNewest
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, nil)
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetNewest).Return(newest, nil)
	start, hwm, err = c.getOffsets(topic, partition, sarama.OffsetNewest)
	ensure.Nil(t, err)
	ensure.True(t, start == newest)
	ensure.True(t, hwm == newest)
}

func TestSimpleConsumer_AddPartition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		client          = mock.NewMockClient(ctrl)
		consumer        = mock.NewMockConsumer(ctrl)
		pc              = mock.NewMockPartitionConsumer(ctrl)
		topic           = "topic"
		partition int32 = 123
		offset    int64 = 1234
		oldest    int64 = 1233
		hwm       int64 = 1237
		messages        = make(chan *sarama.ConsumerMessage)
		ch              = make(chan Event)
		cherr           = make(chan *sarama.ConsumerError)
	)

	c := &simpleConsumer{
		client:     client,
		consumer:   consumer,
		partitions: make(map[topicPartition]sarama.PartitionConsumer),
		events:     ch,
	}

	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, errors.New("some error"))
	err := c.AddPartition(topic, partition, offset)
	ensure.NotNil(t, err)

	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, nil)
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetNewest).Return(hwm, nil)
	consumer.EXPECT().ConsumePartition(topic, partition, offset).Return(nil, errors.New("some error"))
	err = c.AddPartition(topic, partition, offset)
	ensure.NotNil(t, err)

	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, nil)
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetNewest).Return(hwm, nil)
	consumer.EXPECT().ConsumePartition(topic, partition, offset).Return(pc, nil)
	pc.EXPECT().Messages().Return(messages).AnyTimes()
	pc.EXPECT().Errors().Return(cherr).AnyTimes()
	err = c.AddPartition(topic, partition, offset)
	ensure.Nil(t, err)
	m, ok := (<-ch).(*BOF)
	ensure.True(t, ok)
	ensure.DeepEqual(t, m, &BOF{Topic: topic, Partition: partition, Offset: offset, Hwm: hwm})

	err = c.AddPartition(topic, partition, offset)
	ensure.NotNil(t, err)

	// TODO(jb): uncomment that again

	// doTimed(t, func() {
	// 	close(messages)
	// 	pc.EXPECT().AsyncClose()
	// 	consumer.EXPECT().Close().Return(errors.New("some error"))
	// 	c.dying = make(chan bool)
	// 	err = c.Close()
	// 	ensure.NotNil(t, err)

	// 	consumer.EXPECT().Close().Return(nil)
	// 	client.EXPECT().Close().Return(errors.New("some error"))
	// 	c.dying = make(chan bool)
	// 	err = c.Close()
	// 	ensure.NotNil(t, err)

	// 	consumer.EXPECT().Close().Return(nil)
	// 	client.EXPECT().Close().Return(nil)
	// 	c.dying = make(chan bool)
	// 	err = c.Close()
	// 	ensure.Nil(t, err)
	// })
}

func TestSimpleConsumer_RemovePartition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		client          = mock.NewMockClient(ctrl)
		consumer        = mock.NewMockConsumer(ctrl)
		pc              = mock.NewMockPartitionConsumer(ctrl)
		topic           = "topic"
		partition int32 = 123
		offset    int64 = 1234
		oldest    int64 = 1233
		hwm       int64 = 1237
		messages        = make(chan *sarama.ConsumerMessage)
		ch              = make(chan Event)
		cherr           = make(chan *sarama.ConsumerError)
	)

	c := &simpleConsumer{
		client:     client,
		consumer:   consumer,
		partitions: make(map[topicPartition]sarama.PartitionConsumer),
		events:     ch,
	}

	err := c.RemovePartition(topic, partition)
	ensure.NotNil(t, err)

	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, nil)
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetNewest).Return(hwm, nil)
	consumer.EXPECT().ConsumePartition(topic, partition, offset).Return(pc, nil)
	pc.EXPECT().Messages().Return(messages).AnyTimes()
	pc.EXPECT().Errors().Return(cherr).AnyTimes()
	err = c.AddPartition(topic, partition, offset)
	ensure.Nil(t, err)

	m, ok := (<-ch).(*BOF)
	ensure.True(t, ok)
	ensure.DeepEqual(t, m, &BOF{Topic: topic, Partition: partition, Offset: offset, Hwm: hwm})

	ensure.DeepEqual(t, c.partitions, map[topicPartition]sarama.PartitionConsumer{
		topicPartition{topic, partition}: pc,
	})

	// add partition again
	err = c.AddPartition(topic, partition, offset)
	ensure.NotNil(t, err)

	pc.EXPECT().Close()
	err = c.RemovePartition(topic, partition)
	ensure.Nil(t, err)

	// TODO(jb): uncomment that again
	// doTimed(t, func() {
	// 	close(messages)
	// 	consumer.EXPECT().Close().Return(nil)
	// 	client.EXPECT().Close().Return(nil)
	// 	c.dying = make(chan bool)
	// 	err = c.Close()
	// 	ensure.Nil(t, err)
	// })
}

func TestSimpleConsumer_ErrorBlocked(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		client          = mock.NewMockClient(ctrl)
		consumer        = mock.NewMockConsumer(ctrl)
		pc              = mock.NewMockPartitionConsumer(ctrl)
		topic           = "topic"
		partition int32 = 123
		offset    int64 = 1234
		oldest    int64 = 1233
		hwm       int64 = 1237
		messages        = make(chan *sarama.ConsumerMessage)
		ch              = make(chan Event)
		cherr           = make(chan *sarama.ConsumerError)
	)

	c := &simpleConsumer{
		client:     client,
		consumer:   consumer,
		partitions: make(map[topicPartition]sarama.PartitionConsumer),
		events:     ch,
		dying:      make(chan bool),
	}

	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, errors.New("some error"))
	err := c.AddPartition(topic, partition, offset)
	ensure.NotNil(t, err)

	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, nil)
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetNewest).Return(hwm, nil)
	consumer.EXPECT().ConsumePartition(topic, partition, offset).Return(nil, errors.New("some error"))
	err = c.AddPartition(topic, partition, offset)
	ensure.NotNil(t, err)

	client.EXPECT().GetOffset(topic, partition, sarama.OffsetOldest).Return(oldest, nil)
	client.EXPECT().GetOffset(topic, partition, sarama.OffsetNewest).Return(hwm, nil)
	consumer.EXPECT().ConsumePartition(topic, partition, offset).Return(pc, nil)
	pc.EXPECT().Messages().Return(messages).AnyTimes()
	pc.EXPECT().Errors().Return(cherr).AnyTimes()
	err = c.AddPartition(topic, partition, offset)
	ensure.Nil(t, err)
	m, ok := (<-ch).(*BOF)
	ensure.True(t, ok)
	ensure.DeepEqual(t, m, &BOF{Topic: topic, Partition: partition, Offset: offset, Hwm: hwm})

	err = c.AddPartition(topic, partition, offset)
	ensure.NotNil(t, err)

	messages <- &sarama.ConsumerMessage{
		Key:       []byte("somekey"),
		Value:     []byte("somevalue"),
		Topic:     "sometopic",
		Partition: 123,
	}
	pc.EXPECT().HighWaterMarkOffset().Return(int64(0))
	mo, ok := (<-ch).(*Message)
	ensure.True(t, ok)
	ensure.DeepEqual(t, mo, &Message{Topic: "sometopic", Partition: 123, Key: "somekey", Value: []byte("somevalue")})

	// we now write, but don't read events
	messages <- &sarama.ConsumerMessage{
		Key:       []byte("somekey"),
		Value:     []byte("somevalue"),
		Topic:     "sometopic",
		Partition: 123,
	}

	// TODO(jb): uncomment that again
	// err = doTimed(t, func() {
	// 	pc.EXPECT().AsyncClose()
	// 	consumer.EXPECT().Close().Return(nil)
	// 	client.EXPECT().Close().Return(nil)
	// 	err := c.Close()
	// 	ensure.Nil(t, err)
	// })
	// ensure.Nil(t, err)
}
