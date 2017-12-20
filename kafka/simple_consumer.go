package kafka

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
)

type topicPartition struct {
	topic     string
	partition int32
}

type simpleConsumer struct {
	client     sarama.Client
	consumer   sarama.Consumer
	partitions map[topicPartition]sarama.PartitionConsumer
	m          sync.Mutex

	events chan Event
	dying  chan bool

	wg sync.WaitGroup
}

func newSimpleConsumer(brokers []string, events chan Event, config *sarama.Config) (*simpleConsumer, error) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("Cannot connect to kafka: %v", err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("Cannot create consumer: %v", err)
	}

	return &simpleConsumer{
		client:     client,
		consumer:   consumer,
		events:     events,
		dying:      make(chan bool),
		partitions: make(map[topicPartition]sarama.PartitionConsumer),
	}, nil
}

func (c *simpleConsumer) Close() error {
	// stop any blocking writes to channels
	close(c.dying)

	c.m.Lock()
	defer c.m.Unlock()
	for tp, pc := range c.partitions {
		pc.AsyncClose()
		delete(c.partitions, tp)
	}

	// wait until all partition consumers have finished
	c.wg.Wait()

	if err := c.consumer.Close(); err != nil {
		return fmt.Errorf("Failed to close consumer: %v", err)
	}

	if err := c.client.Close(); err != nil {
		return fmt.Errorf("Failed to close client in consumer: %v", err)
	}
	return nil
}

func (c *simpleConsumer) AddPartition(topic string, partition int32, offset int64) error {
	c.m.Lock()
	defer c.m.Unlock()
	tp := topicPartition{topic, partition}
	if _, has := c.partitions[tp]; has {
		return fmt.Errorf("%s/%d already added", topic, partition)
	}

	// find best offset
	start, hwm, err := c.getOffsets(topic, partition, offset)
	if err != nil {
		return fmt.Errorf("error getting offsets %s/%d: %v", topic, partition, err)
	}

	pc, err := c.consumer.ConsumePartition(topic, partition, start)
	if err != nil {
		return fmt.Errorf("error creating consumer for %s/%d: %v", topic, partition, err)
	}
	c.partitions[tp] = pc

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() {
			if err := recover(); err != nil {
				c.events <- &Error{
					Err: fmt.Errorf("panic: %v", err),
				}
			}
		}()
		c.run(pc, topic, partition, start, hwm)
	}()
	return nil
}

func (c *simpleConsumer) run(pc sarama.PartitionConsumer, topic string, partition int32, start, hwm int64) {
	// mark beginning of partition consumption
	select {
	case c.events <- &BOF{
		Topic:     topic,
		Partition: partition,
		Offset:    start,
		Hwm:       hwm,
	}:
	case <-c.dying:
		return
	}

	// generate EOF if nothing to consume
	if start == hwm {
		select {
		case c.events <- &EOF{
			Topic:     topic,
			Partition: partition,
			Hwm:       hwm,
		}:
		case <-c.dying:
			return
		}
	}

	count := 0
	// wait for messages to arrive
	for {
		select {
		case m, ok := <-pc.Messages():
			if !ok {
				// Partition was removed. Continue to loop until errors are also
				// drained.
				continue
			}
			select {
			case c.events <- &Message{
				Topic:     m.Topic,
				Partition: m.Partition,
				Offset:    m.Offset,
				Key:       string(m.Key),
				Value:     m.Value,
				Timestamp: m.Timestamp,
			}:
			case <-c.dying:
				return
			}

			if m.Offset == pc.HighWaterMarkOffset()-1 {
				select {
				case c.events <- &EOF{
					Topic:     m.Topic,
					Partition: m.Partition,
					Hwm:       m.Offset + 1,
				}:
				case <-c.dying:
					return
				}
			}

			count++
			if count%1000 == 0 && m.Offset >= hwm { // was this EOF?
				select {
				case c.events <- &EOF{
					Topic:     m.Topic,
					Partition: m.Partition,
					Hwm:       pc.HighWaterMarkOffset(),
				}:
				case <-c.dying:
					return
				}
			}
		case err, ok := <-pc.Errors():
			if !ok {
				// Partition was removed.
				return
			}
			select {
			case c.events <- &Error{
				Err: err,
			}:
			case <-c.dying:
				return
			}
			return
		case <-c.dying:
			// Only closed when simple_consumer was closed, not when partitions are removed.
			return
		}
	}
}

func (c *simpleConsumer) RemovePartition(topic string, partition int32) error {
	tp := topicPartition{topic, partition}
	c.m.Lock()
	defer c.m.Unlock()
	pc, has := c.partitions[tp]
	if !has {
		return fmt.Errorf("%s/%d was not added", topic, partition)
	}
	delete(c.partitions, tp)

	if err := pc.Close(); err != nil {
		return fmt.Errorf("error closing consumer for %s/%d: %v", topic, partition, err)
	}

	return nil
}

func (c *simpleConsumer) getOffsets(topic string, partition int32, offset int64) (start, hwm int64, err error) {
	// check if there is anything to consume in topic/partition
	oldest, err := c.client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		err = fmt.Errorf("Error reading oldest log offset from kafka: %v", err)
		return
	}

	// get HighWaterMark
	hwm, err = c.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		err = fmt.Errorf("Error reading current log offset from kafka: %v", err)
		return
	}

	start = offset

	if offset == sarama.OffsetOldest {
		start = oldest
	} else if offset == sarama.OffsetNewest {
		start = hwm
	}

	if start > hwm {
		start = hwm
	}
	if start < oldest {
		start = oldest
	}
	return
}
