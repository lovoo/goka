package confluent

import (
	"fmt"
	"log"
	"strings"

	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/kafka"
)

type topicPartitionInfo struct {
	tp      rdkafka.TopicPartition
	hwm     int64
	bofSent bool
}

type confluent struct {
	tablePartitions  map[string]map[int32]topicPartitionInfo
	streamPartitions map[int32][]rdkafka.TopicPartition
	partitionMap     map[int32]bool

	consumer    confluentConsumer
	events      chan kafka.Event
	groupTopics map[string]int64
	cmds        chan interface{}
	stop        chan bool
	done        chan bool
}

type addPartition struct {
	topic         string
	partition     int32
	initialOffset int64
}

type removePartition struct {
	topic     string
	partition int32
}

type addGroupPartition struct {
	partition int32
}

func NewConsumer(brokers []string, group string, bufsize int) (kafka.Consumer, error) {
	consumer, err := rdkafka.NewConsumer(
		&rdkafka.ConfigMap{
			"bootstrap.servers":  strings.Join(brokers, ","),
			"group.id":           group,
			"session.timeout.ms": 6000,
			// TODO(diogo): implement Commit()
			//"enable.auto.commit":              false,
			"go.events.channel.size":          bufsize,
			"go.events.channel.enable":        true,
			"go.application.rebalance.enable": true,
			"default.topic.config":            rdkafka.ConfigMap{"auto.offset.reset": "earliest"},
		},
	)
	if err != nil {
		return nil, err
	}

	c := &confluent{
		consumer:         consumer,
		tablePartitions:  make(map[string]map[int32]topicPartitionInfo),
		streamPartitions: make(map[int32][]rdkafka.TopicPartition),
		partitionMap:     make(map[int32]bool),
		events:           make(chan kafka.Event, 1024),
		cmds:             make(chan interface{}, 1024),
		stop:             make(chan bool),
		done:             make(chan bool),
	}

	// start go routine
	go c.run()
	return c, nil
}

// NewConsumerBuilder builds confluent-based consumers with channel size.
func NewConsumerBuilder(size int) goka.ConsumerBuilder {
	return func(brokers []string, group, clientID string) (kafka.Consumer, error) {
		consumer, err := NewConsumer(brokers, group, size)
		if err != nil {
			log.Fatalf("cannot create confluent consumer: %v", err)
		}
		return consumer, nil
	}
}

func (c *confluent) Events() <-chan kafka.Event {
	return c.events
}

func (c *confluent) Subscribe(topics map[string]int64) error {
	log.Println("%% confluent %%", "subscribe", topics)
	c.groupTopics = topics

	var tops []string
	for topic := range topics {
		tops = append(tops, string(topic))
	}

	err := c.consumer.SubscribeTopics(tops, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *confluent) Commit(topic string, partition int32, offset int64) error { return nil }

func (c *confluent) AddGroupPartition(partition int32) {
	select {
	case c.cmds <- &addGroupPartition{partition}:
	case <-c.stop:
	}
}

func (c *confluent) AddPartition(topic string, partition int32, initialOffset int64) {
	select {
	case c.cmds <- &addPartition{topic, partition, initialOffset}:
	case <-c.stop:
	}
}

func (c *confluent) RemovePartition(topic string, partition int32) {
	select {
	case c.cmds <- &removePartition{topic, partition}:
	case <-c.stop:
	}
}

func (c *confluent) Close() error {
	// stop go routines
	close(c.stop)
	<-c.done

	return nil
}

func (c *confluent) run() {
	defer close(c.done)
	for {
		select {
		case ev := <-c.consumer.Events():
			//	log.Println("%% confluent %%", "received event", ev)
			switch e := ev.(type) {
			case rdkafka.AssignedPartitions:
				c.events <- c.rebalance(e)

			case rdkafka.RevokedPartitions:
				c.consumer.Unassign()

			case *rdkafka.Message:
				var (
					topic     = *e.TopicPartition.Topic
					partition = e.TopicPartition.Partition
				)

				c.events <- &kafka.Message{
					Topic:     topic,
					Partition: partition,
					Offset:    int64(e.TopicPartition.Offset),
					Key:       string(e.Key),
					Value:     e.Value,
					Timestamp: e.Timestamp,
				}

			case rdkafka.PartitionEOF:
				//log.Printf("%%%% confluent %%%% Reached %v\n", e)
				c.events <- &kafka.EOF{
					Topic:     *e.Topic,
					Partition: e.Partition,
					Hwm:       int64(e.Offset),
				}

			case rdkafka.Error:
				c.events <- &kafka.Error{fmt.Errorf("error from rdkafka: %v", e)}

			default:
				//log.Printf("HANDLE ME: %v", ev)
			}

		case cmd := <-c.cmds:
			switch cmd := cmd.(type) {
			case *addPartition:
				c.addPartition(cmd.topic, cmd.partition, cmd.initialOffset)
			case *removePartition:
				c.removePartition(cmd.topic, cmd.partition)
			case *addGroupPartition:
				c.addGroupPartition(cmd.partition)
			default:
				c.events <- &kafka.Error{fmt.Errorf("invalid command: %T", cmd)}
			}

		case <-c.stop:
			log.Println("%% confluent %% terminated")
			return
		}
	}
}

func (c *confluent) addGroupPartition(partition int32) {
	log.Println("%% confluent %%", "adding group partition", partition)
	c.partitionMap[partition] = true
	c.reassign()
}

func (c *confluent) addPartition(topic string, partition int32, initialOffset int64) {
	log.Println("%% confluent %%", "adding topic partition", topic, partition, initialOffset)
	if _, has := c.tablePartitions[topic]; !has {
		c.tablePartitions[topic] = make(map[int32]topicPartitionInfo)
	}
	c.tablePartitions[topic][partition] = topicPartitionInfo{
		tp: rdkafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
			Offset:    rdkafka.Offset(initialOffset),
			Error:     nil,
		},
	}

	// send BOF
	l, h, err := c.consumer.QueryWatermarkOffsets(topic, partition, 500)
	if err != nil {
		select {
		case c.events <- &kafka.Error{fmt.Errorf("error querying watermarks: %v", err)}:
		case <-c.stop:
			return
		}
	}
	select {
	case c.events <- &kafka.BOF{
		Topic:     topic,
		Partition: partition,
		Offset:    l,
		Hwm:       h,
	}:
	case <-c.stop:
		return
	}

	c.reassign()
}

func (c *confluent) removePartition(topic string, partition int32) {
	log.Println("%% confluent %%", "remove topic partition", topic, partition)
	if _, has := c.tablePartitions[topic]; !has {
		return
	}
	if _, has := c.tablePartitions[topic][partition]; !has {
		return
	}
	delete(c.tablePartitions[topic], partition)
	if len(c.tablePartitions[topic]) == 0 {
		delete(c.tablePartitions, topic)
	}
	c.reassign()
}

func (c *confluent) reassign() {
	var tps []rdkafka.TopicPartition
	for p, tp := range c.streamPartitions {
		if c.partitionMap[p] {
			tps = append(tps, tp...)
		}
	}
	for _, m := range c.tablePartitions {
		for _, tp := range m {
			tps = append(tps, tp.tp)
		}
	}
	c.consumer.Assign(tps)
}

func (c *confluent) rebalance(e rdkafka.AssignedPartitions) *kafka.Assignment {
	var (
		as = make(kafka.Assignment)
		pm = c.partitionMap
	)

	c.partitionMap = make(map[int32]bool)
	for _, p := range e.Partitions {
		if p.Offset == -1001 {
			off := c.groupTopics[*p.Topic]
			as[p.Partition] = off
		} else {
			as[p.Partition] = int64(p.Offset)
		}

		c.streamPartitions[p.Partition] = append(c.streamPartitions[p.Partition], p)
		c.partitionMap[p.Partition] = pm[p.Partition] // keep already assigned partitions
	}
	return &as
}

//go:generate mockgen -package mock -destination ../mock/confluent.go -source=confluent.go confluentConsumer
type confluentConsumer interface {
	Assign(partitions []rdkafka.TopicPartition) (err error)
	Close() (err error)
	Commit() ([]rdkafka.TopicPartition, error)
	CommitMessage(m *rdkafka.Message) ([]rdkafka.TopicPartition, error)
	CommitOffsets(offsets []rdkafka.TopicPartition) ([]rdkafka.TopicPartition, error)
	Events() chan rdkafka.Event
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*rdkafka.Metadata, error)
	Poll(timeoutMs int) (event rdkafka.Event)
	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error)
	String() string
	Subscribe(topic string, rebalanceCb rdkafka.RebalanceCb) error
	SubscribeTopics(topics []string, rebalanceCb rdkafka.RebalanceCb) (err error)
	Unassign() (err error)
	Unsubscribe() (err error)
}
