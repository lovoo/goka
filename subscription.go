package goka

import (
	"errors"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

// Subscription represents a subscribed topic in Kafka
type Subscription struct {
	Name          string
	initialOffset int64
	consume       ConsumeCallback
	codec         Codec
	internal      bool
	table         bool
}

func (s *Subscription) null() bool {
	return s.Name == ""
}

func (s *Subscription) is(topic string) bool {
	return topic == s.Name
}

// addPrefix adds a prefix to the topic name
func (s *Subscription) addPrefix(prefix string) *Subscription {
	s.Name = prefix + "-" + s.Name
	return s
}

func (s *Subscription) validate() error {
	if s.consume == nil {
		return fmt.Errorf("cannot use nil consume callback for topic %s", s.Name)
	}
	return nil
}

// Subscriptions is a group of subscriptions to be consumed by a processor.
type Subscriptions []Subscription

func (subs Subscriptions) strings() []string {
	var r []string
	for _, s := range subs {
		r = append(r, s.Name)
	}
	return r
}

func (subs Subscriptions) validate() error {
	// check at least one topic
	if len(subs) == 0 {
		return errors.New("processor: no topic to process")
	}

	ok := false
	for _, s := range subs {
		if err := s.validate(); err != nil {
			return err
		}

		if !s.table { // check if at least one stream
			ok = true
			break
		}
	}
	if !ok {
		return errors.New("no stream subscribed")
	}

	return nil
}

// Subscribe combines several lists of subscriptions in a single list.
func Subscribe(subs ...Subscriptions) Subscriptions {
	var r Subscriptions
	for _, s := range subs {
		r = append(r, s...)
	}
	return r
}

// Table is one or more co-partitioned, log-compacted topic. The processor
// subscribing for a table topic will start reading from the oldest offset
// of the partition.
func Table(topic string, codec Codec) Subscriptions {
	return []Subscription{
		{
			Name:          topic,
			initialOffset: sarama.OffsetOldest,
			codec:         codec,
			table:         true,
		}}
}

// Stream returns a subscription for a co-partitioned topic. The processor
// subscribing for a stream topic will start reading from the newest offset of
// the partition.
func Stream(topic string, codec Codec, consume ConsumeCallback) Subscriptions {
	return []Subscription{
		{
			Name:          topic,
			initialOffset: sarama.OffsetNewest,
			codec:         codec,
			consume:       consume,
		}}
}

// Loop defines a consume callback on the loop topic
func Loop(codec Codec, consume ConsumeCallback) Subscriptions {
	if consume == nil {
		log.Fatalln("cannot use nil consume callback")
	}
	return []Subscription{
		{
			Name:          "loop",
			initialOffset: sarama.OffsetNewest,
			internal:      true,
			codec:         codec,
			consume:       consume,
		}}
}

// Streams subscribes to multiple topics using one codec and one consume callback
func Streams(topics []string, codec Codec, consume ConsumeCallback) Subscriptions {
	var subs Subscriptions
	for _, t := range topics {
		subs = append(subs, Subscription{
			Name:          t,
			initialOffset: sarama.OffsetNewest,
			codec:         codec,
			consume:       consume,
		})
	}
	return subs
}

// tableName returns the name of the group table topic
func tableName(group string) string {
	return group + "-state"
}

// GroupTableTopic returns the name of the table topic of a group.
func GroupTableTopic(group string) string {
	return tableName(group)
}
