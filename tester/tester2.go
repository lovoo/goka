package tester

/*
import (
	"fmt"
	"hash"
	"reflect"
	"sync"
	"testing"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/mock"
	"github.com/lovoo/goka/storage"

	"github.com/Shopify/sarama"
)

type queue2 struct {
	sync.Mutex
	topic    string
	messages []*message
	hwm      int64
}

func newQueue2(topic string) *queue2 {

	return &queue2{
		topic: topic,
	}
}

type client struct {
	clientID       string
	consumerGroup  *ConsumerGroup
	consumer       *SaramaConsumer
	expectConsumer bool
}

type Tester2 struct {
	t        *testing.T
	producer *mock.Producer
	tmgr     *mock.TopicManager

	clients map[string]*client

	codecs      map[string]goka.Codec
	mQueues     sync.RWMutex
	topicQueues map[string]*queue2
	storages    map[string]storage.Storage
}

func NewTester2(t *testing.T) *Tester2 {

	return &Tester2{
		t:        t,
		tmgr:     mock.NewTopicManager(1, 1),
		producer: mock.NewProducer(t),

		clients: make(map[string]*client),

		codecs:      make(map[string]goka.Codec),
		topicQueues: make(map[string]*queue2),
		storages:    make(map[string]storage.Storage),
	}
}

func (tt *Tester2) nextClient() *client {
	c := &client{
		clientID:      fmt.Sprintf("client-%d", len(tt.clients)),
		consumer:      NewSaramaConsumer(tt),
		consumerGroup: NewConsumerGroup(tt.t),
	}
	tt.clients[c.clientID] = c
	return c
}

func (tt *Tester2) ConsumerGroupBuilder() kafka.ConsumerGroupBuilder {
	return func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) {
		client, exists := tt.clients[clientID]
		if !exists {
			return nil, fmt.Errorf("cannot create consumergroup because no client registered with ID: %s", clientID)
		}

		return client.consumerGroup, nil
	}
}

func (tt *Tester2) ConsumerBuilder() kafka.SaramaConsumerBuilder {
	return func(brokers []string, clientID string) (sarama.Consumer, error) {
		client, exists := tt.clients[clientID]
		if !exists {
			return nil, fmt.Errorf("cannot create sarama consumer because no client registered with ID: %s", clientID)
		}

		return client.consumer, nil
	}
}

func (tt *Tester2) ProducerBuilder() kafka.ProducerBuilder {
	return func(b []string, cid string, hasher func() hash.Hash32) (kafka.Producer, error) {
		return tt.producer, nil
	}
}

func (tt *Tester2) TopicManagerBuilder() kafka.TopicManagerBuilder {
	return func(brokers []string) (kafka.TopicManager, error) {
		return tt.tmgr, nil
	}
}

// RegisterGroupGraph is called by a processor when the tester is passed via
// `WithTester(..)`.
// This will setup the tester with the neccessary consumer structure
func (tt *Tester2) RegisterGroupGraph(gg *goka.GroupGraph) string {

	client := tt.nextClient()
	if gg.GroupTable() != nil {
		queue := tt.getOrCreateQueue(gg.GroupTable().Topic())
		client.expectConsumerGroupForTopic(
		client.expectSaramaConsumer(queue)
		tt.registerCodec(gg.GroupTable().Topic(), gg.GroupTable().Codec())
	}

	for _, input := range gg.InputStreams() {
		client.expectGroupConsumer(input.Topic())
		tt.registerCodec(input.Topic(), input.Codec())
	}

	for _, output := range gg.OutputStreams() {
		tt.registerCodec(output.Topic(), output.Codec())
		tt.getOrCreateQueue(output.Topic())
	}
	for _, join := range gg.JointTables() {
		client.expectSimpleConsumer()
		// tt.getOrCreateQueue(join.Topic()).expectSimpleConsumer()
		tt.registerCodec(join.Topic(), join.Codec())
	}

	if loop := gg.LoopStream(); loop != nil {
		client.expectGroupConsumer(loop.Topic())
		// tt.getOrCreateQueue(loop.Topic()).expectGroupConsumer()
		tt.registerCodec(loop.Topic(), loop.Codec())
	}

	for _, lookup := range gg.LookupTables() {
		client.expectSimpleConsumer(lookup)
		// tt.getOrCreateQueue(lookup.Topic()).expectSimpleConsumer()
		tt.registerCodec(lookup.Topic(), lookup.Codec())
	}

}

func (tt *Tester2) getOrCreateQueue(topic string) *queue2 {
	tt.mQueues.RLock()
	_, exists := tt.topicQueues[topic]
	tt.mQueues.RUnlock()
	if !exists {
		tt.mQueues.Lock()
		if _, exists = tt.topicQueues[topic]; !exists {
			tt.topicQueues[topic] = newQueue2(topic)
		}
		tt.mQueues.Unlock()
	}

	tt.mQueues.RLock()
	defer tt.mQueues.RUnlock()
	return tt.topicQueues[topic]
}

func (tt *Tester2) codecForTopic(topic string) goka.Codec {
	codec, exists := tt.codecs[topic]
	if !exists {
		panic(fmt.Errorf("No codec for topic %s registered.", topic))
	}
	return codec
}

func (tt *Tester2) registerCodec(topic string, codec goka.Codec) {
	if existingCodec, exists := tt.codecs[topic]; exists {
		if reflect.TypeOf(codec) != reflect.TypeOf(existingCodec) {
			panic(fmt.Errorf("There are different codecs for the same topic. This is messed up (%#v, %#v)", codec, existingCodec))
		}
	}
	tt.codecs[topic] = codec
}
*/
