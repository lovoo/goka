package goka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/facebookgo/ensure"
)

func foo(ctx Context, msg interface{}) {}

func TestStream(t *testing.T) {
	strs := []string{"topic1", "topic2"}
	topics := Streams(strs, rawCodec, foo)
	ensure.True(t, len(topics) == len(strs))
	for i, topic := range topics {
		ensure.DeepEqual(t, topic.Name, strs[i])
		ensure.DeepEqual(t, topic.initialOffset, sarama.OffsetNewest)
	}
}

func TestTopics(t *testing.T) {
	strs := []string{"topic1", "topic2"}
	subs := Subscribe(
		Streams(strs, rawCodec, foo),
		Stream(strs[0], rawCodec, foo),
	)
	ensure.True(t, len(subs) == 1+len(strs))
	for i, s := range subs {
		ensure.DeepEqual(t, s.Name, strs[i%len(strs)])
	}
}
