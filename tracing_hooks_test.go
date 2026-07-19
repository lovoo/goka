package goka

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

func TestWithProcessMiddleware_CompositionOrder(t *testing.T) {
	var order []string
	opts := &poptions{}
	gg := new(GroupGraph)

	WithProcessMiddleware(func(next ProcessCallback) ProcessCallback {
		return func(ctx Context, msg interface{}) {
			order = append(order, "outer-before")
			next(ctx, msg)
			order = append(order, "outer-after")
		}
	})(opts, gg)

	WithProcessMiddleware(func(next ProcessCallback) ProcessCallback {
		return func(ctx Context, msg interface{}) {
			order = append(order, "inner-before")
			next(ctx, msg)
			order = append(order, "inner-after")
		}
	})(opts, gg)

	pp := &PartitionProcessor{opts: opts}
	cb := pp.applyProcessMiddlewares(func(ctx Context, msg interface{}) {
		order = append(order, "handler")
	})
	cb(nil, nil)

	require.Equal(t, []string{
		"outer-before",
		"inner-before",
		"handler",
		"inner-after",
		"outer-after",
	}, order)
}

func TestWithConsumerGroupHandlerWrapper(t *testing.T) {
	opts := &poptions{}
	gg := new(GroupGraph)
	var called bool

	WithConsumerGroupHandlerWrapper(func(h sarama.ConsumerGroupHandler) sarama.ConsumerGroupHandler {
		called = true
		require.NotNil(t, h)
		return h
	})(opts, gg)

	g := &Processor{opts: opts}
	handler := g.consumerGroupHandler()
	require.True(t, called)
	require.Equal(t, g, handler)
}

func TestWithConsumerGroupHandlerWrapper_Nil(t *testing.T) {
	g := &Processor{opts: &poptions{}}
	require.Equal(t, g, g.consumerGroupHandler())
}
