# Distributed tracing with Goka

Goka does not bundle a tracing SDK. Applications instrument Kafka I/O with libraries such as [Datadog's IBM/sarama integration](https://github.com/DataDog/dd-trace-go/tree/main/contrib/IBM/sarama) or [otelsarama](https://github.com/dnwe/otelsarama), and use goka's extension points to attach those wrappers and to continue traces across message handling and emits.

## Prerequisites

Sarama tracing relies on Kafka message headers. Configure a broker version that supports them:

```go
cfg := goka.DefaultConfig()
cfg.Version = sarama.V0_11_0_0 // or newer
```

## Extension points

| API | Role |
|-----|------|
| `WithConsumerGroupHandlerWrapper` | Wraps the processor's `sarama.ConsumerGroupHandler` before `ConsumerGroup.Consume`. Pass the result of `WrapConsumerGroupHandler` (or equivalent). |
| `WithProducerBuilder` + `ProducerBuilderWithConfig` | Builds a `sarama.AsyncProducer` and optionally wraps it (e.g. `WithAsyncProducerWrapper`), then hands it to goka. |
| `NewProducerFromAsyncProducer` | Same idea for custom `ProducerBuilder` implementations: wrap first, then construct goka's `Producer`. |
| `WithProcessMiddleware` | Wraps each `ProcessCallback` (input topics and visits). Use this to start and finish a span around handler logic. |
| `TextMapHeaders` | Adapts `goka.Headers` for OpenTelemetry (`Get`/`Set`/`Keys`) and Datadog (`Set`/`ForeachKey`) text-map carriers. |
| `WithConsumerSaramaBuilder` / `WithViewConsumerSaramaBuilder` | Can wrap plain `sarama.Consumer` instances used for tables and views (e.g. `WrapConsumer`). |

Incoming message headers are available on `Context.Headers()`. Outgoing headers can be set per emit with `WithCtxEmitHeaders`, or as processor defaults with `WithProducerDefaultHeaders`.

## How messages flow (and what that means for spans)

A processor does not run your callback on the Sarama claim goroutine. Roughly:

1. The consumer group delivers a message on the claim's `Messages()` channel.
2. Goka copies it onto an internal per-partition queue (`part.input`).
3. Another goroutine later runs `ProcessCallback`, which may `Emit`, `Loopback`, `SetValue`, or `Delete`.

Sarama instrumentation that wraps `ConsumerGroupHandler` typically creates a span when the message is read from the claim and finishes it based on claim delivery (for example, when the next message is handed off). That span therefore reflects **claim handoff**, not **callback execution**.

To time the work your callback actually does—and to have an active span while emitting—start a span in `WithProcessMiddleware` that lasts for the duration of `next(...)`.

Wrapping the plain table/view consumer is optional. Those clients are used heavily during recovery and catchup; wrapping them can create a large volume of receive spans that are unrelated to request handling.

## Propagating context into emits

`WrapAsyncProducer` (and equivalents) create produce spans from the trace context in the **outgoing** Kafka headers. Goka does not automatically copy the active process span into those headers.

The usual approach is to return a custom `Context` from process middleware that:

1. Exposes the process span via `Context()` (so application code and further instrumentation can find it).
2. Overrides `Emit`, `Loopback`, `SetValue`, and/or `Delete` to inject the current span into headers (via `WithCtxEmitHeaders`) before delegating.

You choose which operations to instrument. For example, you may inject on `Emit` and `Loopback` but skip `SetValue` if table-write produce spans are too noisy. If you override a method, you can also start a child span named for that operation (`goka.emit`, `goka.setvalue`, …). That is useful for distinguishing `SetValue` from `Delete`, which both write to the group table topic and otherwise look identical in producer-only instrumentation.

Implement every method you care about; any produce path you leave on the embedded `Context` will not get your injection or child spans.

## Example: Datadog

```go
package tracing

import (
	"context"

	saramatrace "github.com/DataDog/dd-trace-go/contrib/IBM/sarama/v2"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
)

// Apply returns processor options that wire Datadog's Sarama instrumentation
// into a goka processor.
func Apply(group goka.Group, cfg *sarama.Config) []goka.ProcessorOption {
	return []goka.ProcessorOption{
		goka.WithConsumerGroupHandlerWrapper(func(h sarama.ConsumerGroupHandler) sarama.ConsumerGroupHandler {
			return saramatrace.WrapConsumerGroupHandler(h, saramatrace.WithGroupID(string(group)))
		}),
		goka.WithProcessMiddleware(func(next goka.ProcessCallback) goka.ProcessCallback {
			return func(ctx goka.Context, msg interface{}) {
				opts := []tracer.StartSpanOption{
					tracer.ResourceName(string(ctx.Topic())),
					tracer.SpanType(ext.SpanTypeMessageConsumer),
					tracer.Tag(ext.MessagingSystem, ext.MessagingSystemKafka),
					tracer.Tag(ext.MessagingKafkaPartition, ctx.Partition()),
					tracer.Tag("offset", ctx.Offset()),
					tracer.Measured(),
				}
				if parent, err := tracer.Extract(goka.TextMapHeaders(ctx.Headers())); err == nil {
					opts = append(opts, tracer.ChildOf(parent))
				}
				span := tracer.StartSpan("goka.process", opts...)
				defer span.Finish()
				next(&tracingContext{Context: ctx, span: span}, msg)
			}
		}),
		goka.WithProducerBuilder(goka.ProducerBuilderWithConfig(cfg,
			goka.WithAsyncProducerWrapper(saramatrace.WrapAsyncProducer),
		)),
	}
}

type tracingContext struct {
	goka.Context
	span *tracer.Span
}

func (c *tracingContext) Context() context.Context {
	return tracer.ContextWithSpan(c.Context.Context(), c.span)
}

func (c *tracingContext) Emit(topic goka.Stream, key string, value interface{}, options ...goka.ContextOption) {
	c.withProduceSpan("goka.emit", string(topic), func(opts []goka.ContextOption) {
		c.Context.Emit(topic, key, value, opts...)
	}, options...)
}

func (c *tracingContext) Loopback(key string, value interface{}, options ...goka.ContextOption) {
	c.withProduceSpan("goka.loopback", "", func(opts []goka.ContextOption) {
		c.Context.Loopback(key, value, opts...)
	}, options...)
}

func (c *tracingContext) SetValue(value interface{}, options ...goka.ContextOption) {
	c.withProduceSpan("goka.setvalue", "", func(opts []goka.ContextOption) {
		c.Context.SetValue(value, opts...)
	}, options...)
}

func (c *tracingContext) Delete(options ...goka.ContextOption) {
	c.withProduceSpan("goka.delete", "", func(opts []goka.ContextOption) {
		c.Context.Delete(opts...)
	}, options...)
}

func (c *tracingContext) withProduceSpan(
	op, topic string,
	call func(opts []goka.ContextOption),
	options ...goka.ContextOption,
) {
	opts := []tracer.StartSpanOption{
		tracer.ResourceName(op),
		tracer.SpanType(ext.SpanTypeMessageProducer),
		tracer.Tag(ext.MessagingSystem, ext.MessagingSystemKafka),
		tracer.Tag("goka.op", op),
		tracer.ChildOf(c.span.Context()),
	}
	if topic != "" {
		opts = append(opts, tracer.Tag(ext.MessagingDestinationName, topic))
	}
	span := tracer.StartSpan(op, opts...)
	defer span.Finish()

	h := goka.Headers{}
	_ = tracer.Inject(span.Context(), goka.TextMapHeaders(h))
	call(append(options, goka.WithCtxEmitHeaders(h)))
}
```

```go
cfg := goka.DefaultConfig()
cfg.Version = sarama.V0_11_0_0

proc, err := goka.NewProcessor(brokers, graph, tracing.Apply(group, cfg)...)
```

Example trace for a callback that updates state and emits downstream:

```text
kafka.consume                          // from WrapConsumerGroupHandler (claim-scoped)
  └─ goka.process                      // from ProcessMiddleware (callback-scoped)
       ├─ goka.setvalue
       │    └─ Produce Topic <group>-table
       └─ goka.emit
            └─ Produce Topic <output>
```

`WrapConsumerGroupHandler` is optional if you only want callback-scoped and produce spans. Keep it when you want Datadog Data Streams consumer-group tagging or claim-level spans as well.

## OpenTelemetry

Use the same goka options with otelsarama's `WrapConsumerGroupHandler` and `WrapAsyncProducer`, and propagate with `otel.GetTextMapPropagator().Extract` / `Inject` against `goka.TextMapHeaders`. Start the callback span in `WithProcessMiddleware` the same way as in the Datadog example; otelsarama's consumer-group wrapper finishes its receive span at claim handoff, so middleware is still where handler duration should be measured.
