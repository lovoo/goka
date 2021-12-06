# Goka 
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause) [![Unit Tests/System Tests](https://github.com/lovoo/goka/actions/workflows/main.yml/badge.svg)](https://github.com/lovoo/goka/actions/workflows/main.yml)
 [![GoDoc](https://godoc.org/github.com/lovoo/goka?status.svg)](https://godoc.org/github.com/lovoo/goka) [![Go Report Card](https://goreportcard.com/badge/github.com/lovoo/goka)](https://goreportcard.com/report/github.com/lovoo/goka)

Goka is a compact yet powerful distributed stream processing library for [Apache Kafka] written in Go. Goka aims to reduce the complexity of building highly scalable and highly available microservices.

Goka extends the concept of Kafka consumer groups by binding a state table to them and persisting them in Kafka. Goka provides sane defaults and a pluggable architecture.

## Features
  * **Message Input and Output**

    Goka handles all the message input and output for you. You only have to provide one or more callback functions that handle messages from any of the Kafka topics you are interested in. You only ever have to deal with deserialized messages.

  * **Scaling**

    Goka automatically distributes the processing and state across multiple instances of a service. This enables effortless scaling when the load increases.

  * **Fault Tolerance**

    In case of a failure, Goka will redistribute the failed instance's workload and state across the remaining healthy instances. All state is safely stored in Kafka and messages delivered with *at-least-once* semantics.

  * **Built-in Monitoring and Introspection**

    Goka provides a web interface for monitoring performance and querying values in the state.

  * **Modularity**

    Goka fosters a pluggable architecture which enables you to replace for example the storage layer or the Kafka communication layer.

## Documentation

This README provides a brief, high level overview of the ideas behind Goka.
A more detailed introduction of the project can be found in this [blog post](https://lovoodotblog.wordpress.com/2017/05/23/goka/).

Package API documentation is available at [GoDoc] and the [Wiki](https://github.com/lovoo/goka/wiki/Tips#configuring-log-compaction-for-table-topics) provides several tips for configuring, extending, and deploying Goka applications.

## Installation

You can install Goka by running the following command:

``$ go get -u github.com/lovoo/goka``

## Configuration

Goka relies on [Sarama](https://github.com/Shopify/sarama) to perform the actual communication with Kafka, which offers many configuration settings. The config is documented [here](https://godoc.org/github.com/Shopify/sarama#Config).

In most cases, you need to modify the config, e.g. to set the Kafka Version.

```go
cfg := goka.DefaultConfig()
cfg.Version = sarama.V2_4_0_0
goka.ReplaceGlobalConfig(cfg)
```

This makes all goka components use the updated config.

If you do need specific configuration for different components, you need to pass customized builders to the 
component's constructor, e.g.

```go
cfg := goka.DefaultConfig()
// modify the config with component-specific settings


// use the config by creating a builder which allows to override global config
goka.NewProcessor(// ...,
	goka.WithConsumerGroupBuilder(
		goka.ConsumerGroupBuilderWithConfig(cfg),
	),
	// ...
)
```

## Concepts

Goka relies on Kafka for message passing, fault-tolerant state storage and workload partitioning.

* **Emitters** deliver key-value messages into Kafka. As an example, an emitter could be a database handler emitting the state changes into Kafka for other interested applications to consume.

* **Processor** is a set of callback functions that consume and perform state transformations upon delivery of these emitted messages. *Processor groups* are formed of one or more instances of a processor. Goka distributes the partitions of the input topics across all processor instances in a processor group. This enables effortless scaling and fault-tolerance. If a processor instance fails, its partitions and state are reassigned to the remaining healthy members of the processor group. Processors can also emit further messages into Kafka.

* **Group table** is the state of a processor group. It is a partitioned key-value table stored in Kafka that belongs to a single processor group. If a processor instance fails, the remaining instances will take over the group table partitions of the failed instance recovering them from Kafka.

* **Views** are local caches of a complete group table. Views provide read-only access to the group tables and can be used to provide external services for example through a gRPC interface.

* **Local storage** keeps a local copy of the group table partitions to speedup recovery and reduce memory utilization. By default, the local storage uses [LevelDB](https://github.com/syndtr/goleveldb), but in-memory map and [Redis-based storage](https://github.com/lovoo/goka/tree/master/storage/redis) are also available.


## Get Started

An example Goka application could look like the following.
An emitter emits a single message with key "some-key" and value "some-value" into the "example-stream" topic.
A processor processes the "example-stream" topic counting the number of messages delivered for "some-key".
The counter is persisted in the "example-group-table" topic.
To locally start a dockerized Zookeeper and Kafka instances, execute `make start` with the `Makefile` in the [examples] folder.

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "example-stream"
	group   goka.Group  = "example-group"
)

// Emit messages forever every second
func runEmitter() {
	emitter, err := goka.NewEmitter(brokers, topic, new(codec.String))
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer emitter.Finish()
	for {
		time.Sleep(1 * time.Second)
		err = emitter.EmitSync("some-key", "some-value")
		if err != nil {
			log.Fatalf("error emitting message: %v", err)
		}
	}
}

// process messages until ctrl-c is pressed
func runProcessor() {
	// process callback is invoked for each message delivered from
	// "example-stream" topic.
	cb := func(ctx goka.Context, msg interface{}) {
		var counter int64
		// ctx.Value() gets from the group table the value that is stored for
		// the message's key.
		if val := ctx.Value(); val != nil {
			counter = val.(int64)
		}
		counter++
		// SetValue stores the incremented counter in the group table for in
		// the message's key.
		ctx.SetValue(counter)
		log.Printf("key = %s, counter = %v, msg = %v", ctx.Key(), counter, msg)
	}

	// Define a new processor group. The group defines all inputs, outputs, and
	// serialization formats. The group-table topic is "example-group-table".
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), cb),
		goka.Persist(new(codec.Int64)),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			log.Fatalf("error running processor: %v", err)
		} else {
			log.Printf("Processor shutdown cleanly")
		}
	}()

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
	<-wait   // wait for SIGINT/SIGTERM
	cancel() // gracefully stop processor
	<-done
}

func main() {
	go runEmitter() // emits one message and stops
	runProcessor()  // press ctrl-c to stop
}

```
A very similar example is also in *1-simplest*. Just run `go run examples/1-simplest/main.go`.

Note that tables have to be configured in Kafka with log compaction.
For details check the [Wiki](https://github.com/lovoo/goka/wiki/Tips#configuring-log-compaction-for-table-topics).

## How to contribute

Contributions are always welcome.
Please fork the repo, create a pull request against master, and be sure tests pass.
See the [GitHub Flow] for details.

[Apache Kafka]: https://kafka.apache.org/
[GoDoc]: https://godoc.org/github.com/lovoo/goka
[examples]: https://github.com/lovoo/goka/tree/master/examples
[GitHub Flow]: https://guides.github.com/introduction/flow
