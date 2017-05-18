# Goka [![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause) [![Build Status](http://drone.lovoo.io/api/badges/lovoo/goka/status.svg)](http://drone.lovoo.io/lovoo/goka) [![GoDoc](https://godoc.org/github.com/lovoo/goka?status.svg)](https://godoc.org/github.com/lovoo/goka)

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

Package API documentation is available at [GoDoc].

## Installation

You can install Goka by running the following command:

``$ go get -u github.com/lovoo/goka``

## Concepts

Goka relies on Kafka for message passing, fault-tolerant state storage and workload partitioning.

* **Emitters** deliver key-value messages into Kafka. As an example, an emitter could be a database handler emitting the state changes into Kafka for other interested applications to consume.

* **Processor** is a set of callback functions that consume and perform operations on these emitted messages. *Processor groups* are formed of one or more instances of a processor. Goka distributes a topic's partitions across all the processor instances in a processor group. This enables effortless scaling and fault-tolerance. If a processor instance fails, its partitions are reassigned to the remaining healthy members of the processor group. Processors can also emit further messages into Kafka.

* **Group tables** are partitioned key-value tables stored in Kafka that belong to a single processor group. If a processor instance fails, the remaining instances will take over the group table partitions of the failed instance recovering them from Kafka.

* **Views** are local caches of a processor group's complete group table. Views provide read-only access to the group tables and can be used to provide external services for example through a gRPC interface.


## Get Started

An example Goka application could look like the following:

### Emitter
```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "mini-input"
)

func main() {
  // create a new emitter which allows you to send
  // messages to Kafka
	emitter, err := goka.NewEmitter(brokers, topic,
		new(codec.String))
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}

  // emitter Finish should be called always before
  // terminating the application to ensure the emitter
  // has delivered all the pending messages to Kafka
	defer emitter.Finish()

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

  // on every timer tick, emit a message to containing
  // the current timestamp to Kafka
	i := 0
	for range t.C {
		key := fmt.Sprintf("%d", i%10)
		value := fmt.Sprintf("%s", time.Now())
		emitter.EmitSync(key, value)
		i++
	}
}
```

### Processor
```go
package main

import (
	"log"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "mini-input"
	group   goka.Group  = "mini-group"
)

func main() {
  // Define a new processor group. The group defines all
  // the inputs, output, serialization formats and the
  // topics of the processor
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), process),
		goka.Persist(new(codec.Int64)),
	)
	if p, err := goka.NewProcessor(brokers, g); err != nil {
		log.Fatalf("error creating processor: %v", err)
	} else if err = p.Start(); err != nil {
		log.Fatalf("error running processor: %v", err)
	}
}

// process is the callback the processor will call for
// each message that arrives in the "mini-input" topic.
func process(ctx goka.Context, msg interface{}) {
	var counter int64
  // ctx.Value gets from the group table the value that
  // is stored for the message's key.
	if val := ctx.Value(); val != nil {
		counter = val.(int64)
	}
	counter++
  // SetValue stores the incremented counter in the
  // group table for in the message's key.
	ctx.SetValue(counter)

	log.Println("[proc] key:", ctx.Key(),
		"count:", counter, "msg:", msg)
}
```

### View
```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers            = []string{"localhost:9092"}
	group   goka.Group = "mini-group"
)

func main() {
  // creates a new view which is provides read-only
  // access to the mini-group's group table
	view, err := goka.NewView(brokers,
		goka.GroupTable(group),
		new(codec.Int64),
	)
	if err != nil {
		log.Fatalf("error creating view: %v", err)
	}
  // starting the view begins receiving updates
  // from Kafka
	go view.Start()
	defer view.Stop()

	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

  // on every timer tick, print out the values
  // stored in the group table
	for range t.C {
		for i := 0; i < 10; i++ {
			val, _ := view.Get(fmt.Sprintf("%d", i))
			log.Printf("[view] %d: %v\n", i, val)
		}
	}
}
```

[Apache Kafka]: https://kafka.apache.org/
[GoDoc]: https://godoc.org/github.com/lovoo/goka
