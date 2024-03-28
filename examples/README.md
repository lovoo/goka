# Goka Examples

The examples in this directory will demonstrate different patterns and features
of goka.

## Setup
All examples are runnable out of the box. All you need to do is start
Kafka and Zookeeper locally. Just run

```shell
make start
# or
make restart # if it was started already and you need fresh instances.
```

This will start the docker containers and configure kafka to auto-create the topics.

### Simple Example
This is a very simple toy application that demonstrates how to
 * use an Emitter
 * use a Processor
 * run a View to print the group table's values

 [Example](https://github.com/lovoo/goka/tree/master/examples/1-simplest/)

### Clicks
Similar to the first example, Emitter, Processor and View are demonstrated here.
In addition, it shows how to

* access the View using a web endpoint
* use a Codecs

[Example](https://github.com/lovoo/goka/tree/master/examples/2-clicks/)

### More complex examples
The following examples show the combination of multiple processors, views, etc.

[Messaging Example](https://github.com/lovoo/goka/tree/master/examples/3-messaging/)

By generating a random folder for storage, this example can be executed in parallel multiple times to get a feeling for the rebalancing that's happening under the hood.

[Example](https://github.com/lovoo/goka/tree/master/examples/5-multiple/)


###  Monitoring
Shows how to use the monitoring- and query-interface of goka.

[Example](https://github.com/lovoo/goka/tree/master/examples/8-monitoring)


###  Testing
Shows how to (unit-)test programs using goka.

[Example](https://github.com/lovoo/goka/tree/master/examples/4-tests)

### DeferCommit

Demonstrates the rather new context function to defer (postpone) the commit of a callback.

[Example](https://github.com/lovoo/goka/tree/master/examples/9-defer-commit)
