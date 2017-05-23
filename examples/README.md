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
In Addition it shows how to

* access the View using a web endpoint
* use a Codecs

[Example](https://github.com/lovoo/goka/tree/master/examples/2-clicks/)

###  Monitoring
Shows how to use the monitoring- and query-interface of goka.

TODO

[Example](https://github.com/lovoo/goka/tree/master/examples/monitoring)


###  Testing
Shows how to (unit-)test programs using goka.

TODO

[Example](https://github.com/lovoo/goka/tree/master/examples/testing)
