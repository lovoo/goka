# Goka

Goka is a simple but powerful stream processing library for Apache Kafka that eases the development of microservices.
Goka extends the concept of _consumer group_ with a _group table_, which represents the state of the group.
A microservice modifies and serves the content of a table employing two complementary object types: processors and views.

A _processor_ is a set of callback functions that modify the group table when messages arrive and may also emit messages into other topics.
Messages as well as rows in the group table are key-value pairs.
Callbacks receive the arriving message and the row addressed by the message's key.

In Kafka, keys are used to partition topics.
A goka processor consumes from a set of co-partitioned topics (topics with the same number of partitions and the same key range).
A _group topic_ keeps track of the group table updates, allowing for recovery and rebalancing of processors:
When multiple processor instances start in the same consumer group, the instances split the co-partitioned input topics and load the respective group table partitions from the group topic.
A local disk storage minimizes recovery time by caching partitions of group table.


Goka also provides views.
A _view_ is a materialized view (ie, a persistent cache) of a _group table_.
A view subscribes for the updates of all partitions of a group table and keeps local disk storage in sync with the group topic.
With a view, one can easily serve up-to-date content of the group table via, for example, gRPC.
