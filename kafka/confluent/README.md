This a wrapper around the confluent-kafka-go library.

To use library
- Create processor or view with confluent consumer, eg, `goka.WithConsumerBuilder(confluent.NewConsumerBuilder(1000))`
- Install `librdkafka` in the compilation environment
- Compile the go binary with `-tags "confluent static"`
- Install `libssl1.0.0` `libsasl2-2` (or equivalent) in the execution environment

Note that this is experimental, not well tested and features are missing (in particular `auto.commit` is set to true).

