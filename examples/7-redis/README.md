# Redis storage example

Using [Redis](https://redis.io/) as option of storage.

This example has an **Emitter** and one **Processor**. The emitter generates
events with random keys (_user_id_) consumed by Kafka. The processor consumes
this events and uses redis as storage/cache.


## Usage

Run a local Kafka cluster and redis server by calling `make start` in folder `examples/`.
This will start a kafka cluster accessible under `127.0.0.1:9092` and a redis service under
`127.0.0.1:6379` if you want to use a different configuration you need to adjust the config 
file `config.yaml`.

The config file also has some more options to set:

```yaml
kafka:
  brokers:
    - "127.0.0.1:9092"
  group: "examples"
  stream: "events"
  redis: "127.0.0.1:6379"
  namespace: "producer"
```

Where:
  * **brokers** : slice of kafka brokers hosts.
  * **group** : group name of this example belongs.
  * **stream**: stream name of this example belongs.
  * **redis**: address of redis (`localhost:6379`).
  * **namespace**: namespace distinguish applications that write to the same keys on Redis.

After starting the machines and adjusting the config run the example with
`go run 7-redis/*.go -config 7-redis/config.yaml`.

The events are produced and consumed by Kafka with random keys. It's possible
run several of the same binary and check the behaviour of kafka
rebalancing and removing partitions.

After you are finished with the example you may stop the Kafka cluster and redis server by
entering `make stop` in the `examples/` folder.
