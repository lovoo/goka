# Goka Example (Redis)

Using [Redis](https://redis.io/) as option of storage.

This example has an **Emitter** and one **Processor** with when this emitter
generates events with random keys (_user_id_) consumed by Kafka that uses
Redis as storage/cache.


## Usage

It's easy to configures this example.

  1. Starts the Kafka present on [`docker-compose`](kafka.yml) file with command bellow:

  ```console
  docker-compose -f kafka.yml -p ci up -d
  ```

  2. Check the status of docker containers:

  ```console
  docker ps
  ```

  Resulting for example:

  ```console
  CONTAINER ID        IMAGE                 COMMAND                  CREATED             STATUS              PORTS                                            NAMES
  38a6efb145ba        wurstmeister/kafka    "start-kafka.sh"         9 seconds ago       Up 4 seconds        0.0.0.0:9092->9092/tcp, 0.0.0.0:9997->9997/tcp   kafka
  48df80931a1f        confluent/zookeeper   "/usr/local/bin/zk-d…"   10 seconds ago      Up 5 seconds        2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp       zookeeper
  040fbe9dfc13        redis:latest          "docker-entrypoint.s…"   10 seconds ago      Up 5 seconds        0.0.0.0:6379->6379/tcp                           redis
  ```

  This same configuration should be present on `config.yaml` file, with kafka an redis like:

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

  3. Fetch the go [redis](gopkg.in/redis.v5) client package:

  ```console
  go get -u gopkg.in/redis.v5
  ```

  4. Build and run the example:

  ```console
  go build .
  ./redis -config config.yaml
  ```

  The events are produced and consumed by Kafka with random keys. It's possible
  run several of the same binary and check the behaviour of kafka
  rebalancing and removing partitions without broken.
