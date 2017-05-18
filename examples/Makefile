# Makefile to get Zookeeper and Kafka up and running

ifdef DOCKER_HOST
	DOCKER_IP=$(shell docker-machine ip)
else
	DOCKER_IP=127.0.0.1
endif

all:
	@echo targets: start restart stop

# delete all images and restart
restart: stop start
	@echo done

# start all images
start: run-zk run-kafka
	@echo done

# stop all images (forces deletion of images)
stop:
	@echo stopping Kafka and Zookeeper
	@docker rm -f zookeeper kafka

run-zk:
	@echo starting Zookeeper...
	@docker run -d -t --name zookeeper \
	-p 2181:2181                       \
	wurstmeister/zookeeper

run-kafka:
	@echo starting Kafka...
	@docker run -d -t --name kafka             \
	-e HOST_IP=kafka                           \
	-e KAFKA_ADVERTISED_HOST_NAME=${DOCKER_IP} \
	-e KAFKA_ADVERTISED_PORT=9092              \
	-e KAFKA_NUM_PARTITIONS=10                 \
	-e KAFKA_DEFAULT_REPLICATION_FACTOR=1      \
	-e KAFKA_REPLICATION_FACTOR=1              \
	-p ${DOCKER_IP}:9092:9092                  \
	-p ${DOCKER_IP}:9997:9997                  \
	-e KAFKA_BROKER_ID=1                       \
	-e ZK=zk -p 9092                           \
	--link zookeeper:zk                        \
	wurstmeister/kafka:0.10.1.0
