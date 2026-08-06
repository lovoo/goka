all:
	@echo targets: start restart stop

DOCKER_BIN ?= $(shell command -v docker 2>/dev/null || command -v podman 2>/dev/null || echo "docker")

# stop and start docker compose (zookeeper and kafka)
restart: stop start
	@echo done

# start docker compose (zookeeper and kafka) and wait until all are up
start: compose-up
	@echo done
	@sh wait.sh "localhost:2181,localhost:9092,localhost:9093,localhost:9094,localhost:6379"
	@sh wait-kafka.sh "kafka1:9092,kafka2:9093,kafka3:9094"

# shutdown docker compose (zookeeper and kafka)
stop: compose-down
	@echo done

compose-up:
	@echo start docker compose
	@$(DOCKER_BIN) compose up -d

compose-down:
	@echo stop docker compose
	@$(DOCKER_BIN) compose down
