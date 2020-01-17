all:
	@echo targets: start restart stop

# stop and start docker compose (zookeeper and kafka)
restart: stop start
	@echo done

# start docker compose (zookeeper and kafka)
start: compose-up
	@echo done

# shutdown docker compose (zookeeper and kafka)
stop: compose-down
	@echo done

compose-up:
	@echo start docker compose
	@docker-compose up -d

compose-down:
	@echo stop docker compose
	@docker-compose down
