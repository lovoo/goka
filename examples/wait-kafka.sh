#!/bin/bash

#
# waits for kafka to be ready, when it can list topics
# $1: expects the hostname from docker compose and port in array form: localhost:9092,localhost:9093
#

DOCKER_BIN=$([ docker --help ] && echo "docker" || echo "podman" )

wait_for_kafka() {
    IFS=','; for i in $1; do
        host="$(echo "$i" | cut -d':' -f1)"
        port="$(echo "$i" | cut -d':' -f2)"
        while ! ${DOCKER_BIN} exec -i ${host} sh -c "kafka-topics --bootstrap-server localhost:${port} --list"; do
            echo "Waiting for kafka '$i' to list topics..."
            sleep 5
        done
        echo "$i is ready. Continuing."
    done
}

if [ -z $1 ]; then
    echo "no arguments given. Please add then in the comma separated array form 'host:port,host2:port2"
    exit 1
fi

wait_for_kafka "$1";

echo "All dependencies are up. Ready!"