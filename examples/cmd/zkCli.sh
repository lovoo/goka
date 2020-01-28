#!/bin/bash
CMD="bin/zkCli.sh $@"
docker run --net=host --rm -it zookeeper:3.4.9 bash -c "$CMD"
