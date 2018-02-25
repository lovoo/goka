#!/bin/bash

## lists the scripts inside the Kafka container and creates local scripts to call them with docker.

set -e

# directory to save the scripts
TARGET=$1
mkdir -p $TARGET

# create Kafka scripts
SCRIPTS=$(docker run --rm -it --entrypoint /bin/bash wurstmeister/kafka -c "ls \$KAFKA_HOME/bin/*.sh")
for SCRIPT in $SCRIPTS; do
	SCRIPT=$(echo $SCRIPT | tr -d '\r')
	FN=$TARGET/$(basename $SCRIPT)
	echo creating $FN
	cat <<-EOF > $FN
		#!/bin/bash
		CMD="$SCRIPT \$@"
		docker run --net=host --rm -it --entrypoint /bin/bash wurstmeister/kafka -c "\$CMD"
EOF
	chmod +x $FN
done

# create ZooKeeper client scriptt
echo creating $TARGET/zkCli.sh
cat <<-EOF > $TARGET/zkCli.sh
	#!/bin/bash
	CMD="bin/zkCli.sh \$@"
	docker run --net=host --rm -it wurstmeister/zookeeper bash -c "\$CMD"
EOF
chmod +x $TARGET/zkCli.sh
