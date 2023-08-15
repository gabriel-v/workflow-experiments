#!/bin/bash
set -ex
export DOCKER_IMG=redun_test_img:redun_test_img
export DOCKER_CONTAINER_NAME=redun_test_cont
export MOUNT_DATA=/opt/node/collections/testdata/data

docker build . --tag $DOCKER_IMG >/dev/null

docker rm -f $DOCKER_CONTAINER_NAME || true
if ! ( docker ps | grep $DOCKER_CONTAINER_NAME ) >/dev/null; then
        docker run \
                -d \
                --rm \
                --hostname $DOCKER_CONTAINER_NAME \
                --name $DOCKER_CONTAINER_NAME \
                -v "$PWD:/v" \
                -v "$MOUNT_DATA:$MOUNT_DATA" \
                -w /v \
                -u "$(id -u):$(id -g)" \
                --net host \
                --shm-size=4gb \
                --memory 6gb \
                --memory-swap 6gb \
                $DOCKER_IMG \
                sleep 10000000
fi

time docker exec $DOCKER_CONTAINER_NAME ./start.sh
