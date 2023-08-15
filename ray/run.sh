#!/bin/bash -ex
export DOCKER_IMG=ray_test_img:ray_test_img
export DOCKER_CONTAINER_NAME=ray_test_cont
export MOUNT_DATA=/opt/node/collections/testdata/data

if ! docker image inspect $DOCKER_IMG >/dev/null; then
        docker build . --tag $DOCKER_IMG
fi

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
                -p 8265:8265 \
                -p 6969:6969 \
                --shm-size=4gb \
                --memory 6gb \
                --memory-swap 6gb \
                $DOCKER_IMG \
                sleep 10000000
        docker exec $DOCKER_CONTAINER_NAME ./start-server.sh
        docker exec $DOCKER_CONTAINER_NAME ./start-workers.sh
fi

time docker exec $DOCKER_CONTAINER_NAME ./start-task.sh
