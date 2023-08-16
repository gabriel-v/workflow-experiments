#!/bin/bash
set -ex
export DOCKER_CONTAINER_NAME=redun_test_cont

time docker exec -it $DOCKER_CONTAINER_NAME $@
