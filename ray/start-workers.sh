#!/bin/bash -ex
until (
        ray start \
                --address=127.0.0.1:6969 \
                --num-cpus=20 --object-store-memory=4294967296 \
                --min-worker-port 42000 --max-worker-port 63000 \
                --dashboard-port 41000 --dashboard-grpc-port 41001 --dashboard-agent-grpc-port 41002 \
                --dashboard-agent-listen-port 41003 --metrics-export-port 41004 \
                --disable-usage-stats
); do sleep 10; done
