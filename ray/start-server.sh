#!/bin/bash -ex

SYSTEM_CONFIG='
{"object_spilling_config":"{\"type\":\"filesystem\",\"params\":{\"directory_path\":\"/tmp/ray-obj-spill\"}}"
,"kill_idle_workers_interval_ms": 5000}
'

ray start --head --node-ip-address=0.0.0.0 --port=6969  \
        --dashboard-host 0.0.0.0 \
        --num-cpus=20 \
        --disable-usage-stats \
        --object-store-memory=4294967296 \
        --min-worker-port 20000 --max-worker-port 40000 \
        --system-config="$SYSTEM_CONFIG"

