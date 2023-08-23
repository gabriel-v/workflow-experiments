#!/bin/bash
set -ex
rm -rf *.png *.dot redun.db .redun tmp __pycache__

until PGUSER=postgres PGPASSWORD=postgres psql postgresql://localhost:5432 -l --no-readline >/dev/null; do sleep 1; done

sleep 1

for i in $( seq 1 18 ); do
  python w.py start-pg-executor-worker default &
done

sleep 1

# python w.py &
python w.py || true
sleep 1000000000
