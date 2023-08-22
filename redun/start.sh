#!/bin/bash
set -ex
rm -rf *.png *.dot redun.db .redun tmp

until PGUSER=postgres PGPASSWORD=postgres psql postgresql://localhost:5432 -l --no-readline >/dev/null; do sleep 1; done


for i in $( seq 1 16 ); do
  echo $i; 
  python qxxworker.py &
done

python w.py || sleep 1000000000
