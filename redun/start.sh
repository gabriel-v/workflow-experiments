#!/bin/bash
set -ex
rm -rf *.png *.dot redun.db .redun tmp

until PGUSER=postgres PGPASSWORD=postgres psql postgresql://localhost:5432 -l --no-readline >/dev/null; do sleep 1; done

# PGUSER=postgres PGPASSWORD=postgres pipenv run redun init

PGUSER=postgres PGPASSWORD=postgres python dir_file.py
