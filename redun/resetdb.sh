docker rm -f redun-pg || true
rm -rf pg_data || true
mkdir pg_data

if [ -s "$RAMONLY" ]; then
docker run -d --rm --name redun-pg  \
        -e POSTGRES_PASSWORD=postgres \
        -p 5432:5432 \
        -v "./pg.conf:/etc/postgresql.conf" \
        --mount type=tmpfs,destination=/var/lib/postgresql/data \
        --shm-size=5gb \
        --memory 6gb \
        --memory-swap 6gb \
        postgres:14.5
else
docker run -d --rm --name redun-pg  \
        -v "./pg_data:/var/lib/postgresql/data" \
        -u "$(id -u):$(id -g)" \
        -e POSTGRES_PASSWORD=postgres \
        -p 5432:5432 \
        -v "./pg.conf:/etc/postgresql.conf" \
        --shm-size=5gb \
        --memory 6gb \
        --memory-swap 6gb \
        postgres:14.5

fi
