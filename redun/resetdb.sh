docker rm -f redun-pg || true
rm -rf pg_data || true
mkdir pg_data
docker run -d --rm --name redun-pg  \
        -e POSTGRES_PASSWORD=postgres \
        -p 5432:5432 \
        -v "./pg_data:/var/lib/postgresql/data" \
        -v "./pg.conf:/etc/postgresql.conf" \
        -u "$(id -u):$(id -g)" \
        --shm-size=5gb \
        --memory 6gb \
        --memory-swap 6gb \
        postgres:14.5
