docker rm -f phppgadmin
mkdir -p pg_admin_data
docker run --name='phppgadmin' -d \
  --net host \
    -e 'PGADMIN_DEFAULT_EMAIL=root@root.com' \
    -e 'PGADMIN_DEFAULT_PASSWORD=root' \
    -e 'PGADMIN_LISTEN_PORT=9999' \
    -v "./pg_admin_data:/var/lib/pgadmin" \
dpage/pgadmin4
