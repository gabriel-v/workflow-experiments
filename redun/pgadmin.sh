docker rm -f phppgadmin
docker run --name='phppgadmin' -d \
  --net host \
    -e 'PGADMIN_DEFAULT_EMAIL=root@root.com' \
    -e 'PGADMIN_DEFAULT_PASSWORD=root' \
    -e 'PGADMIN_LISTEN_PORT=9999' \
dpage/pgadmin4
