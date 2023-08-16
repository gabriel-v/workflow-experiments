docker rm -f phppgadmin
docker run --name='phppgadmin' -d \
  --publish=80:80 \
  --net host \
    -e 'PGADMIN_DEFAULT_EMAIL=r@r.r' \
    -e 'PGADMIN_DEFAULT_PASSWORD=root' \
dpage/pgadmin4
