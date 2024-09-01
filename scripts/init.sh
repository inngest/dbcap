#!/bin/bash

docker rm -f pg16
docker run -d --name pg16 -p 5432 \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=db \
  --net=host \
  postgres:16 \
  postgres -c wal_level=logical

sleep 2

DATABASE_URL=postgres://postgres:password@localhost:5432/db go run ./scripts/init.go
DATABASE_URL=postgres://postgres:password@localhost:5432/db go run ./scripts/push_data.go

# 
# docker exec -ti pg16 psql -U postgres -d db -c "CREATE USER inngest WITH REPLICATION PASSWORD 'password'";
# 
# docker exec -ti pg16 psql -U postgres -d db -c "GRANT USAGE ON SCHEMA public TO inngest;
# GRANT SELECT ON ALL TABLES IN SCHEMA public TO inngest;
# ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO inngest;";
# 
# docker exec -ti pg16 psql -U postgres -D db -c "CREATE PUBLICATION inngest FOR ALL TABLES;"
