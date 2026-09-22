Bật docker desktop hoặc docker engine

```bash
docker compose up -d postgres
```

```bash
docker exec -it postgres_dwh psql -U dataeng -d dwh -c "CREATE DATABASE airflow;"
docker exec -it postgres_dwh psql -U dataeng -d dwh -c "CREATE DATABASE superset;"
```

```bash
docker compose up -d
```

```bash
docker compose restart airflow-webserver airflow-scheduler
```

```bash
# 1. Build the database tables
docker exec -it superset_bi superset db upgrade

# 2. Create your admin login (Username: admin, Password: admin)
docker exec -it superset_bi superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin

# 3. Setup default roles and permissions
docker exec -it superset_bi superset init
```

vào browser mở `localhost:8080` và `localhost:8088` nhập username và password đều là admin
