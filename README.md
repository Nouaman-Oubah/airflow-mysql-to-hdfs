# MySQL → Airflow → HDFS pipeline

```
generator (every 1 min, 1-10 fake orders) ──> MySQL shop.orders
Airflow DAG mysql_to_hdfs (every 5 min):
  extract new rows (id > watermark) -> write CSV to HDFS -> verify in HDFS vs MySQL -> move watermark
```

## Start
Requires Docker Desktop (WSL2 backend, ~6 GB RAM).

Create your local `.env` from the template and set your own passwords:
```bash
cp .env.example .env
```

```bash
docker compose up -d --build
```

| Service | URL / port | Login |
|---|---|---|
| Airflow UI | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env` |
| HDFS NameNode UI | http://localhost:9870 (Utilities → Browse the file system) | – |
| MySQL | localhost:3306 | `MYSQL_USER` / `MYSQL_PASSWORD` from `.env` (db `shop`) |

The DAG is unpaused automatically; first run happens at the next 5-minute mark.

## Check it works
Run these from PowerShell or cmd. In Git Bash, put `MSYS_NO_PATHCONV=1` in front of any command that has an HDFS path like `/data/...`, or Git Bash rewrites it to a Windows path and the command prints nothing.

Generator is inserting:
```bash
docker compose logs -f generator
```

Rows in MySQL:
```bash
docker exec mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" shop -e "SELECT COUNT(*), MAX(id) FROM orders"'
```

Files in HDFS:
```bash
docker exec namenode hdfs dfs -ls -R /data/orders
```

Total rows in HDFS (minus one header line per file) should equal MySQL rows up to the watermark:
```bash
docker exec namenode bash -c "hdfs dfs -cat /data/orders/*/*.csv | grep -v '^id,' | wc -l"
```

Current watermark:
```bash
docker exec airflow-scheduler airflow variables get last_order_id
```

In the Airflow UI, the `verify_in_hdfs` task log prints `VERIFIED: N new rows ... present in hdfs://...`.
It fails the run if the file is missing, the row count differs, or the ids don't match MySQL.

## Failure test
```bash
docker stop datanode
```
Wait for a run to fail (watermark stays put), then:
```bash
docker start datanode
```
The next run picks up all rows that were missed.

## Stop / reset
```bash
docker compose down
```
Add `-v` to also delete all MySQL, HDFS and Airflow data.
