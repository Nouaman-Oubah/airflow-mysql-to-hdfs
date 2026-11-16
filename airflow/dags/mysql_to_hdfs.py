"""Every 5 minutes: copy new MySQL orders to HDFS as CSV and verify they landed.

Flow: extract -> has_new_rows -> load_to_hdfs -> verify_in_hdfs -> update_watermark

The watermark (Airflow Variable `last_order_id`) only moves forward after the
HDFS verification passes, so a failed run is simply re-extracted next time.
"""
import csv
import io
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from hdfs import InsecureClient

MYSQL_CONN_ID = "mysql_shop"
HDFS_CONN_ID = "webhdfs_default"
WATERMARK_VAR = "last_order_id"
HDFS_BASE_DIR = "/data/orders"
COLUMNS = ["id", "customer", "product", "quantity", "amount", "created_at"]


def hdfs_client() -> InsecureClient:
    conn = BaseHook.get_connection(HDFS_CONN_ID)
    return InsecureClient(f"http://{conn.host}:{conn.port or 9870}", user=conn.login or "root")


@dag(
    dag_id="mysql_to_hdfs",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["mysql", "hdfs"],
)
def mysql_to_hdfs():
    @task
    def extract() -> dict:
        watermark = int(Variable.get(WATERMARK_VAR, default_var=0))
        rows = MySqlHook(mysql_conn_id=MYSQL_CONN_ID).get_records(
            f"SELECT {', '.join(COLUMNS)} FROM orders WHERE id > %s ORDER BY id",
            parameters=(watermark,),
        )
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(COLUMNS)
        for row in rows:
            writer.writerow([str(v) for v in row])

        batch = {
            "watermark": watermark,
            "row_count": len(rows),
            "min_id": rows[0][0] if rows else None,
            "max_id": rows[-1][0] if rows else None,
            "csv": buf.getvalue(),
        }
        print(f"watermark={watermark} extracted={len(rows)} ids={batch['min_id']}-{batch['max_id']}")
        return batch

    @task.short_circuit
    def has_new_rows(batch: dict) -> bool:
        return batch["row_count"] > 0

    @task
    def load_to_hdfs(batch: dict, ds=None, ts_nodash=None) -> str:
        path = f"{HDFS_BASE_DIR}/dt={ds}/orders_{ts_nodash}_{batch['min_id']}-{batch['max_id']}.csv"
        hdfs_client().write(path, data=batch["csv"], encoding="utf-8", overwrite=True)
        print(f"Wrote {batch['row_count']} rows to hdfs://{path}")
        return path

    @task
    def verify_in_hdfs(batch: dict, path: str) -> int:
        client = hdfs_client()
        if client.status(path, strict=False) is None:
            raise ValueError(f"File not found in HDFS: {path}")

        with client.read(path, encoding="utf-8") as reader:
            records = list(csv.DictReader(reader))
        ids = [int(r["id"]) for r in records]

        # Compare against MySQL directly: every order the generator wrote in
        # this id range must be present in the HDFS file, and nothing else.
        mysql_ids = [
            r[0]
            for r in MySqlHook(mysql_conn_id=MYSQL_CONN_ID).get_records(
                "SELECT id FROM orders WHERE id BETWEEN %s AND %s ORDER BY id",
                parameters=(batch["min_id"], batch["max_id"]),
            )
        ]
        problems = []
        if len(ids) != batch["row_count"]:
            problems.append(f"row count {len(ids)} != extracted {batch['row_count']}")
        if len(set(ids)) != len(ids):
            problems.append("duplicate ids in HDFS file")
        if sorted(ids) != mysql_ids:
            missing = sorted(set(mysql_ids) - set(ids))
            extra = sorted(set(ids) - set(mysql_ids))
            problems.append(f"id mismatch vs MySQL: missing={missing} extra={extra}")
        if problems:
            raise ValueError(f"HDFS verification failed for {path}: " + "; ".join(problems))

        print(f"VERIFIED: {len(ids)} new rows (ids {batch['min_id']}-{batch['max_id']}) present in hdfs://{path}")
        return batch["max_id"]

    @task
    def update_watermark(max_id: int) -> None:
        Variable.set(WATERMARK_VAR, str(max_id))
        print(f"Watermark {WATERMARK_VAR} -> {max_id}")

    batch = extract()
    gate = has_new_rows(batch)
    path = load_to_hdfs(batch)
    gate >> path
    update_watermark(verify_in_hdfs(batch, path))


mysql_to_hdfs()
