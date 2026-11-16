"""Insert a random batch of fake orders into MySQL every INTERVAL_SECONDS."""
import logging
import os
import random
import time
from decimal import Decimal

import mysql.connector
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("generator")

DB = dict(
    host=os.getenv("MYSQL_HOST", "localhost"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "shop_user"),
    password=os.environ["MYSQL_PASSWORD"],
    database=os.getenv("MYSQL_DATABASE", "shop"),
)
INTERVAL = int(os.getenv("INTERVAL_SECONDS", "60"))
MIN_ROWS = int(os.getenv("MIN_ROWS", "1"))
MAX_ROWS = int(os.getenv("MAX_ROWS", "10"))

PRODUCTS = {
    "Laptop": Decimal("899.00"),
    "Monitor": Decimal("189.99"),
    "Keyboard": Decimal("49.90"),
    "Mouse": Decimal("24.50"),
    "Headphones": Decimal("79.00"),
    "Webcam": Decimal("59.95"),
    "USB-C Hub": Decimal("34.99"),
}

fake = Faker()


def connect():
    while True:
        try:
            conn = mysql.connector.connect(**DB)
            log.info("Connected to MySQL at %s:%s", DB["host"], DB["port"])
            return conn
        except mysql.connector.Error as exc:
            log.warning("MySQL not ready (%s), retrying in 5s", exc)
            time.sleep(5)


def make_row():
    product, price = random.choice(list(PRODUCTS.items()))
    qty = random.randint(1, 5)
    return fake.name(), product, qty, price * qty


def insert_batch(conn):
    rows = [make_row() for _ in range(random.randint(MIN_ROWS, MAX_ROWS))]
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO orders (customer, product, quantity, amount) VALUES (%s, %s, %s, %s)",
        rows,
    )
    conn.commit()
    first_id = cur.lastrowid
    cur.close()
    log.info("Inserted %d rows (ids %d-%d)", len(rows), first_id, first_id + len(rows) - 1)


def main():
    conn = connect()
    while True:
        try:
            if not conn.is_connected():
                conn = connect()
            insert_batch(conn)
        except mysql.connector.Error as exc:
            log.error("Insert failed: %s", exc)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
