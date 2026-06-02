"""
Continuous streaming producer.
Generates random IoT sensor readings and appends them to the Iceberg table
via the Polaris REST catalog every INTERVAL seconds.
"""

import random
import signal
import time
from datetime import datetime, timezone

import pyarrow as pa
from faker import Faker

from data_polaris.config import NAMESPACE, TABLE_NAME
from data_polaris.setup_polaris import get_iceberg_catalog

fake = Faker()

SENSOR_IDS = [f"sensor_{i:03d}" for i in range(1, 21)]
INTERVAL_SECONDS = 3
ROWS_PER_BATCH = 20

ARROW_SCHEMA = pa.schema(
    [
        pa.field("sensor_id", pa.string(), nullable=False),
        pa.field("ts", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("temperature", pa.float64()),
        pa.field("humidity", pa.float64()),
        pa.field("pressure", pa.float64()),
        pa.field("batch_id", pa.int64()),
    ]
)


def generate_batch(batch_id: int) -> pa.Table:
    now = datetime.now(timezone.utc)
    n = ROWS_PER_BATCH
    return pa.table(
        {
            "sensor_id": [random.choice(SENSOR_IDS) for _ in range(n)],
            "ts": [now] * n,
            "temperature": [round(random.gauss(22.0, 6.0), 2) for _ in range(n)],
            "humidity": [round(random.uniform(30.0, 90.0), 2) for _ in range(n)],
            "pressure": [round(random.uniform(995.0, 1025.0), 2) for _ in range(n)],
            "batch_id": [batch_id] * n,
        },
        schema=ARROW_SCHEMA,
    )


def main() -> None:
    catalog = get_iceberg_catalog()
    table = catalog.load_table((NAMESPACE, TABLE_NAME))
    print(f"Connected → {NAMESPACE}.{TABLE_NAME}")
    print(f"Writing {ROWS_PER_BATCH} rows every {INTERVAL_SECONDS}s  (Ctrl+C to stop)\n")

    running = True
    batch_id = 0

    def _stop(sig, frame):
        nonlocal running
        print("\nStopping producer...")
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    while running:
        batch_id += 1
        batch = generate_batch(batch_id)
        table.append(batch)
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] batch {batch_id:04d}  → {len(batch)} rows written")
        time.sleep(INTERVAL_SECONDS)

    print(f"\nProducer stopped. Total batches: {batch_id}")


if __name__ == "__main__":
    main()
