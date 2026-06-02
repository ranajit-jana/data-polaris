"""
Read and display the current state of the sensor readings Iceberg table.

Usage:
  polaris-read              # single snapshot
  polaris-read --watch      # refresh every 5 s
"""

import sys
import time
from datetime import datetime

import pyarrow.compute as pc

from data_polaris.config import NAMESPACE, TABLE_NAME
from data_polaris.setup_polaris import get_iceberg_catalog

WATCH_INTERVAL = 5


def display(df) -> None:
    total = len(df)
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n{'─'*60}")
    print(f"  {NAMESPACE}.{TABLE_NAME}   [{ts}]   {total:,} total rows")
    print(f"{'─'*60}")

    if total == 0:
        print("  (no data yet — start the producer)")
        return

    temps = df.column("temperature").to_pylist()
    humids = df.column("humidity").to_pylist()
    latest_batch = pc.max(df.column("batch_id")).as_py()

    print(f"  Batches written : {latest_batch}")
    print(f"  Temperature (°C): min={min(temps):.1f}  max={max(temps):.1f}  avg={sum(temps)/len(temps):.1f}")
    print(f"  Humidity    (%) : min={min(humids):.1f}  max={max(humids):.1f}  avg={sum(humids)/len(humids):.1f}")

    # Latest 10 rows
    tail = df.slice(max(0, total - 10))
    print(f"\n  Last {len(tail)} rows:")
    try:
        import pandas as pd
        print(tail.to_pandas().to_string(index=False))
    except ImportError:
        for row in tail.to_pydict().items():
            print(f"    {row}")


def main() -> None:
    watch = "--watch" in sys.argv
    catalog = get_iceberg_catalog()
    table = catalog.load_table((NAMESPACE, TABLE_NAME))

    while True:
        df = table.scan().to_arrow()
        display(df)
        if not watch:
            break
        time.sleep(WATCH_INTERVAL)


if __name__ == "__main__":
    main()
