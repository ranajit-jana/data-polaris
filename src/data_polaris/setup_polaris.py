"""
One-time bootstrap:
  1. Wait for Polaris to start.
  2. Create the Iceberg catalog (FILE storage, no cloud creds needed).
  3. Create the namespace and table via PyIceberg REST catalog.
"""

import time
import requests
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    TimestamptzType,
    DoubleType,
    LongType,
)

from data_polaris.config import (
    POLARIS_URL,
    POLARIS_CATALOG_API,
    POLARIS_MGMT_API,
    CLIENT_ID,
    CLIENT_SECRET,
    CATALOG_NAME,
    NAMESPACE,
    TABLE_NAME,
    WAREHOUSE_DIR,
)

SENSOR_SCHEMA = Schema(
    NestedField(1, "sensor_id", StringType(), required=True),
    NestedField(2, "ts", TimestamptzType(), required=True),
    NestedField(3, "temperature", DoubleType()),
    NestedField(4, "humidity", DoubleType()),
    NestedField(5, "pressure", DoubleType()),
    NestedField(6, "batch_id", LongType()),
)


def wait_for_polaris(retries: int = 30) -> None:
    print("Waiting for Polaris to be ready...")
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(f"{POLARIS_URL}/", timeout=2)
            # Any HTTP response (even 404) means the server is up.
            print("Polaris is ready.")
            return
        except Exception:
            print(f"  attempt {attempt}/{retries} ...")
            time.sleep(2)
    raise RuntimeError("Polaris did not respond — check `docker compose logs polaris`")


def get_token() -> str:
    r = requests.post(
        f"{POLARIS_CATALOG_API}/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    r.raise_for_status()
    return r.json()["access_token"]


def create_catalog(token: str) -> None:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    r = requests.get(f"{POLARIS_MGMT_API}/catalogs/{CATALOG_NAME}", headers=headers)
    if r.status_code == 200:
        print(f"Catalog '{CATALOG_NAME}' already exists.")
        return

    r = requests.post(
        f"{POLARIS_MGMT_API}/catalogs",
        headers=headers,
        json={
            "catalog": {
                "name": CATALOG_NAME,
                "type": "INTERNAL",
                "properties": {
                    "default-base-location": f"file://{WAREHOUSE_DIR}/{CATALOG_NAME}"
                },
                "storageConfigInfo": {
                    "storageType": "FILE",
                    "allowedLocations": [f"file://{WAREHOUSE_DIR}/"],
                },
            }
        },
    )
    r.raise_for_status()
    print(f"Created catalog '{CATALOG_NAME}'.")


def get_iceberg_catalog() -> RestCatalog:
    return RestCatalog(
        name="polaris_streaming",
        **{
            "uri": POLARIS_CATALOG_API,
            "warehouse": CATALOG_NAME,
            "credential": f"{CLIENT_ID}:{CLIENT_SECRET}",
            "scope": "PRINCIPAL_ROLE:ALL",
        },
    )


def setup_table() -> None:
    catalog = get_iceberg_catalog()

    ns = (NAMESPACE,)
    existing_ns = [n for n in catalog.list_namespaces()]
    if ns not in existing_ns:
        catalog.create_namespace(ns)
        print(f"Created namespace '{NAMESPACE}'.")
    else:
        print(f"Namespace '{NAMESPACE}' already exists.")

    table_id = (NAMESPACE, TABLE_NAME)
    if not catalog.table_exists(table_id):
        catalog.create_table(identifier=table_id, schema=SENSOR_SCHEMA)
        print(f"Created table '{NAMESPACE}.{TABLE_NAME}'.")
    else:
        print(f"Table '{NAMESPACE}.{TABLE_NAME}' already exists.")


def main() -> None:
    wait_for_polaris()
    token = get_token()
    create_catalog(token)
    setup_table()
    print("\nSetup complete! Run 'polaris-produce' to start streaming.\n")


if __name__ == "__main__":
    main()
