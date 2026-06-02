"""
One-time bootstrap:
  1. Wait for Polaris to start.
  2. Create an INTERNAL catalog backed by local FILE storage.
  3. Grant catalog access to the root principal.
  4. Create the namespace and table via PyIceberg REST catalog.
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
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
    WAREHOUSE_LOCATION,
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
            requests.get(f"{POLARIS_URL}/", timeout=2)
            print("Polaris is ready.")
            return
        except Exception:
            print(f"  attempt {attempt}/{retries} ...")
            time.sleep(2)
    raise RuntimeError("Polaris did not respond — check `docker compose logs polaris`")


def _credentials_from_docker_logs() -> tuple[str, str] | None:
    import re, subprocess
    try:
        out = subprocess.check_output(
            ["docker", "logs", "polaris"], stderr=subprocess.STDOUT, text=True
        )
        m = re.search(r"root principal credentials:\s+(\S+):(\S+)", out)
        if m:
            return m.group(1), m.group(2)
    except Exception:
        pass
    return None


def get_token() -> str:
    client_id, client_secret = CLIENT_ID, CLIENT_SECRET
    r = requests.post(
        f"{POLARIS_CATALOG_API}/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    if r.status_code == 401:
        fallback = _credentials_from_docker_logs()
        if fallback:
            client_id, client_secret = fallback
            print(
                f"⚠  Using auto-generated credentials from Docker logs.\n"
                f"   Set POLARIS_CLIENT_ID={client_id} POLARIS_CLIENT_SECRET={client_secret} to avoid this."
            )
            r = requests.post(
                f"{POLARIS_CATALOG_API}/v1/oauth/tokens",
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": "PRINCIPAL_ROLE:ALL",
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
    r.raise_for_status()
    return r.json()["access_token"]


def create_catalog(token: str) -> None:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
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
                "properties": {"default-base-location": WAREHOUSE_LOCATION},
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": [f"s3://{MINIO_BUCKET}/"],
                    # Polaris requires a roleArn for S3. MinIO's AssumeRole accepts
                    # any ARN and returns credentials based on the calling user.
                    "roleArn": "arn:aws:iam::000000000000:role/minio-local-demo",
                },
            }
        },
    )
    r.raise_for_status()
    print(f"Created catalog '{CATALOG_NAME}'.")


def setup_catalog_rbac(token: str) -> None:
    """Grant CATALOG_MANAGE_CONTENT so the root principal can create tables."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    role_name = "demo_writer"
    requests.post(
        f"{POLARIS_MGMT_API}/catalogs/{CATALOG_NAME}/catalog-roles",
        headers=headers,
        json={"catalogRole": {"name": role_name}},
    )
    r = requests.put(
        f"{POLARIS_MGMT_API}/catalogs/{CATALOG_NAME}/catalog-roles/{role_name}/grants",
        headers=headers,
        json={"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
    )
    r.raise_for_status()
    requests.put(
        f"{POLARIS_MGMT_API}/principal-roles/service_admin/catalog-roles/{CATALOG_NAME}",
        headers=headers,
        json={"catalogRole": {"name": role_name}},
    )
    print(f"RBAC: '{role_name}' granted CATALOG_MANAGE_CONTENT → service_admin.")


def get_iceberg_catalog() -> RestCatalog:
    return RestCatalog(
        name="polaris_streaming",
        **{
            "uri": POLARIS_CATALOG_API,
            "warehouse": CATALOG_NAME,
            "credential": f"{CLIENT_ID}:{CLIENT_SECRET}",
            "scope": "PRINCIPAL_ROLE:ALL",
            # PyIceberg's own FileIO talks to MinIO directly (path-style).
            # These are used both as fallback credentials and as the S3 endpoint
            # config that carries forward even when Polaris vends STS credentials.
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def setup_table() -> None:
    catalog = get_iceberg_catalog()
    ns = (NAMESPACE,)
    if ns not in catalog.list_namespaces():
        catalog.create_namespace(ns)
        print(f"Created namespace '{NAMESPACE}'.")
    else:
        print(f"Namespace '{NAMESPACE}' already exists.")
    table_id = (NAMESPACE, TABLE_NAME)
    if not catalog.table_exists(table_id):
        # create_table_transaction uses stage-create=True: Polaris registers
        # the table without writing to S3 itself. PyIceberg writes the initial
        # metadata.json via its own FileIO (path-style MinIO), then commits.
        txn = catalog.create_table_transaction(identifier=table_id, schema=SENSOR_SCHEMA)
        txn.commit_transaction()
        print(f"Created table '{NAMESPACE}.{TABLE_NAME}'.")
    else:
        print(f"Table '{NAMESPACE}.{TABLE_NAME}' already exists.")


def main() -> None:
    wait_for_polaris()
    token = get_token()
    create_catalog(token)
    setup_catalog_rbac(token)
    setup_table()
    print("\nSetup complete! Run 'polaris-produce' to start streaming.\n")


if __name__ == "__main__":
    main()
