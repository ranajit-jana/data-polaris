import os

POLARIS_URL = os.getenv("POLARIS_URL", "http://localhost:8181")
POLARIS_CATALOG_API = f"{POLARIS_URL}/api/catalog"
POLARIS_MGMT_API = f"{POLARIS_URL}/api/management/v1"

CLIENT_ID = os.getenv("POLARIS_CLIENT_ID", "root")
CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET", "polaris-secret")

CATALOG_NAME = "streaming_demo"
NAMESPACE = "sensors"
TABLE_NAME = "readings"

# MinIO — local S3-compatible server, data lives in the minio container's /data.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "warehouse"
WAREHOUSE_LOCATION = f"s3://{MINIO_BUCKET}/streaming_demo"
