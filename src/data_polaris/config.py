import os

POLARIS_URL = os.getenv("POLARIS_URL", "http://localhost:8181")
POLARIS_CATALOG_API = f"{POLARIS_URL}/api/catalog"
POLARIS_MGMT_API = f"{POLARIS_URL}/api/management/v1"

# Root credentials configured in docker-compose.yml via POLARIS_BOOTSTRAP_CREDENTIALS.
# Override with env vars if you extract them from `docker logs polaris` instead.
CLIENT_ID = os.getenv("POLARIS_CLIENT_ID", "root")
CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET", "polaris-secret")

CATALOG_NAME = "streaming_demo"
NAMESPACE = "sensors"
TABLE_NAME = "readings"

# Bind-mounted at the same path in the container and on the host so that
# PyIceberg running locally can resolve file:// paths returned by Polaris.
WAREHOUSE_DIR = "/tmp/polaris-warehouse"
