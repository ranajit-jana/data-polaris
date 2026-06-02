#!/usr/bin/env bash
# Full end-to-end demo:
#   1. Start Polaris in Docker
#   2. Bootstrap catalog / namespace / table
#   3. Run producer in the background
#   4. Poll the reader every few seconds
set -euo pipefail

echo "=== Data Streaming with Apache Polaris (Iceberg REST catalog) ==="
echo ""

# FILE storage bind-mount: container and host must share the same absolute path.
mkdir -p /tmp/polaris-warehouse

echo ">>> Starting Polaris..."
docker compose up -d

echo ">>> Installing / syncing Python dependencies..."
uv sync

echo ">>> Bootstrapping catalog, namespace, and table..."
uv run polaris-setup

echo ">>> Starting producer in background..."
uv run polaris-produce &
PRODUCER_PID=$!
echo "    Producer PID: $PRODUCER_PID"

# Give the producer a head-start before we start reading
sleep 6

echo ""
echo ">>> Reading table (3 snapshots, 8 s apart)..."
for i in 1 2 3; do
    uv run polaris-read
    [ $i -lt 3 ] && sleep 8
done

echo ""
echo ">>> Stopping producer..."
kill "$PRODUCER_PID" 2>/dev/null || true

echo ""
echo "=== Demo done ==="
echo "    Keep streaming : uv run polaris-produce"
echo "    Watch the table: uv run polaris-read --watch"
echo "    Polaris UI     : http://localhost:8181"
