# Data Streaming with Apache Polaris

A hands-on demo that streams simulated IoT sensor data into **Apache Iceberg** tables managed by the **Apache Polaris** REST catalog.

---

## What is Apache Polaris?

**Apache Polaris** (incubating) is an open-source, vendor-neutral **catalog for Apache Iceberg** tables. Think of it as the "control plane" for your data lakehouse — it knows where every table lives, what its schema looks like, and who is allowed to access it.

Polaris was originally built by Snowflake and donated to the Apache Software Foundation in 2024. It is now the reference implementation of the **Iceberg REST Catalog specification**.

### The Problem It Solves

Before Iceberg catalogs like Polaris existed, teams struggled with:

- **Discovery**: "Where is the `orders` table? Which S3 path?"
- **Governance**: "Who can read customer data? Who can delete partitions?"
- **Interoperability**: Spark, Flink, DuckDB, and Trino each needed their own connector to the same data.

Polaris solves all three with a single REST API that every engine can speak.

---

## Core Concepts

### 1. Iceberg Table Format

Apache Iceberg is an open **table format** for large-scale data. A table is not just a folder of Parquet files — it's a structured set of metadata:

```
s3://warehouse/my_table/
├── metadata/
│   ├── v1.metadata.json   ← schema, partition spec, snapshots
│   └── snap-123.avro      ← list of data files in this snapshot
└── data/
    └── part-00000.parquet ← actual rows
```

Every write creates a new **snapshot**. Old snapshots are retained, giving you time-travel queries for free.

### 2. Why Parquet? (and why it beats CSV / JSON / Avro for analytics)

Iceberg uses **Apache Parquet** as its default data file format. Understanding why helps explain many of the performance characteristics of a modern data lakehouse.

#### Row-oriented vs. column-oriented storage

The fundamental difference is how bytes are arranged on disk:

```
CSV / JSON / Avro  — ROW-oriented
┌──────────────────────────────────────────────┐
│ row 1:  sensor_01 | 2024-01-01 | 22.5 | 65.0 │
│ row 2:  sensor_02 | 2024-01-01 | 19.3 | 70.1 │
│ row 3:  sensor_03 | 2024-01-01 | 25.8 | 55.4 │
│ ...                                          │
└──────────────────────────────────────────────┘

Parquet  — COLUMN-oriented
┌──────────────────────────────────────────────┐
│ sensor_id col:   sensor_01, sensor_02, ...   │
│ ts col:          2024-01-01, 2024-01-01, ... │
│ temperature col: 22.5, 19.3, 25.8, ...       │
│ humidity col:    65.0, 70.1, 55.4, ...       │
└──────────────────────────────────────────────┘
```

When you run `SELECT AVG(temperature) FROM readings`, a row-oriented format must read every byte of every row just to extract the one column you care about. Parquet reads only the temperature column bytes — everything else stays on disk.

#### The five advantages in detail

**1. Projection pushdown — read only the columns you need**

Analytics queries rarely need all columns. A query touching 3 of 50 columns reads ~6% of the data with Parquet vs. 100% with CSV. This directly reduces I/O, cost (cloud storage egress), and query time.

**2. Predicate pushdown — skip rows before reading them**

Each Parquet file stores **column statistics** in its footer: the min and max value of every column in every row group (a chunk of ~128 MB of rows).

```
Parquet file footer (read first, ~few KB):
  Row group 0:  temperature  min=18.1  max=28.4
  Row group 1:  temperature  min=30.2  max=41.7
  Row group 2:  temperature  min=15.0  max=22.9

Query: WHERE temperature > 35
  → skip row group 0  (max 28.4 < 35)
  → read row group 1  (might contain matches)
  → skip row group 2  (max 22.9 < 35)
```

Combined with Iceberg's manifest-level statistics, a single query can skip entire Parquet files before opening them.

**3. Compression — columns compress far better than rows**

Because all values in a column share the same data type and often similar magnitudes, compression algorithms achieve much higher ratios:

| Format | Typical size for 1 M sensor rows |
|---|---|
| CSV (uncompressed) | ~85 MB |
| CSV + gzip | ~22 MB |
| Parquet (Snappy) | ~8 MB |
| Parquet (ZSTD) | ~5 MB |

Parquet also applies **encoding tricks** per column before compression:
- **Dictionary encoding**: repeated strings like `"sensor_001"` stored once; rows store a small integer index.
- **Delta encoding**: timestamps stored as differences (`+3s, +3s, +3s`) rather than full values.
- **Run-length encoding (RLE)**: `sensor_001` repeated 1 000 times stored as `(sensor_001, 1000)`.

These can reduce size by another 2–5× before the compression codec even runs.

**4. Schema enforcement and evolution**

Parquet files embed a full schema in their footer. Unlike CSV (which is just strings), every value has a declared type. Readers can detect mismatches immediately. And because each column is independent, adding a new nullable column is a backward-compatible change — old files simply return `null` for the new column.

**5. Vectorized reads — modern CPUs love columnar data**

Query engines like DuckDB, Spark, and Trino read Parquet in **column batches** (e.g. 4 096 values at a time) and process them with SIMD CPU instructions — the same operation applied to 8–16 values per clock cycle. This is impossible with row-oriented formats where each row may have a different memory layout.

#### When NOT to use Parquet

Parquet is optimised for **reads**, not individual-row writes. It is a poor choice for:
- High-frequency single-row inserts (use a transactional DB instead)
- Streaming records where you need millisecond read latency on the latest row
- Data that changes in-place (Parquet files are immutable; updates create new files)

Iceberg addresses the last two points: it manages a set of immutable Parquet files as snapshots and uses **merge-on-read** or **copy-on-write** strategies to handle updates and deletes at the table level.

#### How Parquet, Iceberg, and Polaris fit together

```
Polaris          knows WHERE the table is, WHO can access it
  │
Iceberg          knows WHICH Parquet files form the current snapshot
  │              handles schema, partitioning, time-travel, ACID commits
  │
Parquet          the actual bytes on disk — compressed, columnar,
                 self-describing, engine-agnostic
```

Each layer is independently open-source and swappable: Iceberg also supports ORC and Avro data files; Polaris manages any Iceberg table regardless of its data file format.

### 3. REST Catalog (the Iceberg spec)


The **Iceberg REST Catalog spec** (sometimes called the "Iceberg REST API" or "REST catalog protocol") is an open HTTP standard published by the Apache Iceberg community. It defines a language-agnostic contract that any catalog server must implement, and any Iceberg-compatible client (Spark, Flink, PyIceberg, DuckDB, Trino…) can speak — without knowing anything about the catalog's internal implementation.

#### Why it matters

Before this spec existed, every engine had its own proprietary connector:
- Spark needed `SparkSessionCatalog` with Hive-specific configs.
- Trino had its own Glue / Hive connector with different semantics.
- Swapping from Hive Metastore to Glue meant reconfiguring every pipeline.

The REST spec flips this: **one URL, one auth token, every engine works**. If a catalog implements the spec, it is immediately compatible with the entire Iceberg ecosystem.

#### The full API surface

The spec is grouped into five functional areas:

**Authentication**

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/oauth/tokens` | Exchange `client_id` + `client_secret` for a Bearer token (OAuth2 client credentials flow) |

**Configuration**

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/config` | Fetch catalog-level config overrides for this warehouse (e.g. storage endpoint, default properties) |

**Namespace management** (a namespace is like a database / schema)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/{prefix}/namespaces` | List all namespaces |
| `POST` | `/v1/{prefix}/namespaces` | Create a namespace |
| `GET` | `/v1/{prefix}/namespaces/{ns}` | Fetch namespace properties |
| `POST` | `/v1/{prefix}/namespaces/{ns}/properties` | Update namespace properties |
| `DELETE` | `/v1/{prefix}/namespaces/{ns}` | Drop namespace |

**Table management**

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/{prefix}/namespaces/{ns}/tables` | List tables |
| `POST` | `/v1/{prefix}/namespaces/{ns}/tables` | Create a table (returns metadata location) |
| `GET` | `/v1/{prefix}/namespaces/{ns}/tables/{table}` | Load table (schema, snapshots, partition spec) |
| `POST` | `/v1/{prefix}/namespaces/{ns}/tables/{table}` | **Commit a new snapshot** (atomic metadata update) |
| `DELETE` | `/v1/{prefix}/namespaces/{ns}/tables/{table}` | Drop table |
| `POST` | `/v1/{prefix}/tables/rename` | Rename a table |
| `HEAD` | `/v1/{prefix}/namespaces/{ns}/tables/{table}` | Check if table exists |

**Transactions (multi-table)**

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/{prefix}/transactions/commit` | Atomically commit changes across multiple tables in one request |

#### The commit protocol — the most important part

The snapshot commit is the heart of Iceberg's correctness guarantee:

```
Client                                   Polaris (catalog)
  │                                           │
  ├─── POST /v1/{prefix}/{ns}/{table} ───────►│
  │    body: {                                │
  │      "requirements": [                    │
  │        { "type": "assert-current-schema-id",
  │          "current-schema-id": 3 }         │  ← optimistic lock
  │      ],                                   │
  │      "updates": [                         │
  │        { "action": "add-snapshot",        │
  │          "snapshot": { ... } },           │
  │        { "action": "set-snapshot-ref",    │
  │          "ref-name": "main",              │
  │          "snapshot-id": 98765 }           │
  │      ]                                    │
  │    }                                      │
  │                                           │
  │◄─── 200 OK  (or 409 Conflict) ────────────┤
  │                                           │ Polaris atomically
  │                                           │ swaps the metadata
  │                                           │ pointer on success.
```

- The client declares what it **expects** the current state to be (`requirements`).
- If another writer already changed the table, Polaris returns `409 Conflict` — the client retries.
- On success, Polaris atomically writes the new `metadata.json` pointer. No reader ever sees a partial state.

This is how Iceberg achieves **serializable snapshot isolation** on plain object storage.

#### Credential vending

The spec also supports an optional **credential vending** extension. When a client loads a table, the catalog can return short-lived, scoped storage credentials alongside the metadata:

```json
{
  "metadata": { "...": "..." },
  "config": {
    "s3.access-key-id": "ASIA...",
    "s3.secret-access-key": "...",
    "s3.session-token": "...",
    "s3.region": "us-east-1"
  }
}
```

The client uses those credentials only for the files in that table. This means:
- No long-lived service account keys in application configs.
- Per-table access scoping enforced at the storage layer.
- Polaris can revoke access by simply not issuing new credentials.

Polaris implements credential vending for AWS S3, Google Cloud Storage, and Azure Data Lake Storage.

Polaris implements this spec, so **any Iceberg-compatible engine works with it without modification**.

### 4. Polaris-Specific Concepts

Polaris adds a **management layer** on top of the Iceberg REST spec:

```
┌─────────────────────────────────────────────────────┐
│                    Apache Polaris                   │
│                                                     │
│  Management API (/api/management/v1/)               │
│  ┌──────────┐  ┌───────────────┐  ┌─────────────┐  │
│  │ Catalogs │  │  Principals   │  │    Roles    │  │
│  │          │  │  (users/apps) │  │  + Grants   │  │
│  └──────────┘  └───────────────┘  └─────────────┘  │
│                                                     │
│  Iceberg REST API (/api/catalog/v1/)                │
│  ┌──────────────────────────────────────────────┐   │
│  │  Namespaces → Tables → Snapshots → Files     │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

| Concept | Description |
|---|---|
| **Catalog** | A named group of namespaces and tables, backed by a storage location (S3, GCS, Azure, or local file) |
| **Principal** | A service account or user identity that authenticates via OAuth2 client credentials |
| **Principal Role** | A named collection of privileges assigned to one or more principals |
| **Catalog Role** | Fine-grained permissions on a catalog (e.g. `TABLE_READ`, `CATALOG_MANAGE_CONTENT`) |
| **Grant** | Links a principal role to a catalog role, controlling what a principal can do |

### 5. How Authentication Works

```
Client (PyIceberg / Spark / Flink)
  │
  ├─ POST /api/catalog/v1/oauth/tokens
  │    body: grant_type=client_credentials
  │          client_id=my_app
  │          client_secret=my_secret
  │          scope=PRINCIPAL_ROLE:ALL
  │
  │  ← { "access_token": "eyJ...", "expires_in": 3600 }
  │
  └─ GET /api/catalog/v1/config?warehouse=streaming_demo
       Authorization: Bearer eyJ...
     ← catalog config (storage location, overrides)

  All subsequent table operations use the Bearer token.
```

### 6. How a Write Works End-to-End

```
Producer (Python / PyIceberg)
  │
  ├─ 1. Authenticate → get Bearer token from Polaris
  ├─ 2. Load table metadata → Polaris returns metadata.json location
  ├─ 3. Write Parquet files → directly to storage (S3 / local file)
  ├─ 4. Build new snapshot manifest (list of data files)
  └─ 5. Commit snapshot → POST to Polaris REST API
           Polaris atomically updates the metadata pointer
           (no partial reads possible for concurrent readers)
```

This is why Iceberg is "ACID on the data lake" — the commit step is atomic.

---

## Project Architecture

```
┌──────────────────────────────────────────────────────┐
│  This Demo                                           │
│                                                      │
│  producer.py                                         │
│    │  generates 20 fake sensor rows every 3 s        │
│    │  appends them as a new Iceberg snapshot          │
│    ▼                                                 │
│  Apache Polaris  (Docker, port 8181)                 │
│    │  manages catalog metadata                       │
│    │  enforces access control                        │
│    ▼                                                 │
│  /tmp/polaris-warehouse/  (local filesystem)         │
│    │  Parquet data files                             │
│    │  Iceberg metadata JSON                          │
│    ▼                                                 │
│  reader.py                                           │
│    reads the table via Polaris REST → PyIceberg      │
│    prints stats and latest rows                      │
└──────────────────────────────────────────────────────┘
```

---

## Prerequisites

| Tool | Install |
|---|---|
| Docker + Docker Compose | [docs.docker.com](https://docs.docker.com/get-docker/) |
| Python 3.11+ | `sudo apt install python3.11` or pyenv |
| uv | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |

---

## Quick Start

```bash
# 1. Clone / enter the project
cd data-polaris

# 2. Run the full demo (Docker + setup + produce + read)
./run_demo.sh
```

### Step-by-Step (manual)

```bash
# Start Polaris catalog server
docker compose up -d

# Install Python dependencies
uv sync

# Bootstrap: create catalog, namespace, and table in Polaris
uv run polaris-setup

# Terminal A — start the streaming producer
uv run polaris-produce

# Terminal B — read the table (single snapshot)
uv run polaris-read

# Terminal B — or watch it update live
uv run polaris-read --watch
```

---

## Configuration

All settings live in [src/data_polaris/config.py](src/data_polaris/config.py) and can be overridden with environment variables:

| Variable | Default | Description |
|---|---|---|
| `POLARIS_URL` | `http://localhost:8181` | Polaris server URL |
| `POLARIS_CLIENT_ID` | `root` | OAuth2 client ID |
| `POLARIS_CLIENT_SECRET` | `polaris-secret` | OAuth2 client secret |

The root credentials are pre-configured in `docker-compose.yml` via `POLARIS_BOOTSTRAP_CREDENTIALS`. If your Polaris version generates random credentials instead, extract them with:

```bash
docker logs polaris 2>&1 | grep -i secret
```

Then export them before running the scripts:

```bash
export POLARIS_CLIENT_SECRET=<value-from-logs>
uv run polaris-setup
```

---

## Table Schema

The demo writes IoT sensor readings with this Iceberg schema:

| Column | Type | Description |
|---|---|---|
| `sensor_id` | `string` | Device identifier (e.g. `sensor_007`) |
| `ts` | `timestamptz` | Reading timestamp (UTC) |
| `temperature` | `double` | Temperature in °C |
| `humidity` | `double` | Relative humidity % |
| `pressure` | `double` | Atmospheric pressure in hPa |
| `batch_id` | `long` | Monotonically increasing batch counter |

---

## How the Code Maps to Polaris Concepts

| Script | What it does in Polaris |
|---|---|
| `setup_polaris.py` | Gets OAuth token → creates `INTERNAL` catalog → creates namespace + table via PyIceberg REST |
| `producer.py` | Loads table via Polaris REST → appends PyArrow record batches → Polaris commits new Iceberg snapshot |
| `reader.py` | Loads table via Polaris REST → full table scan → aggregates with PyArrow compute |

---

## Polaris vs. Other Catalogs

| Feature | Polaris | Hive Metastore | AWS Glue | Project Nessie |
|---|---|---|---|---|
| Iceberg REST spec | ✅ native | ❌ needs bridge | ✅ | ✅ |
| Fine-grained RBAC | ✅ built-in | ❌ | limited | ❌ |
| Multi-engine | ✅ | ✅ (via HMS) | ✅ | ✅ |
| Open source | ✅ Apache | ✅ Apache | ❌ AWS only | ✅ |
| Credential vending | ✅ (S3/GCS/Azure) | ❌ | ❌ | ❌ |
| Git-like branching | ❌ | ❌ | ❌ | ✅ |

---

## Next Steps

- **Add partitioning**: partition the `readings` table by day using `PartitionSpec` in PyIceberg
- **Add access control**: create a read-only principal for the reader and a write-only principal for the producer
- **Use real storage**: swap the FILE backend for MinIO (S3-compatible) by changing `storageType` to `S3` and adding MinIO to `docker-compose.yml`
- **Query with DuckDB**: `SELECT * FROM iceberg_scan('/tmp/polaris-warehouse/streaming_demo/...')`
- **Connect Spark**: configure `spark.sql.catalog.polaris` to point at `http://localhost:8181/api/catalog`

---

## References

- [Apache Polaris (incubating)](https://polaris.apache.org/)
- [Apache Iceberg docs](https://iceberg.apache.org/docs/latest/)
- [Iceberg REST Catalog spec](https://iceberg.apache.org/docs/latest/rest-catalog/)
- [PyIceberg docs](https://py.iceberg.apache.org/)
