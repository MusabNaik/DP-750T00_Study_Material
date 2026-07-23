# Learning Path 3: Prepare and Process Data

**Exam Weight:** ~25-30% of DP-750  
**Focus:** Ingesting, cleaning, transforming, modeling, and enforcing quality on data using Azure Databricks with Unity Catalog and Delta Lake.  
**Core Services:** Azure Databricks, Unity Catalog, Delta Lake, Lakeflow Connect, Auto Loader, Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables / DLT)

---

## 3.1 Design and Implement Data Modeling

### 3.1.1 Table Formats in Unity Catalog

Azure Databricks and Unity Catalog support multiple open table formats. Understanding their differences and trade-offs is critical.

#### Delta Lake (Default & Dominant)

**Delta Lake** is the default format on Azure Databricks. It provides **ACID transactions**, **time travel**, **schema enforcement/evolution**, and **scalable metadata handling** via a transaction log stored as Parquet + JSON.

```sql
-- Creating a Delta table in Unity Catalog
CREATE TABLE main.dp750.sales (
  sale_id      BIGINT,
  customer_id  BIGINT,
  product_name STRING,
  amount       DECIMAL(10,2),
  sale_date    DATE
)
USING DELTA
LOCATION 'abfss://container@storage.dfs.core.windows.net/sales';
```

Key features:
- **ACID transactions** — concurrent readers and writers never see partial writes
- **Time travel** — query snapshots via `VERSION AS OF` or `TIMESTAMP AS OF`
- **Schema evolution** — `ALTER TABLE ... ADD COLUMNS` or `mergeSchema` option
- **Change Data Feed (CDF)** — row-level change tracking
- **Generated columns** — automatically populate columns from expressions
- **Liquid Clustering** — adaptive data layout (introduced in Delta 3.0)

#### Apache Iceberg

**Apache Iceberg** is an open table format optimized for massive analytic tables. It has a **hierarchical metadata layer** (metadata file → manifest list → manifest files → data files) that makes partition pruning exceptionally fast.

```sql
-- Creating an Iceberg table in Unity Catalog
CREATE TABLE main.dp750.sales_iceberg (
  sale_id      BIGINT,
  customer_id  BIGINT,
  amount       DECIMAL(10,2),
  sale_date    DATE
)
USING ICEBERG
PARTITIONED BY (days(sale_date));
```

Key features:
- **Hidden partitioning** — partitions are computed automatically (e.g., `days(sale_date)`)
- **Expressive partitioning** — partition transforms like `bucket(N, col)`, `truncate(L, col)`
- **Catalog-native** — table metadata lives in the metastore, not just file system
- **V2 row-level deletes** — supports `MERGE`, `DELETE`, `UPDATE` with positional delete files

#### Apache Hudi

**Apache Hudi** (Hadoop Upserts Deletes and Incrementals) is designed for **near-real-time ingestion** with upserts and incremental pull.

```sql
-- Creating a Hudi table in Unity Catalog
CREATE TABLE main.dp750.sales_hudi (
  sale_id      BIGINT,
  customer_id  BIGINT,
  amount       DECIMAL(10,2),
  sale_date    DATE
)
USING HUDI
OPTIONS (
  primaryKey = 'sale_id',
  preCombineField = 'sale_date',
  type = 'cow'   -- Copy-on-Write (cow) or Merge-on-Read (mor)
);
```

Key features:
- **Primary keys** — built-in UPSERT support
- **Incremental queries** — `beginInstantTime` / `endInstantTime` to pull changes
- **Two storage types**: Copy-on-Write (COW) and Merge-on-Read (MOR)

#### Delta UniForm (Universal Format) — Deep Dive

**Delta UniForm** makes a single Delta table readable as Iceberg or Hudi **without copying data**. It produces Iceberg and/or Hudi metadata alongside Delta metadata from the same Parquet files.

##### What is UniForm?

UniForm (Universal Format) is a Delta Lake feature that allows Delta tables to be read by Iceberg and Hudi clients **without duplicating data**. It writes Iceberg/Hudi metadata alongside Delta metadata. The underlying Parquet data files are shared — only the metadata is different per format.

##### How It Works

- When UniForm is enabled, Delta writes additional metadata files in Iceberg format
- The underlying Parquet data files remain the same — **no data duplication**
- Iceberg readers see the table as a native Iceberg table via the generated Iceberg metadata
- Delta readers continue to see it as a Delta table via the standard Delta transaction log
- The Iceberg metadata is generated asynchronously after each Delta commit

##### Enabling UniForm

```sql
-- Enable UniForm for Iceberg on a new table
CREATE TABLE main.dp750.sales (
  sale_id      BIGINT,
  customer_id  BIGINT,
  amount       DECIMAL(10,2)
) USING DELTA
TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- Enable UniForm on an existing table
ALTER TABLE main.dp750.sales
SET TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- Enable UniForm for both Iceberg and Hudi
CREATE TABLE main.dp750.sales_multi
USING DELTA
TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'iceberg,hudi'
);
```

##### Prerequisites

- Delta Lake 3.0+ (Databricks Runtime 13.3 LTS+)
- The writer must use the Delta Lake writer, not direct Parquet writes to the table location
- An Iceberg or Hudi catalog must be configured separately on the reader side — UniForm only generates the metadata, it does not register the table in a foreign catalog
- The underlying storage must be accessible to both Delta and Iceberg/Hudi readers

##### When to Use UniForm

- Your organization uses both Delta and Iceberg engines (e.g., Databricks for ETL, Trino/Athena for ad-hoc queries)
- You need to share Delta data with teams using Iceberg-native tools
- You are migrating from a system that uses Iceberg and need a transition period
- You want a single copy of data serving multiple query engines

##### When NOT to Use UniForm

- All consumers already use Delta Lake (UniForm adds unnecessary metadata overhead)
- Storage costs are the primary concern (the additional metadata adds ~5-10% overhead on metadata operations)
- You need full Delta feature compatibility from the Iceberg reader side (not all Delta features are exposed)

##### Limitations of UniForm

- **Not all Delta features are compatible** with Iceberg readers — Delta-specific constraints, generated columns, CDF reader are not available through the Iceberg metadata
- **Asynchronous metadata generation** — UniForm writes Iceberg metadata asynchronously after each Delta commit; there may be a slight delay before Iceberg readers see the latest data
- **Iceberg reader view** — Iceberg readers see only the Iceberg-compatible portion of the table metadata; any Delta-only features are invisible
- **Write path is Delta-only** — you cannot write to the table through Iceberg; UniForm is a read-compatibility feature only
- **No automatic catalog registration** — UniForm generates metadata files but does not register the table in Iceberg/Hudi catalogs; this must be done separately

💡 **Exam Tip:** UniForm does NOT duplicate data — only metadata. The Parquet data files are shared. The exam may ask what happens to storage costs when UniForm is enabled: metadata storage increases slightly, data storage stays the same. Also key: UniForm is **read-only** from the Iceberg/Hudi side — you cannot write to the table via Iceberg.

### 3.1.2 Partitioning vs Liquid Clustering vs Z-Ordering

Choosing the right data organization strategy directly impacts query performance.

#### Partitioning

**Static directory-based layout.** Data is split into directories based on column values.

```sql
CREATE TABLE main.dp750.sales_partitioned (
  sale_id      BIGINT,
  customer_id  BIGINT,
  amount       DECIMAL(10,2),
  sale_date    DATE
)
USING DELTA
PARTITIONED BY (sale_date);
```

**Pros:** Simple, well-understood, excellent for filtering on the partition column.  
**Cons:** Can produce too many small files (data skew), requires knowing partition columns ahead of time, cannot change partitioning without rewriting the table.

⚠️ **Warning:** Partitioning on high-cardinality columns (e.g., `customer_id` with millions of unique values) creates thousands of directories and kills performance.

#### Liquid Clustering (Delta 3.0+)

**Adaptive, incremental data layout.** No directories — data files are clustered based on multiple columns, and the clustering is maintained incrementally on write.

```sql
CREATE TABLE main.dp750.sales_clustered (
  sale_id      BIGINT,
  customer_id  BIGINT,
  region       STRING,
  amount       DECIMAL(10,2),
  sale_date    DATE
)
USING DELTA
CLUSTER BY (region, sale_date);
```

**Pros:**
- **Adaptive** — layout evolves as data grows
- **Incremental** — only rewrites affected files during `OPTIMIZE`
- **Multi-column** — you can cluster on up to 4 columns
- **No data skew** — avoids the small-files problem of partitioning
- **Compatible with Z-order-like pruning** — the optimizer tracks data ranges per file

**Cons:** Slightly more overhead on write compared to naive partitioning. Requires periodic `OPTIMIZE` runs to re-cluster.

```python
# In Python, run OPTIMIZE to maintain liquid clustering
spark.sql("OPTIMIZE main.dp750.sales_clustered")
```

#### Z-Ordering

**Interleaved column ordering within file statistics.** A coarser clustering approach that reduces data scan for queries filtering on multiple columns.

```sql
OPTIMIZE main.dp750.sales
ZORDER BY (customer_id, product_name);
```

**Pros:** Good for multi-column filter queries.  
**Cons:** Not incremental — full `OPTIMIZE` rewrites large portions of the table, expensive for large datasets.

---

#### Decision Matrix

| Approach | Best For | Avoid If | Maintenance |
|---|---|---|---|
| **Partitioning** | Date-partitioned tables, low-cardinality filters | High-cardinality or multi-column filters | Zero (manual rewrite needed to change) |
| **Liquid Clustering** | General-purpose, multi-column filters, streaming workloads | Extreme need for partition pruning on a single column | Periodic `OPTIMIZE` |
| **Z-Ordering** | Ad-hoc queries on multiple high-cardinality columns | Tables that update frequently | Full `OPTIMIZE` is expensive |

💡 **Exam Tip:** Liquid clustering is the **recommended default** for Delta 3.0+ tables in DP-750. Partitioning is legacy but still tested. Z-Ordering is for cases where you need multi-column pruning on tables that don't update much.

🔬 **Lab Reference (Lab 06 — Northbank Financial):** In the data modeling lab, you create a Delta Lake model using **liquid clustering** on columns like `customer_id` and `account_open_date`, then run `OPTIMIZE` to maintain the cluster.

---

### 3.1.3 Slowly Changing Dimensions (SCD Types)

Dimensional modeling uses SCD strategies to track historical changes. **SCD Type 2** is the most heavily tested.

#### SCD Type 1 — Overwrite

Simply overwrite old values with new ones. No history is preserved.

```sql
MERGE INTO main.dp750.dim_customer_s1 AS target
USING updates AS source
  ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

#### SCD Type 2 — Add New Row (Track History)

Preserve full history by inserting a **new version** of the row with date-range markers. Classic implementation:

```sql
CREATE OR REPLACE TABLE main.dp750.dim_customer_scd2 (
  customer_id      BIGINT,
  customer_name    STRING,
  email            STRING,
  loyalty_tier     STRING,
  effective_date   DATE,
  end_date         DATE,
  is_current       BOOLEAN
)
USING DELTA
CLUSTER BY (customer_id);

-- SCD Type 2 merge
MERGE INTO main.dp750.dim_customer_scd2 AS target
USING (
  SELECT customer_id, customer_name, email, loyalty_tier, CURRENT_DATE AS effective_date
  FROM source_updates
) AS source
ON target.customer_id = source.customer_id AND target.is_current = true

-- If attributes changed, expire the old row and insert a new one
WHEN MATCHED AND (
  target.customer_name   != source.customer_name OR
  target.email           != source.email OR
  target.loyalty_tier    != source.loyalty_tier
) THEN UPDATE SET
  target.end_date   = source.effective_date - INTERVAL 1 DAY,
  target.is_current = false

WHEN NOT MATCHED THEN INSERT (
  customer_id, customer_name, email, loyalty_tier,
  effective_date, end_date, is_current
) VALUES (
  source.customer_id, source.customer_name, source.email, source.loyalty_tier,
  source.effective_date, null, true
);
```

**Querying point-in-time state:**

```sql
-- What did customer 123 look like on Jan 15, 2025?
SELECT * FROM main.dp750.dim_customer_scd2
WHERE customer_id = 123
  AND effective_date <= '2025-01-15'
  AND (end_date IS NULL OR end_date >= '2025-01-15');
```

#### SCD Type 3 — Add New Column

Store only the **previous and current** values in separate columns. Limited history — only one change tracked.

```sql
ALTER TABLE main.dp750.dim_customer_scd3 ADD COLUMNS (
  previous_loyalty_tier STRING,
  loyalty_tier_change_date DATE
);
```

🔬 **Lab Reference (Lab 06 — Northbank Financial):** You implement **SCD Type 2** on the `customer` dimension using `MERGE`. The lab walks through expiring old rows and inserting new versions with effective dates.

💡 **Exam Tip:** Know the SCD Type 2 `MERGE` pattern cold. The exam often presents scenarios asking: "You need to track historical changes to `email` and `loyalty_tier` for audit compliance" — that's always **SCD Type 2**.

---

### 3.1.4 Temporal Tables with `FOR SYSTEM_TIME AS OF`

Delta Lake's **time travel** lets you query the table as it existed at a specific point in time. This is powered by the Delta transaction log.

```sql
-- Query as of a specific version number
SELECT * FROM main.dp750.sales VERSION AS OF 42
WHERE customer_id = 123;

-- Query as of a specific timestamp
SELECT * FROM main.dp750.sales
TIMESTAMP AS OF '2025-01-15T10:30:00.000Z'
WHERE customer_id = 123;

-- Query using relative timestamp (Spark SQL)
SELECT * FROM main.dp750.sales
TIMESTAMP AS OF (CURRENT_TIMESTAMP - INTERVAL 1 DAY)
WHERE customer_id = 123;
```

**Temporal tables in Lakeview Dashboards** also allow point-in-time analysis using the `AS OF` syntax in the SQL editor.

```python
# Reading historical data in PySpark
df = (
  spark.read
    .format("delta")
    .option("timestampAsOf", "2025-01-15")
    .table("main.dp750.sales")
)
```

💡 **Exam Tip:** Time travel works only for the last **30 days by default** (configurable via `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration`). Vacuum operations permanently remove old files, making snapshots before the vacuum unavailable.

---

### 3.1.5 Change Data Feed (CDF)

**Change Data Feed** (CDF) provides row-level change tracking for Delta tables. When enabled, every `INSERT`, `UPDATE`, `DELETE`, and `MERGE` operation records which rows changed and how.

```sql
-- Enable CDF on a table
ALTER TABLE main.dp750.sales SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = true
);

-- Query changes between two versions
SELECT * FROM table_changes('main.dp750.sales', 10, 20);

-- Query changes between two timestamps
SELECT * FROM table_changes(
  'main.dp750.sales',
  '2025-01-15T00:00:00.000Z',
  '2025-01-16T00:00:00.000Z'
);
```

The CDF output includes these metadata columns:
- `_change_type` — `insert`, `update_preimage`, `update_postimage`, `delete`
- `_commit_version` — the Delta version that produced this change
- `_commit_timestamp` — when the change was committed

**Common use cases:**
- **Audit trails** — track who changed what and when
- **Incremental ETL** — process only changed rows since the last run
- **Streaming downstream** — feed changes into a streaming sink

```python
# Read CDF as a streaming source
df_changes = (
  spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", "10")
    .table("main.dp750.sales")
)
```

🔬 **Lab Reference (Lab 06 — Northbank Financial):** You enable CDF on the `customer` table and query `table_changes()` to produce an **audit trail** for regulatory compliance.

💡 **Exam Tip:** CDF is the recommended way to do **incremental processing** — it's more reliable than comparing `last_updated` timestamps because it captures **all operations** including physical deletes.

---

### 3.1.6 Managed vs External Tables (Revisited)

| Aspect | Managed Table | External Table |
|---|---|---|
| **Data location** | Unity Catalog manages the storage location | You specify `LOCATION` explicitly |
| **DROP behavior** | Drops both metadata AND data | Drops only metadata, data remains |
| **Lifecycle** | Tied to Unity Catalog | Independent — data can outlive the table |
| **When to use** | Most workloads | Data in existing ADLS/Blob locations, data shared across systems |

```sql
-- Managed table (no LOCATION specified)
CREATE TABLE main.dp750.managed_sales (
  sale_id BIGINT, amount DECIMAL(10,2)
) USING DELTA;

-- External table (explicit LOCATION)
CREATE TABLE main.dp750.external_sales (
  sale_id BIGINT, amount DECIMAL(10,2)
) USING DELTA
LOCATION 'abfss://bronze@storage.dfs.core.windows.net/sales';
```

⚠️ **Warning:** Dropping a managed table **deletes the underlying data files**. Use `DROP TABLE` with `PURGE` on external tables to remove data manually, or just drop the metadata.

#### Adding Descriptive Metadata with COMMENT ON

In Unity Catalog, you can add descriptive metadata to tables and columns for data discovery. This is especially useful in the medallion architecture where many teams share access to the same tables.

```sql
-- Add a table-level description
COMMENT ON TABLE main.dp750.sales IS
  'Daily sales transactions from the retail platform. Bronze layer — raw data.';

-- Add column-level descriptions
COMMENT ON COLUMN main.dp750.sales.amount IS
  'Transaction amount in USD, excluding taxes';
COMMENT ON COLUMN main.dp750.sales.customer_id IS
  'Unique customer identifier from the CRM system';

-- View comments in table metadata
DESCRIBE EXTENDED main.dp750.sales;
```

💡 **Exam Tip:** `COMMENT ON` is tested alongside `DESCRIBE DETAIL` and `SHOW TBLPROPERTIES` as part of Unity Catalog data discovery. The exam may present a scenario where you need to document a table for your team — `COMMENT ON TABLE` is the answer.

---

### 3.1.7 CLONE Operations in Detail

Delta Lake provides two types of `CLONE` operations for creating copies of tables. Understanding the difference is critical for exam scenarios involving data migration, sandbox creation, and external-to-managed table conversion.

#### DEEP CLONE

**DEEP CLONE** copies all data files to the new table location. The cloned table is fully independent — changes to the source do not affect the clone.

```sql
-- Deep clone a table
CREATE OR REPLACE TABLE main.dp750.sales_deep_clone
DEEP CLONE main.dp750.sales;

-- Deep clone with specific location
CREATE OR REPLACE TABLE main.dp750.sales_deep_clone
DEEP CLONE main.dp750.sales
LOCATION 'abfss://silver@storage.dfs.core.windows.net/sales_clone';
```

**Characteristics:**
- Copies all data files to the new table location
- The cloned table is **fully independent** — deletions or modifications to the source table do not affect the clone
- Preserves table schema, partitioning, and table properties
- Does **NOT** preserve time travel history — the clone starts at version 0
- Cost: incurs full data copy, can be expensive for large tables
- Best for: production-grade copies, test environments that need real data, migration between storage locations

#### SHALLOW CLONE

**SHALLOW CLONE** creates a metadata-only copy. The data files are shared with the source table.

```sql
-- Shallow clone a table
CREATE OR REPLACE TABLE main.dp750.sales_shallow
SHALLOW CLONE main.dp750.sales;
```

**Characteristics:**
- Creates a metadata-only copy — data files are shared with the source
- Cloned table **depends on source files remaining accessible**
- Much faster than deep clone (minutes vs hours for large tables)
- Preserves schema and partitioning
- Does **NOT** preserve table properties — you need to re-add them explicitly
- The clone and source share the same underlying Parquet files
- ⚠️ **Warning:** If the source table runs `VACUUM`, the clone's files could be deleted if they're no longer referenced in the source's Delta log. Use shallow clones with caution in production.
- Best for: quick dev copies, sandbox testing, transient environments

#### Deep Clone vs Shallow Clone — Comparison

| Aspect | Deep Clone | Shallow Clone |
|--------|-----------|--------------|
| **Data Copy** | Yes — all files copied | No — references original files |
| **Independence** | Fully independent | Dependent on source files |
| **Speed** | Slow (data volume proportional) | Fast (metadata only) |
| **Table Properties Preserved** | ✅ | ❌ |
| **Time Travel History** | ❌ (starts at version 0) | ❌ |
| **Use Case** | Test environments, archival | Quick dev copies, sandbox testing |
| **VACUUM Safe** | ✅ | ⚠️ Risk of losing shared files |

💡 **Exam Tip:** Know the difference: DEEP CLONE = full independent copy (slower, costs more storage). SHALLOW CLONE = metadata-only (fast, shares files). Use shallow clone for quick sandbox environments that can be discarded. The exam may present a scenario: "You need to create a copy of a production Delta table for a data scientist to experiment with, minimizing storage costs" → **SHALLOW CLONE**.

🔬 **Lab Reference (Lab 06 — Northbank Financial):** You use **DEEP CLONE** in the lab to create a development copy of the customer dimension table from production.

---

### 3.1.8 Converting Between Managed and External Tables

A common scenario on the exam: you have an external table pointing to ADLS and need to convert it to a managed table in Unity Catalog while preserving Iceberg read compatibility.

**Key Challenge:** External tables cannot be "converted" in-place. You must create a new managed table and move the data. Additionally, table properties like UniForm are **not inherited** — they must be explicitly set on the new table.

#### Option 1: CREATE TABLE AS SELECT (CTAS) with UniForm

```sql
-- Step 1: Create a managed table from the external table with UniForm enabled
CREATE TABLE main.dp750.sales_managed
USING DELTA
TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS SELECT * FROM main.dp750.sales_external;

-- Step 2: Drop the external table metadata (data stays in ADLS)
DROP TABLE main.dp750.sales_external;

-- Step 3: Rename the managed table to the original name
ALTER TABLE main.dp750.sales_managed RENAME TO main.dp750.sales;
```

**Pros:** Simple, straightforward.  \
**Cons:** Does not preserve table properties from the original. No time travel history.

#### Option 2: DEEP CLONE (preserves structure and properties)

```sql
-- Deep clone copies data to managed storage and preserves structure
CREATE OR REPLACE TABLE main.dp750.sales_managed
DEEP CLONE main.dp750.sales_external;

-- Enable UniForm on the cloned table (NOT inherited from source)
ALTER TABLE main.dp750.sales_managed
SET TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- Drop the external table metadata
DROP TABLE main.dp750.sales_external;

-- Rename
ALTER TABLE main.dp750.sales_managed RENAME TO main.dp750.sales;
```

**Pros:** Preserves table schema, partitioning, and table properties (except time travel history).  \
**Cons:** Still requires full data copy. Must re-enable UniForm explicitly.

#### Option 3: Shallow CLONE (metadata-only, fast but dependent)

```sql
-- Shallow clone references the original data files
CREATE OR REPLACE TABLE main.dp750.sales_shallow
SHALLOW CLONE main.dp750.sales_external;
```

**Use with caution:** Shallow clones depend on the source files remaining accessible. This is not a true "conversion" — it's more of a temporary bridge.

#### Important Considerations for External → Managed Conversion

1. **UniForm must be explicitly set** on the new table — it is not inherited from the original external table
2. **Time travel history is NOT preserved** unless you use DEEP CLONE (and even then, only the current state at time of clone)
3. **The original data files may need to be moved** to managed storage (or left in place — depends on workspace configuration)
4. **DROP TABLE on the external table** removes only the metadata, leaving data files intact (assuming `PURGE` is not used)
5. **RENAME** is a metadata-only operation in Unity Catalog — it does not move data files

💡 **Exam Tip:** The exam may present a scenario: "You have an external Delta table with UniForm enabled for Iceberg. You need to convert it to a managed table while keeping Iceberg read compatibility." The answer is: CREATE a new managed table with UniForm, copy data via AS SELECT or DEEP CLONE, then DROP the external table and RENAME. Remember that UniForm must be **explicitly enabled** on the new table — it is never inherited.

---

## 3.2 Ingest Data into Unity Catalog

### 3.2.0 Choosing Between Batch and Streaming Ingestion

One of the first decisions in any ingestion pipeline is whether to use **batch** or **streaming** processing. The DP-750 exam tests your ability to choose the right approach for different scenarios.

#### Batch Ingestion

Processes data in discrete, scheduled intervals (e.g., hourly, daily).

**When to use batch:**
- Source is files in cloud storage (ADLS, Blob, S3)
- Latency of minutes to hours is acceptable
- You need cost efficiency (batch clusters can be started, do work, and shut down)
- Data volumes are large files (MBs to GBs) delivered in batches
- Simpler to implement, debug, and test

**Common batch tools on Azure Databricks:**
- `COPY INTO` — idempotent file loading
- `CTAS` — create and populate tables in one step
- `MERGE` — incremental upserts
- Auto Loader in **Trigger.AvailableNow** mode (processes all available files, then stops)

#### Streaming Ingestion

Processes data continuously as it arrives, with sub-minute latency.

**When to use streaming:**
- Source is a message queue or event stream (Kafka, Event Hubs, IoT Hub)
- You need sub-minute latency (real-time dashboards, alerts)
- Source is a CDC feed from a database
- Data arrives as small events (KB-scale) at high frequency
- You need continuous processing with exactly-once semantics

**Common streaming tools on Azure Databricks:**
- **Structured Streaming** — Kafka, Event Hubs, file sources
- **Auto Loader** — incremental file ingestion (acts like streaming but reads files)
- **AUTO CDC API** — simplified CDC from databases
- **Lakeflow Spark Declarative Pipelines** with `STREAMING TABLE`

#### Decision Framework

| Decision Factor | Batch | Streaming |
|----------------|-------|----------|
| **Latency requirement** | Minutes to hours | Seconds to minutes |
| **Data volume per event** | Large files (MBs-GBs) | Small events (KB) |
| **Source type** | Files in cloud storage | Event streams, IoT, CDC |
| **Compute cost** | Lower per GB processed (start/stop) | Higher (always-on cluster) |
| **Complexity** | Simpler to implement and debug | More complex (watermarks, state management, checkpointing) |
| **Exactly-once guarantees** | Easier (idempotent writes, dedup) | Checkpoint-based |
| **Testing** | Easy — deterministic with same input | Harder — time-sensitive, state-dependent |

#### Hybrid Approaches

- **Auto Loader with Trigger.AvailableNow** — acts like batch but reads incrementally from file storage
- **Structured Streaming with Trigger.Once** — processes available data as a single micro-batch, then stops
- **Lakeflow Spark Declarative Pipelines** — supports both `LIVE TABLE` (batch refresh) and `STREAMING TABLE` (continuous) in the same pipeline

💡 **Exam Tip:** The exam presents scenarios and asks whether batch or streaming is appropriate. Key clues: "files arrive hourly in ADLS" → **batch** or **Auto Loader**. "Stock trades need sub-second latency from Kafka" → **streaming**. "CDC feed from SQL Server" → **streaming** (AUTO CDC or Structured Streaming). If the scenario mentions cost optimization as a priority, lean toward **batch** with scheduled clusters.

### 3.2.1 Lakeflow Connect (Managed Connectors)

**Lakeflow Connect** provides pre-built connectors for common data sources — no custom code needed.

**Supported sources include:**
- Azure SQL Database / Azure Synapse
- Azure Cosmos DB
- Azure Event Hubs / Kafka
- Azure Blob Storage / ADLS Gen2
- Salesforce, SAP, Google BigQuery, Snowflake, and more

**Setup flow:**

1. In your Azure Databricks workspace, navigate to **Lakeflow** > **Connections**
2. Click **Create Connection** and choose the source type
3. Enter connection details (server, database, credentials via Secret Scope)
4. Click **Create** to test the connection

**Then create a pipeline to ingest:**

```sql
-- Lakeflow Connect uses declarative SQL pipelines
-- (Example for Azure SQL Database)
CREATE OR REFRESH STREAMING TABLE main.dp750.bronze_orders
AS SELECT * FROM lakeflow.azuresql.orders_db;
```

💡 **Exam Tip:** Lakeflow Connect is the **lowest-code** ingestion path. It's ideal when the source is a supported system and you need a quick setup. For unsupported sources or custom transformations, use Auto Loader or Structured Streaming.

---

### 3.2.2 COPY INTO (Idempotent File Loading)

`COPY INTO` loads files from cloud storage **exactly once** — it tracks which files have already been processed using the Delta transaction log. This makes it **idempotent**.

```sql
-- Create target table
CREATE OR REPLACE TABLE main.dp750.raw_sales (
  sale_id      BIGINT,
  customer_id  BIGINT,
  amount       DECIMAL(10,2),
  sale_date    DATE,
  filename     STRING
)
USING DELTA;

-- COPY INTO with file deduplication
COPY INTO main.dp750.raw_sales
FROM 'abfss://landing@storage.dfs.core.windows.net/sales/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  'inferSchema' = 'false',
  'delimiter' = ','
)
COPY_OPTIONS (
  'mergeSchema' = 'true',
  'force' = 'false'          -- skip already-loaded files
)
-- Validate data before loading
VALIDATE 10 ROWS;
```

**Key COPY_OPTIONS:**
- `force` — set to `'true'` to reload files (breaks idempotency)
- `mergeSchema` — automatically add new columns from the source
- `pattern` — filter files by glob pattern (e.g., `'*.csv'`)

**Python equivalent:**

```python
spark.sql("""
  COPY INTO main.dp750.raw_sales
  FROM 'abfss://landing@storage.dfs.core.windows.net/sales/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header' = 'true')
""")
```

🔬 **Lab Reference (Lab 07 — Solaris Energy):** You use `COPY INTO` with **deduplication** to load CSV files from ADLS, ensuring the same file can be retried without creating duplicate records.

💡 **Exam Tip:** `COPY INTO` is **idempotent** — this is its #1 advantage. The exam tests: "Which loading approach ensures that rerunning a failed pipeline doesn't create duplicates?" Answer: **COPY INTO** (or Auto Loader with `cloudFiles.allowOverwrites=false`).

---

### 3.2.3 CTAS (CREATE TABLE AS SELECT)

**CTAS** creates a table and populates it with query results in a single atomic operation.

```sql
-- Simple CTAS — summary table
CREATE OR REPLACE TABLE main.dp750.daily_sales_summary
USING DELTA
CLUSTER BY (sale_date)
AS
SELECT
  sale_date,
  COUNT(*) AS order_count,
  SUM(amount) AS total_revenue,
  AVG(amount) AS avg_order_value
FROM main.dp750.raw_sales
GROUP BY sale_date;

-- CTAS with transformation
CREATE OR REPLACE TABLE main.dp750.clean_sales
USING DELTA
LOCATION 'abfss://silver@storage.dfs.core.windows.net/sales'
AS
SELECT DISTINCT                  -- deduplication
  sale_id,
  customer_id,
  COALESCE(amount, 0) AS amount, -- handle nulls
  DATE(sale_date) AS sale_date,
  CURRENT_TIMESTAMP() AS ingested_at
FROM main.dp750.raw_sales
WHERE sale_id IS NOT NULL;       -- filter invalid rows
```

**Best practices for CTAS in medallion architecture:**
- **Bronze → Silver:** CTAS with deduplication, null handling, type casting
- **Silver → Gold:** CTAS with aggregation, joins, business logic
- Use `CLUSTER BY` or `PARTITIONED BY` to optimize the output table for query patterns

🔬 **Lab Reference (Lab 07 — Solaris Energy):** You build **summary tables with CTAS**, aggregating solar energy production data by region and date.

---

### 3.2.4 MERGE (Upsert)

`MERGE` performs a combination of `INSERT`, `UPDATE`, and `DELETE` in a single atomic operation.

```sql
-- Basic upsert
MERGE INTO main.dp750.dim_customer AS target
USING main.dp750.stage_customer_updates AS source
ON target.customer_id = source.customer_id

-- Update existing records
WHEN MATCHED THEN UPDATE SET
  target.email        = source.email,
  target.loyalty_tier = source.loyalty_tier,
  target.updated_at   = CURRENT_TIMESTAMP()

-- Insert new records
WHEN NOT MATCHED THEN INSERT (
  customer_id, customer_name, email, loyalty_tier, created_at, updated_at
) VALUES (
  source.customer_id, source.customer_name, source.email,
  source.loyalty_tier, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
)

-- Delete records no longer in source (optional)
WHEN NOT MATCHED BY SOURCE THEN DELETE;
```

**MERGE with schema evolution:**

```sql
MERGE INTO main.dp750.sales AS target
USING source_updates AS source
ON target.sale_id = source.sale_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
-- Enables automatic schema evolution
```

⚠️ **Warning:** `WHEN NOT MATCHED BY SOURCE` is powerful but dangerous — it can **delete all data** if the source is empty. Use it with caution.

💡 **Exam Tip:** The `MERGE` command is the foundation of **SCD Type 2**, **upsert patterns**, and **CDF-based incremental loads**. Expect at least one scenario question about MERGE conditions.

---

### 3.2.5 Auto Loader

**Auto Loader** incrementally and idempotently ingests new files from cloud storage as they arrive. It's the recommended file ingestion tool for production pipelines.

#### Two Modes

**Directory Listing Mode** (default)
- Scans the directory periodically
- Tracks processed files via a `_symlink_format_manifest` or checkpoint
- Lower latency, no extra setup
- Supported on all cloud storage

```python
# Auto Loader in directory listing mode
df = (
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.useIncrementalListing", "true")
    .option("header", "true")
    .load("abfss://landing@storage.dfs.core.windows.net/sales/")
)
```

**File Notification Mode** (recommended for production)
- Uses cloud storage event notifications (Azure Event Grid, AWS SQS, GCP Pub/Sub)
- Sub-second latency
- Scales to millions of files without scanning
- Requires additional cloud resource setup

```python
# Auto Loader in file notification mode
df = (
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.subscriptionId", "<subscription-id>")
    .option("cloudFiles.connectionString", "<event-grid-conn-str>")
    .load("abfss://landing@storage.dfs.core.windows.net/sensor-data/")
)
```

#### Schema Inference and Evolution

Auto Loader can **infer schemas** from sample files and **evolve** when new columns appear.

```python
df = (
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/path/to/schema-checkpoint")
    .option("cloudFiles.schemaHints", "customer_id BIGINT, amount DOUBLE")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("abfss://landing@storage.dfs.core.windows.net/sales/")
)
```

**Schema evolution modes:**
- `rescue` (default) — puts mismatched data in `_rescued_data` column
- `failOnNewColumns` — fails the stream on new columns
- `logOnly` — logs schema changes but continues processing
- `addNewColumns` — automatically adds new columns to the target table

#### Rescued Data Column

The **`rescuedDataColumn`** captures columns that don't match the schema:

```python
df = (
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load("abfss://landing@storage.dfs.core.windows.net/sales/")
)
```

Any value that fails type casting or has no target column goes into `_rescued_data` as a JSON string. This **preserves data** that would otherwise be silently dropped.

🔬 **Lab Reference (Lab 07 — Solaris Energy):** You configure **Auto Loader** to continuously detect new CSV files landing in ADLS, infer the schema, and handle schema drift with the rescued data column. You also explore **directory listing vs file notification** modes.

💡 **Exam Tip:** Auto Loader questions often contrast it with `COPY INTO`. **Auto Loader** is for **streaming/continuous** ingestion; **COPY INTO** is for **batch/idempotent** ingestion. The rescued data column is a frequent exam topic — know its purpose (capture schema drift data, not drop it).

---

### 3.2.6 Spark Structured Streaming (Kafka / Event Hubs)

**Structured Streaming** processes real-time data from sources like **Kafka** and **Azure Event Hubs**.

```python
# Reading from Kafka
df_kafka = (
  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:9092,host2:9092")
    .option("subscribe", "sales-topic")
    .option("startingOffsets", "earliest")
    .load()
)

# Parse JSON value from Kafka
from pyspark.sql.functions import from_json, col, schema_of_json

# Define schema
json_schema = "sale_id BIGINT, customer_id BIGINT, amount DOUBLE, sale_date STRING"

df_parsed = (
  df_kafka
    .select(from_json(col("value").cast("string"), json_schema).alias("data"))
    .select("data.*")
)

# Write to Delta
query = (
  df_parsed.writeStream
    .format("delta")
    .option("checkpointLocation", "/path/to/checkpoint")
    .table("main.dp750.streaming_sales")
)
```

**Reading from Azure Event Hubs:**

```python
# Event Hubs uses its own connector
connection_string = "Endpoint=sb://<namespace>.servicebus.windows.net/;..."

df_eh = (
  spark.readStream
    .format("eventhubs")
    .option("eventhubs.connectionString", connection_string)
    .option("eventhubs.consumerGroup", "$Default")
    .load()
)
```

**Key streaming concepts tested:**
- **Checkpointing** — stores progress information for exactly-once semantics
- **Watermarking** — handles late data with `withWatermark()`
- **Output modes** — `append` (new rows only), `complete` (full result), `update` (updated rows)
- **Triggers** — `ProcessingTime('10 seconds')`, `Once` (micro-batch), `AvailableNow` (process all)

💡 **Exam Tip:** The exam tests **concepts**, not API details. Know: what checkpointing does, the difference between output modes, and when to use watermarking. Expect scenarios like "streaming stock trades from Event Hubs into Delta Lake."

---

### 3.2.7 AUTO CDC API

The **AUTO CDC API** simplifies Change Data Capture ingestion. It automatically detects changes from source databases that publish CDC feeds (e.g., Azure SQL Database change tracking, Debezium Kafka connect).

```python
# Simplified CDC ingestion
df_cdc = (
  spark.readStream
    .format("auto-cdc")
    .option("source", "sqldb")       # Source system type
    .option("connectionString", "...")
    .option("database", "orders_db")
    .option("table", "orders")
    .load()
)

# Write changes to Delta
df_cdc.writeStream \
  .format("delta") \
  .option("checkpointLocation", "/path/to/cdc-checkpoint") \
  .table("main.dp750.orders_cdc")
```

The AUTO CDC API handles:
- **Schema detection** — automatically maps source columns
- **Change type mapping** — `INSERT`, `UPDATE`, `DELETE` operations
- **Merge into target** — can directly write to a Delta table with upsert semantics

💡 **Exam Tip:** AUTO CDC is the **newer, simpler alternative** to manually building Structured Streaming CDC pipelines with Debezium. It's fully managed within Azure Databricks.

---

### 3.2.8 Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables / DLT)

**Lakeflow Spark Declarative Pipelines** let you define ETL pipelines in **declarative SQL or Python** — the platform handles orchestration, dependency resolution, and monitoring.

#### Pipeline Structure

A pipeline is a DAG of **tables** and **views** defined with Python decorators or SQL.

**Python API:**

```python
import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table
def bronze_orders():
  """Ingest raw orders from landing zone"""
  return (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", "/path/schema")
      .option("cloudFiles.rescuedDataColumn", "_rescued_data")
      .load("abfss://landing@storage.dfs.core.windows.net/orders/")
  )

@dlt.table
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
def silver_orders():
  """Cleaned and validated orders"""
  return (
    dlt.read_stream("bronze_orders")
      .select(
        col("order_id"),
        col("customer_id"),
        col("amount").cast("decimal(10,2)"),
        col("order_date"),
        current_timestamp().alias("processed_at")
      )
      .dropDuplicates(["order_id"])
  )

@dlt.table
def gold_daily_summary():
  """Daily aggregated revenue"""
  return (
    spark.sql("""
      SELECT
        order_date,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(amount) AS total_revenue
      FROM silver_orders
      GROUP BY order_date
    """)
  )
```

**SQL API:**

```sql
-- Bronze: Raw ingestion
CREATE OR REFRESH STREAMING TABLE bronze_orders
AS SELECT * FROM read_files(
  'abfss://landing@storage.dfs.core.windows.net/orders/',
  format => 'csv',
  header => true,
  schema => 'order_id BIGINT, customer_id BIGINT, amount DOUBLE, order_date DATE'
);

-- Silver: Cleaned with constraints
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL),
  CONSTRAINT positive_amount EXPECT (amount > 0)
)
AS SELECT DISTINCT
    order_id,
    customer_id,
    CAST(amount AS DECIMAL(10,2)) AS amount,
    order_date
FROM bronze_events
WHERE order_id IS NOT NULL;

-- Gold: Aggregated
CREATE OR REFRESH LIVE TABLE gold_daily_summary
AS SELECT
    order_date,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(amount) AS total_revenue
FROM silver_orders
GROUP BY order_date;
```

#### Pipeline Features

| Feature | Description |
|---|---|
| **Auto-scaling** | Pipelines automatically scale compute resources |
| **Dependency resolution** | Pipeline engine determines execution order from the DAG |
| **Error handling** | Per-table error policies: `allow`, `forceRun`, `fail` |
| **Monitoring** | Built-in UI for data quality, lineage, and latency tracking |
| **Continuous vs Triggered** | Choose between streaming (continuous) or batch (triggered) mode |

💡 **Exam Tip:** Lakeflow Spark Declarative Pipelines are the **recommended way to implement medallion architecture** on Azure Databricks. The exam tests the decorator pattern (`@dlt.table`, `@dlt.expect`), constraint syntax, and the difference between streaming and triggered pipelines.

🔬 **Lab Reference (Lab 09 — ClearCover Insurance):** You build a **Lakeflow Spark Declarative Pipeline** with quality constraints, schema drift handling, and monitoring — the full medallion pipeline lifecycle.

---

### 3.2.9 Azure Data Factory Integration

**Azure Data Factory (ADF)** integrates with Azure Databricks to provide orchestration, scheduling, and monitoring for your data pipelines.

#### Two Integration Methods

1. **Databricks Notebook Activity** — Run a Databricks notebook as an ADF pipeline activity; pass parameters via `baseParameters`
2. **Databricks Job Activity** — Trigger an existing Databricks job from an ADF pipeline

#### Common Pattern: ADF Schedules, Databricks Transforms

```
ADF Trigger (Schedule) → ADF Pipeline → Databricks Notebook Activity → Databricks Cluster → Delta Lake
```

#### Passing Parameters

When using the **Databricks Notebook Activity**, ADF can pass parameters to the notebook:

```json
{
  "name": "RunETLNotebook",
  "type": "DatabricksNotebook",
  "linkedServiceName": { "referenceName": "DatabricksLinkedService" },
  "typeProperties": {
    "notebookPath": "/Users/admin/etl_pipeline",
    "baseParameters": {
      "source_date": "@pipeline().parameters.run_date",
      "environment": "production"
    }
  }
}
```

Inside the notebook, access these parameters via `dbutils.widgets.get()`:

```python
source_date = dbutils.widgets.get("source_date")
environment = dbutils.widgets.get("environment")
```

#### When to Use ADF Integration

- You already have an existing ADF infrastructure for orchestration
- You need cross-service dependency management (ADF → Databricks → SQL DB → Power BI)
- You want visual pipeline monitoring and alerting through ADF
- You need complex scheduling logic (time-based, event-based, dependency chains)

#### When to Use Databricks-Only Orchestration

- The entire pipeline lives within Databricks
- You're using Lakeflow Spark Declarative Pipelines (which have built-in orchestration)
- You don't need external service coordination

💡 **Exam Tip:** The exam tests conceptual understanding: ADF can trigger Databricks notebooks and jobs as pipeline activities. The key difference between **Notebook Activity** (ad-hoc notebook execution with parameters) and **Job Activity** (triggering a pre-configured job) is tested. Know that `baseParameters` is how you pass parameters from ADF into a Databricks notebook.

## 3.3 Cleanse, Transform, and Load Data

### 3.3.1 Data Profiling

Before cleansing, you must understand the data. These commands profile the data quality.

```sql
-- DESCRIBE: Get column names and types
DESCRIBE main.dp750.raw_sales;

-- DESCRIBE DETAIL: Get table metadata (size, format, partition info, clustering)
DESCRIBE DETAIL main.dp750.raw_sales;

-- DESCRIBE HISTORY: Show table history/operations
DESCRIBE HISTORY main.dp750.raw_sales;
```

```python
# SUMMARIZE: Statistical summary of numeric columns
df = spark.table("main.dp750.raw_sales")
df.summary().show()

# Custom profiling
df.select(
  count("*").alias("total_rows"),
  countDistinct("customer_id").alias("unique_customers"),
  sum(when(col("amount").isNull(), 1).otherwise(0)).alias("null_amounts"),
  sum(when(col("amount") < 0, 1).otherwise(0)).alias("negative_amounts"),
  min("amount").alias("min_amount"),
  max("amount").alias("max_amount"),
  avg("amount").alias("avg_amount")
).show()
```

🔬 **Lab Reference (Lab 08 — Pristine Properties):** You profile real estate data using `DESCRIBE DETAIL` and `df.summary()` to identify **missing values, data type mismatches, and outliers** before transformation.

---

### 3.3.2 Data Type Optimization

Choosing the right data types saves storage and speeds up queries.

```python
from pyspark.sql.types import (
  StructType, StructField, LongType, IntegerType,
  ShortType, ByteType, DecimalType, DateType, StringType
)

# Optimized schema — uses smallest possible types
optimized_schema = StructType([
  StructField("sale_id", LongType(), True),         -- BIGINT
  StructField("customer_id", IntegerType(), True),  -- INT instead of BIGINT
  StructField("quantity", ShortType(), True),       -- SMALLINT vs INT
  StructField("status_code", ByteType(), True),     -- TINYINT vs STRING
  StructField("amount", DecimalType(10,2), True),   -- DECIMAL vs DOUBLE
  StructField("sale_date", DateType(), True),       -- DATE vs TIMESTAMP
  StructField("product_name", StringType(), True)   -- STRING (no VARCHAR)
])
```

**Type casting in SQL:**

```sql
SELECT
  CAST(sale_id AS BIGINT) AS sale_id,
  CAST(customer_id AS INT) AS customer_id,
  CAST(quantity AS SMALLINT) AS quantity,
  CAST(status_code AS TINYINT) AS status_code,
  CAST(amount AS DECIMAL(10,2)) AS amount,
  CAST(sale_date AS DATE) AS sale_date,
  CAST(product_name AS STRING) AS product_name
FROM main.dp750.raw_sales;
```

**Type optimization guidelines:**
- Use `INTEGER` instead of `BIGINT` when values fit in 2^31 (~2.1 billion)
- Use `SMALLINT` (`ShortType`) for values up to ~32K
- Use `TINYINT` (`ByteType`) for status codes, flags (0-255)
- Use `DECIMAL(p,s)` for financial amounts — never `DOUBLE` or `FLOAT`
- Use `DATE` when you don't need time components
- Use `STRING` for text — no need for VARCHAR(n) in Spark

💡 **Exam Tip:** The exam tests **which Spark data type to choose** given a range. E.g., "A column `age` stores values 0-120" → `ByteType` (TINYINT). "A column `year` stores 2025" → `ShortType` (SMALLINT).

---

### 3.3.3 Deduplication Techniques

Duplicate data corrupts analytics. Here are the key deduplication patterns.

#### Using `DISTINCT`

```sql
CREATE OR REPLACE TABLE main.dp750.dedup_sales AS
SELECT DISTINCT * FROM main.dp750.raw_sales;
```

⚠️ **Warning:** `DISTINCT *` compares **all columns**. If only `sale_id` is the key, use targeted dedup.

#### Using `ROW_NUMBER()` with Window

The most precise dedup — pick one row per key based on a tiebreaker.

```sql
CREATE OR REPLACE TABLE main.dp750.dedup_sales AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY sale_id
      ORDER BY ingested_at DESC   -- keep most recent
    ) AS rn
  FROM main.dp750.raw_sales
)
SELECT * EXCEPT rn FROM ranked WHERE rn = 1;
```

#### Using `dropDuplicates()` in PySpark

```python
df_dedup = (
  spark.table("main.dp750.raw_sales")
    .dropDuplicates(["sale_id"])
    .orderBy(col("ingested_at").desc())
)
```

#### Using `MERGE` for Idempotent Dedup

```sql
MERGE INTO main.dp750.sales AS target
USING (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY sale_id ORDER BY ingested_at DESC
  ) AS rn
  FROM main.dp750.staging_sales
) AS source
ON target.sale_id = source.sale_id AND source.rn = 1
WHEN NOT MATCHED THEN INSERT *;
```

---

### 3.3.4 Null Handling

Null values propagate through aggregations and comparisons — handle them explicitly.

#### In PySpark — `df.na.fill()`

```python
from pyspark.sql.functions import col, when, coalesce

# Fill nulls with default values
df_clean = spark.table("main.dp750.raw_sales").na.fill({
  "amount": 0.0,
  "customer_id": -1,
  "product_name": "UNKNOWN"
})

# Conditionally replace nulls
df_clean = df_clean.withColumn(
  "amount",
  when(col("amount").isNull(), 0.0).otherwise(col("amount"))
)

# Drop rows with any nulls
df_no_nulls = spark.table("main.dp750.raw_sales").na.drop()

# Drop rows where key column is null
df_key_present = spark.table("main.dp750.raw_sales").na.drop(subset=["sale_id"])
```

#### In SQL — `COALESCE` and `IFNULL`

```sql
SELECT
  sale_id,
  COALESCE(customer_id, -1) AS customer_id,
  COALESCE(amount, 0.0) AS amount,
  IFNULL(product_name, 'UNKNOWN') AS product_name   -- IFNULL = COALESCE with 2 args
FROM main.dp750.raw_sales;
```

#### `IFNULL` vs `COALESCE` vs `NULLIF`

| Function | Behavior | Example |
|---|---|---|
| `IFNULL(expr, replacement)` | Returns `replacement` if `expr` is NULL, else `expr` | `IFNULL(amount, 0)` |
| `COALESCE(expr1, expr2, ..., exprN)` | Returns the **first non-NULL** value | `COALESCE(shipping, handling, 0)` |
| `NULLIF(expr1, expr2)` | Returns NULL if `expr1 == expr2`, else `expr1` | `NULLIF(amount, -1)` — treat -1 as unknown |

💡 **Exam Tip:** The exam expects you to choose the right null-handling function. Scenario: "A table has `discount` and `coupon` columns; use the first non-null value" → **COALESCE**.

---

### 3.3.5 Filtering, Grouping, and Aggregation

```sql
-- Filtering: WHERE and HAVING
SELECT
  customer_id,
  COUNT(*) AS order_count,
  SUM(amount) AS total_spent
FROM main.dp750.sales
WHERE sale_date >= '2025-01-01'
  AND amount > 0
GROUP BY customer_id
HAVING COUNT(*) > 5              -- only customers with >5 orders
ORDER BY total_spent DESC;

-- Multiple aggregations
SELECT
  DATE_TRUNC('month', sale_date) AS month,
  product_category,
  COUNT(DISTINCT customer_id) AS unique_customers,
  SUM(amount) AS revenue,
  AVG(amount) AS avg_order_value,
  PERCENTILE_APPROX(amount, 0.5) AS median_order_value
FROM main.dp750.sales
GROUP BY ALL                       -- shorthand: group by all non-aggregate SELECT columns
ORDER BY month, revenue DESC;
```

**PySpark equivalent:**

```python
from pyspark.sql.functions import (
  count, countDistinct, sum, avg, approxQuantile, date_trunc
)

df.groupBy(
  date_trunc("month", col("sale_date")).alias("month"),
  "product_category"
).agg(
  countDistinct("customer_id").alias("unique_customers"),
  sum("amount").alias("revenue"),
  avg("amount").alias("avg_order_value")
).orderBy("month", col("revenue").desc()).show()
```

---

### 3.3.6 Joins

Spark supports standard SQL joins. The execution engine automatically chooses **Broadcast Hash Join** for small tables and **Sort Merge Join** for large ones.

```python
# In PySpark
df_orders = spark.table("main.dp750.orders")
df_customers = spark.table("main.dp750.customers")
df_products = spark.table("main.dp750.products")

# Inner join
df_inner = df_orders.join(df_customers, "customer_id", "inner")

# Left outer join (keep all orders, even without a customer)
df_left = df_orders.join(df_customers, "customer_id", "left")

# Full outer join
df_full = df_orders.join(df_customers, "customer_id", "full")

# Cross join (Cartesian product)
df_cross = df_orders.crossJoin(df_products)

# Semi join — keep orders where customer exists (no right-side columns)
df_semi = df_orders.join(df_customers, "customer_id", "semi")

# Anti join — keep orders where customer does NOT exist
df_anti = df_orders.join(df_customers, "customer_id", "anti")
```

```sql
-- SQL joins
SELECT o.*, c.customer_name, c.loyalty_tier
FROM main.dp750.orders o
LEFT JOIN main.dp750.customers c ON o.customer_id = c.customer_id;

-- Semi join (exists)
SELECT * FROM main.dp750.orders o
WHERE EXISTS (
  SELECT 1 FROM main.dp750.customers c WHERE c.customer_id = o.customer_id
);

-- Anti join (not exists)
SELECT * FROM main.dp750.orders o
WHERE NOT EXISTS (
  SELECT 1 FROM main.dp750.customers c WHERE c.customer_id = o.customer_id
);
```

#### Join Type Quick Reference

| Join Type | Result |
|---|---|
| `inner` | Only matching rows from both sides |
| `left` / `left_outer` | All left rows + matched right rows (NULLs when no match) |
| `right` / `right_outer` | All right rows + matched left rows |
| `full` / `full_outer` | All rows from both sides |
| `cross` | Every left × every right row (Cartesian) |
| `semi` | Left rows that have a match in right (no right columns) |
| `anti` | Left rows that do **not** have a match in right |

💡 **Exam Tip:** The exam tests `semi` and `anti` joins specifically — they're less common in other SQL dialects. Know when to use `semi` (filtering duplicates against a dimension) vs `INNER JOIN` with `DISTINCT`.

---

### 3.3.7 Set Operations

Set operations combine rows from two queries with the **same column structure**.

```sql
-- UNION ALL: Append all rows (allows duplicates)
SELECT sale_id, customer_id FROM main.dp750.sales_2024_q1
UNION ALL
SELECT sale_id, customer_id FROM main.dp750.sales_2024_q2;

-- UNION: Append distinct rows (no duplicates)
SELECT sale_id, customer_id FROM main.dp750.sales_2024
UNION
SELECT sale_id, customer_id FROM main.dp750.sales_2025;

-- INTERSECT: Rows present in both queries
SELECT sale_id, customer_id FROM main.dp750.active_customers
INTERSECT
SELECT sale_id, customer_id FROM main.dp750.premium_customers;

-- EXCEPT (MINUS): Rows in first query but not in second
SELECT sale_id, customer_id FROM main.dp750.all_customers
EXCEPT
SELECT sale_id, customer_id FROM main.dp750.dormant_customers;
```

**In PySpark:**

```python
df1 = spark.table("main.dp750.sales_2024")
df2 = spark.table("main.dp750.sales_2025")

df_union_all = df1.unionAll(df2)      # UNION ALL
df_union = df1.union(df2).distinct()   # UNION
df_intersect = df1.intersect(df2)      # INTERSECT
df_except = df1.exceptAll(df2)         # EXCEPT (Spark 3.1+)
```

⚠️ **Warning:** `UNION` is **slower** than `UNION ALL` because it requires a shuffle to deduplicate. Use `UNION ALL` unless you specifically need deduplication.

---

### 3.3.8 PIVOT and UNPIVOT

**PIVOT** rotates rows into columns. **UNPIVOT** rotates columns back to rows.

#### PIVOT

```sql
-- Pivot monthly sales into columns
SELECT *
FROM (
  SELECT
    product_category,
    DATE_TRUNC('month', sale_date) AS sale_month,
    amount
  FROM main.dp750.sales
  WHERE sale_date >= '2025-01-01' AND sale_date < '2025-07-01'
)
PIVOT (
  SUM(amount)
  FOR sale_month IN (
    '2025-01-01' AS jan,
    '2025-02-01' AS feb,
    '2025-03-01' AS mar,
    '2025-04-01' AS apr,
    '2025-05-01' AS may,
    '2025-06-01' AS jun
  )
)
ORDER BY product_category;
```

**PySpark PIVOT:**

```python
from pyspark.sql.functions import sum

df_pivot = (
  spark.table("main.dp750.sales")
    .filter(col("sale_date") >= "2025-01-01")
    .filter(col("sale_date") < "2025-07-01")
    .groupBy("product_category")
    .pivot("sale_month", ["2025-01-01", "2025-02-01", "2025-03-01",
                           "2025-04-01", "2025-05-01", "2025-06-01"])
    .agg(sum("amount"))
)
```

#### UNPIVOT

```sql
-- Unpivot wide table back to long format
CREATE OR REPLACE TABLE main.dp750.sales_long AS
SELECT product_category, sale_month, amount
FROM main.dp750.sales_wide
UNPIVOT (
  amount FOR sale_month IN (jan, feb, mar, apr, may, jun)
);
```

🔬 **Lab Reference (Lab 08 — Pristine Properties):** You **pivot market statistics** by year and property type, then **unpivot** back to analyze trends across time periods.

💡 **Exam Tip:** PIVOT questions test whether you know the syntax: the aggregate function, the pivot column, and the IN clause listing the values.

---

### 3.3.9 Loading Strategies

The final step — writing data to the target table. Choose the right **load mode**.

| Strategy | When to Use | Command |
|---|---|---|
| **Append** | Fact tables, logs, event data (new rows only) | `.mode("append")` or `INSERT INTO` |
| **Overwrite** | Full refresh of dimension tables, summary tables | `.mode("overwrite")` or `INSERT OVERWRITE` |
| **Merge / Upsert** | Incremental updates (SCD, CDC) | `MERGE` statement |
| **Truncate + Insert** | Full refresh with schema change | `.mode("overwrite").option("overwriteSchema", "true")` |

```python
# Append mode (fact table)
df.write.mode("append").saveAsTable("main.dp750.fact_sales")

# Overwrite mode (dimension table full refresh)
df.write.mode("overwrite").saveAsTable("main.dp750.dim_product")

# Overwrite with schema evolution
df.write.mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("main.dp750.dim_product")
```

```sql
-- Append
INSERT INTO main.dp750.fact_sales
SELECT * FROM main.dp750.staging_sales;

-- Overwrite
INSERT OVERWRITE main.dp750.dim_product
SELECT * FROM main.dp750.staging_products;
```

💡 **Exam Tip:** The exam asks: "Which load strategy for a fact table that receives 10M new events daily?" → **Append**. "Which strategy for a dimension table that needs weekly full refresh?" → **Overwrite**. "Which strategy for incremental SCD Type 2 changes?" → **Merge**.

---

## 3.4 Implement Data Quality Constraints

### 3.4.1 Data Quality Expectations in Lakeflow Spark Declarative Pipelines

**Expectations** define data quality rules that rows must satisfy. They are declared within the pipeline definition.

#### `expect()` — Fail on Violation

If a row violates the constraint, the **pipeline fails**.

```python
@dlt.table
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("positive_amount", "amount > 0")
def silver_orders():
  return dlt.read_stream("bronze_orders")
```

#### `expect_or_drop()` — Drop Violating Rows

Rows that violate the constraint are **silently dropped**. The pipeline continues.

```python
@dlt.table
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
def silver_orders():
  return dlt.read_stream("bronze_orders")
```

#### `expect_or_quarantine()` — Quarantine Violating Rows

Bad rows are moved to a quarantine table for later inspection. The pipeline continues.

```python
@dlt.table
@dlt.expect_or_quarantine("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_quarantine("positive_amount", "amount > 0")
def silver_orders():
  return dlt.read_stream("bronze_orders")
```

#### In SQL

```sql
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT positive_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date EXPECT (order_date IS NOT NULL) ON VIOLATION QUARANTINE
)
AS SELECT * FROM bronze_orders;
```

#### Constraint Behavior Summary

| Decorator | SQL `ON VIOLATION` | Behavior |
|---|---|---|
| `@dlt.expect(...)` | `FAIL UPDATE` | Pipeline stops on violation |
| `@dlt.expect_or_drop(...)` | `DROP ROW` | Bad rows are silently dropped |
| `@dlt.expect_or_quarantine(...)` | `QUARANTINE` | Bad rows go to quarantine table |

🔬 **Lab Reference (Lab 09 — ClearCover Insurance):** You implement all three expectation types — `expect()`, `expect_or_drop()`, and `expect_or_quarantine()` — to handle invalid policy data. Quarantined records are stored for manual review.

💡 **Exam Tip:** The exam tests the **behavior difference**: `expect` = stop, `expect_or_drop` = skip, `expect_or_quarantine` = save aside. Scenario: "Which expectation should you use to prevent bad data from entering the silver table but still preserve it for debugging?" → **expect_or_quarantine()**.

---

### 3.4.2 CHECK Constraints

**CHECK constraints** are enforced **server-side** — Spark validates the constraint on every write to the Delta table, regardless of how the write is performed.

```sql
-- Add CHECK constraint on existing table
ALTER TABLE main.dp750.sales
ADD CONSTRAINT valid_amount CHECK (amount >= 0);

ALTER TABLE main.dp750.sales
ADD CONSTRAINT valid_date_range CHECK (
  sale_date BETWEEN '2020-01-01' AND CURRENT_DATE()
);

-- Check constraints are visible in table metadata
DESCRIBE DETAIL main.dp750.sales;
SHOW TBLPROPERTIES main.dp750.sales;
```

**What happens on violation:**

```sql
-- This INSERT will FAIL because amount < 0 violates the CHECK constraint
INSERT INTO main.dp750.sales VALUES (999, 123, -50.00, '2025-01-15');
-- Error: CHECK constraint 'valid_amount' violated
```

💡 **Exam Tip:** CHECK constraints are **stronger** than expectations because they're enforced at the table level, not just within a pipeline. Anyone writing to the table must satisfy them.

---

### 3.4.3 NOT NULL Constraints

A simpler constraint — ensures a column never contains NULL values.

```sql
-- Add NOT NULL constraint
ALTER TABLE main.dp750.sales
ALTER COLUMN sale_id SET NOT NULL;

ALTER TABLE main.dp750.sales
ALTER COLUMN sale_date SET NOT NULL;

-- Remove NOT NULL constraint
ALTER TABLE main.dp750.sales
ALTER COLUMN sale_date DROP NOT NULL;
```

**Schema enforcement with NOT NULL:**

```sql
CREATE TABLE main.dp750.enforced_sales (
  sale_id      BIGINT NOT NULL,
  customer_id  BIGINT,
  amount       DECIMAL(10,2) NOT NULL,
  sale_date    DATE NOT NULL
)
USING DELTA;
```

⚠️ **Warning:** Adding `NOT NULL` to an existing table fails if the column already contains NULLs. Clean the data first:

```sql
-- Remove nulls before adding constraint
DELETE FROM main.dp750.sales WHERE sale_id IS NULL;
ALTER TABLE main.dp750.sales ALTER COLUMN sale_id SET NOT NULL;
```

---

### 3.4.4 Schema Enforcement and Drift

**Schema enforcement** means Delta Lake rejects writes that introduce columns not in the table schema. **Schema drift** occurs when source data changes schema (new columns added, types changed).

#### Strict enforcement (default)

```sql
-- This INSERT will FAIL if source has columns not in target schema
INSERT INTO main.dp750.sales
SELECT * FROM main.dp750.staging_sales;
```

#### Handling schema drift with `mergeSchema`

```sql
-- Allow schema evolution during append
INSERT INTO main.dp750.sales
SELECT * FROM main.dp750.staging_sales
OPTIONS (mergeSchema = true);
```

```python
# In PySpark write
df.write \
  .mode("append") \
  .option("mergeSchema", "true") \
  .saveAsTable("main.dp750.sales")
```

#### Using `rescuedDataColumn` for schema drift

The **`rescuedDataColumn`** captures data that doesn't match the expected schema. It's configured at **read time** (not write time):

```python
df = (
  spark.read
    .format("delta")
    .option("rescuedDataColumn", "_rescued_data")
    .table("main.dp750.sales")
)
```

But it's more commonly used with **Auto Loader at ingestion time**:

```python
df_bronze = (
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("abfss://landing@storage.dfs.core.windows.net/sales/")
)
```

The `_rescued_data` column is a `STRING` containing the JSON-serialized fields that couldn't be parsed. This is the **safe sandbox** pattern — new columns land in the rescued data column without breaking the pipeline.

---

### 3.4.5 Column Type Validation with `.cast()`

Use `.cast()` to **explicitly validate and convert** column types.

```python
from pyspark.sql.functions import col

df_validated = (
  spark.table("main.dp750.raw_sales")
    .withColumn("amount", col("amount").cast("decimal(10,2)"))
    .withColumn("sale_date", col("sale_date").cast("date"))
    .withColumn("customer_id", col("customer_id").cast("int"))
)
```

Rows where casting fails become `NULL`. You can then filter or quarantine them:

```python
# Identify rows with bad casts
df_bad_casts = df_validated.filter(
  col("amount").isNull() | col("sale_date").isNull()
)

# Keep only valid rows
df_clean = df_validated.filter(
  col("amount").isNotNull() & col("sale_date").isNotNull()
)
```

🔬 **Lab Reference (Lab 09 — ClearCover Insurance):** You use `col().cast()` to validate data **before** writing to the silver table, quarantining rows where type conversion fails.

💡 **Exam Tip:** Casting to a stricter type (`STRING → DATE`, `STRING → DECIMAL`) creates NULLs on failure — this is an intentional data quality gate, not a bug.

---

### 3.4.6 Putting It All Together: A Complete Pipeline

Here's a full medallion pipeline demonstrating all the data quality techniques:

```python
import dlt
from pyspark.sql.functions import col, current_timestamp, input_file_name

# ── BRONZE: Raw ingestion with schema drift protection ──
@dlt.table
def bronze_insurance_policies():
  return (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", "/pipelines/insurance/schema")
      .option("cloudFiles.schemaHints",
              "policy_id BIGINT, customer_id BIGINT, premium DOUBLE, start_date DATE")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.rescuedDataColumn", "_rescued_data")
      .option("cloudFiles.schemaEvolutionMode", "rescue")
      .load("abfss://landing@storage.dfs.core.windows.net/insurance/")
      .withColumn("ingested_at", current_timestamp())
      .withColumn("source_file", input_file_name())
  )

# ── SILVER: Cleansed with quality constraints ──
@dlt.table
@dlt.expect("valid_policy_id", "policy_id IS NOT NULL")
@dlt.expect_or_drop("positive_premium", "premium > 0")
@dlt.expect_or_quarantine("valid_start_date", "start_date IS NOT NULL")
def silver_insurance_policies():
  return (
    dlt.read_stream("bronze_insurance_policies")
      .select(
        col("policy_id").cast("bigint"),
        col("customer_id").cast("int"),
        col("premium").cast("decimal(10,2)"),
        col("start_date").cast("date"),
        col("_rescued_data"),
        col("ingested_at")
      )
      .filter(col("policy_id").isNotNull())
      .dropDuplicates(["policy_id"])
  )

# ── GOLD: Business-ready aggregates ──
@dlt.table
def gold_monthly_premiums():
  return (
    spark.sql("""
      SELECT
        DATE_TRUNC('month', start_date) AS policy_month,
        COUNT(DISTINCT customer_id) AS policyholders,
        SUM(premium) AS total_premiums,
        AVG(premium) AS avg_premium
      FROM silver_insurance_policies
      GROUP BY ALL
      ORDER BY policy_month DESC
    """)
  )
```

---

## Key Exam Tips

### High-Weight Topics (~80% of LP3 questions)

1. **Delta Lake features** — ACID, time travel (`VERSION AS OF` / `TIMESTAMP AS OF`), CDF with `table_changes()`, Liquid Clustering over partitioning
2. **SCD Type 2** — `MERGE` pattern with `effective_date`, `end_date`, `is_current` columns
3. **Auto Loader** — directory listing vs file notification mode, `cloudFiles.rescuedDataColumn`, `cloudFiles.schemaEvolutionMode`
4. **Lakeflow Declarative Pipelines** — `@dlt.table`, `@dlt.expect` family, constraint behaviors
5. **COPY INTO** — idempotent loading, file deduplication, `VALIDATE` clause
6. **Data quality constraints** — CHECK, NOT NULL, expectations, rescued data column
7. **UniForm / Universal Format** — cross-engine read compatibility (Delta→Iceberg/Hudi), metadata-only overhead, read-only from Iceberg side
8. **CLONE operations** — Deep Clone (full copy, independent) vs Shallow Clone (metadata-only, shared files)
9. **Batch vs Streaming decision criteria** — latency, source type, cost, complexity trade-offs

### Common Exam Scenarios

| Scenario | Solution |
|---|---|
| "Ingest CSV files continuously from ADLS" | **Auto Loader** with `readStream.format("cloudFiles")` |
| "Load files exactly once, support retry" | **COPY INTO** (idempotent) |
| "Track what changed in a table for auditing" | **Enable CDF** → `table_changes()` |
| "Build SCD Type 2 dimension" | **MERGE** with effective/end dates and is_current flag |
| "Handle schema drift without breaking pipeline" | **Rescued data column** (`_rescued_data`) |
| "Prevent negative amounts from entering table" | **CHECK constraint** (`amount >= 0`) |
| "Skip bad records, log them for later inspection" | **`expect_or_quarantine()`** in Lakeflow pipeline |
| "Optimize query performance on multiple columns" | **Liquid Clustering** (`CLUSTER BY`) |
| "Stream Kafka events to Delta" | **Structured Streaming** with Kafka source |
| "Expose Delta table to Iceberg readers" | **UniForm** (`delta.universalFormat.enabledFormats` = 'iceberg') |
| "Create independent copy of a table for testing" | **DEEP CLONE** |
| "Create fast metadata-only copy for sandbox" | **SHALLOW CLONE** |
| "Convert external table to managed with Iceberg" | **CTAS** or **DEEP CLONE** + enable **UniForm** |
| "Choose batch vs streaming for file ingestion" | **Batch** (or **Auto Loader** for incremental) |
| "Document table schema for data discovery" | **COMMENT ON TABLE** / **COMMENT ON COLUMN** |

### 30-Second Mnemonics

- **COPY = Idempotent** (C-O-P-Y = **C**an **O**nly **P**rocess **Y**ou once)
- **SCD2 = MERGE + dates** (effective, end, is_current)
- **3 Expects: Stop, Drop, Box** (`expect`=stop, `expect_or_drop`=skip, `expect_or_quarantine`=save aside)
- **Liquid Clustering > Partitioning** (modern, adaptive, multi-column)
- **Rescued Data = Schema Safety Net** (captures what doesn't fit)
- **UniForm = Zero Duplication** (metadata only — data files stay the same)
- **Deep Clone = Full Copy** (slow, independent); **Shallow Clone = Fast Copy** (metadata only, shared files)
- **CLONE Mnemonic: D-FISH** (Deep = Full Independent, SHallow = Files Shared)
- **Batch = Files, Cost Savings; Streaming = Streams, Low Latency**

### Labs Reference Summary

| Lab | Company | Key Skills |
|---|---|---|
| **Lab 06** | Northbank Financial | Delta data model, liquid clustering, SCD Type 2, CDF, time travel |
| **Lab 07** | Solaris Energy | PySpark ingestion, COPY INTO, CTAS, Auto Loader configuration |
| **Lab 08** | Pristine Properties | Data profiling, type optimization, dedup, joins, pivot/unpivot |
| **Lab 09** | ClearCover Insurance | Lakeflow pipeline, expectations, schema drift, rescued data column |

---

> **Next up:** Learning Path 4 — Manage Workflows and Monitor Azure Databricks (CI/CD, orchestration, monitoring).
