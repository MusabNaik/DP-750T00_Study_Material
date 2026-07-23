# Learning Path 4: Deploy and Maintain Data Pipelines and Workloads

**DP-750 | Azure Databricks Data Engineer Associate**
**Exam Weight: ~30-35%**

This learning path covers the complete lifecycle of data pipelines on Azure Databricks — from designing medallion-architecture pipelines and orchestrating multi-task jobs to managing infrastructure as code with Databricks Asset Bundles and diagnosing performance issues with the Spark UI. Each section explains concepts in depth, shows code and configuration examples, and references the official labs.

---

## Module 4.1: Design and Implement Data Pipelines

In this module, you learn how to design production-ready data pipelines using the **medallion architecture**, choose between pipeline authoring approaches, orchestrate multi-step workloads as **Lakeflow Jobs**, and handle errors, retries, and conditional logic.

### The Medallion Architecture

The **medallion architecture** is a data design pattern that organizes data into three progressively refined layers:

- **Bronze (Raw)**: Ingests source data in its original format — JSON files, CSV streams, CDC events, API payloads. This layer is append-only; nothing is deleted or transformed. It preserves an immutable record of every event so you can always replay from source.
- **Silver (Cleaned & Validated)**: Deduplicates, validates schemas, enforces data quality rules, and joins disparate sources into a clean, queryable form. This is where data engineers spend most of their effort. Records may be upserted or merged as corrections arrive.
- **Gold (Aggregated & Business-Ready)**: Contains business-level aggregates, KPIs, dimensional models, and curated feature tables for BI tools, dashboards, and machine learning. This layer is optimized for consumption, not ingestion.

> 💡 **Exam Tip**: Remember the purpose of each layer. The question may describe a scenario ("you need to preserve raw data for auditing" → Bronze, "you need to build a star schema for Power BI" → Gold). Also, medallion is not limited to three layers — some teams add a Platinum layer for ML feature stores.

The pattern works on any Delta Lake table, whether the pipeline is built with notebooks or with **Lakeflow Spark Declarative Pipelines** (formerly called Delta Live Tables, or DLT).

### Pipeline Authoring Approaches

There are two main approaches to authoring pipelines in Azure Databricks:

#### 1. Notebook-Based Pipelines

You write Spark code in Python, SQL, or Scala notebooks, then chain them together in a **Lakeflow Job** as individual tasks. Each notebook typically handles one medallion layer.

```python
# Example: Bronze ingestion notebook
# Reads raw CSV from cloud storage, writes to Bronze Delta table

source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(source_path)

df.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable(target_table)
```

```sql
-- Example: Silver transform notebook
-- Cleans and validates Bronze data, writes to Silver

CREATE OR REPLACE TEMP VIEW silver_clean AS
SELECT DISTINCT
  booking_id,
  LOWER(TRIM(customer_email)) AS customer_email,
  property_name,
  checkin_date,
  checkout_date,
  total_amount,
  currency,
  CURRENT_TIMESTAMP() AS processed_at
FROM globstay_hospitality.bronze_bookings
WHERE booking_id IS NOT NULL
  AND total_amount > 0;
  
MERGE INTO globstay_hospitality.silver_bookings AS target
USING silver_clean AS source
ON target.booking_id = source.booking_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

```sql
-- Example: Gold aggregate notebook
-- Builds business-level KPIs

CREATE OR REPLACE TABLE globstay_hospitality.gold_daily_revenue
AS
SELECT
  property_name,
  DATE(checkin_date) AS day,
  COUNT(DISTINCT booking_id) AS bookings_count,
  SUM(total_amount) AS total_revenue,
  AVG(total_amount) AS avg_booking_value
FROM globstay_hospitality.silver_bookings
GROUP BY property_name, DATE(checkin_date);
```

#### 2. Lakeflow Spark Declarative Pipelines

Lakeflow Spark Declarative Pipelines (formerly **Delta Live Tables** / DLT) let you declare your pipeline logic using Python or SQL with special decorators or `LIVE` syntax. The platform manages dependencies, incremental processing, and data quality automatically.

**Python API with `@dlt` decorators:**

```python
import dlt
from pyspark.sql.functions import col, lower, trim

# Bronze: raw CSV landing
@dlt.table
def bronze_bookings():
    return (
        spark.readStream
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("/databricks-datasets/globstay/raw/")
    )

# Silver: clean and validate
@dlt.table
@dlt.expect_or_drop("valid_email", "customer_email IS NOT NULL")
@dlt.expect("positive_amount", "total_amount > 0")
def silver_bookings():
    return (
        dlt.read_stream("bronze_bookings")
            .select(
                col("booking_id"),
                lower(trim(col("customer_email"))).alias("customer_email"),
                col("property_name"),
                col("total_amount"),
                col("checkin_date")
            )
            .dropDuplicates(["booking_id"])
    )

# Gold: daily aggregates
@dlt.table
def gold_daily_revenue():
    return (
        dlt.read("silver_bookings")
            .groupBy("property_name", "checkin_date")
            .agg(
                count("*").alias("bookings_count"),
                sum("total_amount").alias("total_revenue")
            )
    )
```

**SQL API with `LIVE` syntax:**

```sql
-- Bronze
CREATE OR REFRESH LIVE TABLE bronze_bookings
AS SELECT * FROM read_files(
  '/databricks-datasets/globstay/raw/',
  format => 'csv',
  header => true,
  inferSchema => true
);

-- Silver with constraints
CREATE OR REFRESH LIVE TABLE silver_bookings(
  CONSTRAINT valid_booking_id EXPECT (booking_id IS NOT NULL),
  CONSTRAINT positive_amount EXPECT (total_amount > 0) ON VIOLATION DROP ROW
)
AS SELECT DISTINCT
  booking_id, customer_email, property_name, total_amount
FROM LIVE.bronze_bookings;

-- Gold
CREATE OR REFRESH LIVE TABLE gold_daily_revenue
AS SELECT
  property_name,
  COUNT(DISTINCT booking_id) AS bookings_count,
  SUM(total_amount) AS total_revenue
FROM LIVE.silver_bookings
GROUP BY property_name;
```

> 💡 **Exam Tip**: Lakeflow Spark Declarative Pipelines automatically track dependencies between tables, manage incremental updates, and apply data quality constraints. Notebook-based pipelines give you more procedural control — you choose based on the scenario. If the question emphasizes "declarative," "incremental," or "built-in data quality," the answer is likely toward Lakeflow Declarative Pipelines.

### Data Quality Constraints

Lakeflow Spark Declarative Pipelines support three constraint violation modes:

| Constraint Type | SQL Clause | Behavior on Violation |
|---|---|---|
| **expect** | `CONSTRAINT name EXPECT (condition)` | Writes to a quarantine metric; does NOT fail the pipeline |
| **expect_or_drop** | `CONSTRAINT name EXPECT (condition) ON VIOLATION DROP ROW` | Drops violating rows; pipeline continues |
| **expect_or_fail** | `CONSTRAINT name EXPECT (condition) ON VIOLATION FAIL UPDATE` | Fails the pipeline immediately |

#### COMMENT ON for Column-Level Metadata

Use the SQL `COMMENT ON` statement to document table and column metadata across your medallion layers. This is useful for data lineage tracking, discovery, and governance:

```sql
COMMENT ON TABLE globstay_hospitality.bronze_bookings IS 
  'Raw booking data from the GlobStay reservation API. Append-only, no transformations.';

COMMENT ON COLUMN globstay_hospitality.silver_bookings.customer_email IS 
  'Lowercased and trimmed email address. NULL if original was empty or malformed.';

COMMENT ON COLUMN globstay_hospitality.gold_daily_revenue.total_revenue IS 
  'Sum of total_amount in USD. Excludes canceled bookings.';
```

- Comments are stored in the Delta table metadata and visible in the **Catalog Explorer** and `DESCRIBE TABLE EXTENDED` output
- They persist across pipeline runs and can be queried via `SHOW TBLPROPERTIES`
- Useful for compliance (GDPR data classification), on-call debugging, and onboarding new team members

### Lakeflow Jobs for Orchestration

A **Lakeflow Job** is a multi-task orchestration engine that executes tasks as a **Directed Acyclic Graph (DAG)**. Each task is an independent unit of work — a notebook, Python script, SQL query, dbt model, or declarative pipeline — and tasks can depend on one another.

**Key concepts:**
- **DAG semantics**: Tasks execute in dependency order. Parallel tasks (no dependency) run concurrently. The job completes when every leaf task has finished.
- **Task types**: `notebook_task`, `python_wheel_task`, `spark_jar_task`, `sql_task`, `dbt_task`, `pipeline_task` (for declarative pipelines), and `condition_task` (if/else branching).
- **Compute**: Each task can specify its own cluster, or tasks can share a job cluster defined at the job level.

> 🔬 **Lab 10 – GlobStay Hospitality (45 min)**: In this lab you build a medallion pipeline for a hospitality data platform. You parameterize notebooks with `dbutils.widgets`, create a Lakeflow Job with Bronze → Silver → Gold task dependencies, configure retry policies (max 3 retries, 10-minute intervals), add email notifications for failures, and insert an if/else condition task that routes data-quality failures to an alert notebook instead of the Gold load.

### Error Handling and Retry Policies

Error handling in Lakeflow Jobs is configured at the job level and at individual task levels.

**Task retry settings:**
```json
{
  "tasks": [{
    "task_key": "silver_transform",
    "max_retries": 3,
    "min_retry_interval_millis": 600000,  // 10 minutes
    "retry_on_timeout": true
  }]
}
```

- **`max_retries`**: How many times to retry a failed task before marking it as failed.
- **`min_retry_interval_millis`**: Minimum wait between retries. The actual wait may be longer due to exponential backoff.
- **`retry_on_timeout`**: Whether to retry when a task times out.

**Notification destinations for failures:**
- Email (plain text or — preferably — SMTP with TLS)
- **Slack webhooks** via workspace-level notification destinations
- **PagerDuty** via configured notification destinations
- Generic **webhooks** (POST to any HTTPS endpoint)
- Azure Monitor / Log Analytics (via diagnostic logs)

> 💡 **Exam Tip**: Retries are configured per-task, not per-job. The default is zero retries. If a scenario says "the pipeline should retry up to 3 times with 5-minute intervals," look for the JSON fields `max_retries: 3` and `min_retry_interval_millis: 300000`.

### Error Handling in Notebooks

In addition to job-level retry policies, you can implement error handling directly within your notebook code:

**Try/Except patterns in Python notebooks:**
```python
try:
    df = spark.read.format("csv").load(source_path)
    df.write.mode("append").saveAsTable(target_table)
    
    # Return structured success response
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "rows_written": df.count(),
        "target_table": target_table
    }))
except Exception as e:
    print(f"ERROR: Pipeline failed at stage '{stage_name}': {str(e)}")
    
    # Return structured error response
    dbutils.notebook.exit(json.dumps({
        "status": "failed",
        "error": str(e),
        "stage": stage_name,
        "source_path": source_path
    }))
    raise  # Re-raise to mark the task as failed in the job
```

**Using `dbutils.notebook.exit()` for task communication:**
- Exit values are captured as **task outputs** that downstream tasks can read
- Downstream tasks access them with: `{{tasks.previous_task.notebook_output.key}}`
- **Always return structured JSON** for reliable parsing across tasks
- If you call `dbutils.notebook.exit()` without a value, or if the notebook finishes without calling it, the exit value is `null`

**Using `dbutils.notebook.run()` for sub-notebooks:**
```python
# Run a sub-notebook and capture its exit value
result = dbutils.notebook.run(
    "/shared/transforms/silver_transform",   # notebook path
    300,                                      # timeout in seconds
    {
        "source_table": "bronze_raw",
        "target_table": "silver_clean"
    }
)

# Parse the exit value
result_json = json.loads(result)
if result_json["status"] != "success":
    raise Exception(f"Transform failed: {result_json['error']}")
else:
    print(f"Transform succeeded: {result_json['rows_written']} rows written")
```

**Pipeline-level error handling with condition tasks:**
- Use **condition tasks** to check upstream task outputs (e.g., `{{tasks.silver_transform.notebook_output.status}}`)
- Route to **error-handling notebooks** on failure for diagnostics and cleanup
- Route to **alerting notifications** on threshold breaches (e.g., row count below minimum)

```python
# Example: error-handling notebook (called by condition task on failure)
error_payload = json.loads(dbutils.widgets.get("error_payload"))

# Log detailed error to a Delta table for auditing
spark.sql(f"""
  INSERT INTO pipeline_errors
  SELECT 
    current_timestamp() AS error_time,
    '{error_payload.get("stage")}' AS stage,
    '{error_payload.get("error")}' AS error_message,
    '{dbutils.widgets.get("run_id")}' AS run_id
""")

# Send to notification destination
dbutils.notebook.exit(json.dumps({"status": "alerted"}))
```

**Best practices for notebook error handling:**
1. **Always use try/except** around I/O operations (reads, writes, API calls)
2. **Return structured JSON** from `dbutils.notebook.exit()` — include `status`, `error`, and any relevant metrics
3. **Use `raise` after `dbutils.notebook.exit()`** in except blocks — the exit sets the output, the raise marks the task as failed
4. **Separate error handling into dedicated notebooks** for complex pipelines
5. **Log errors to a Delta audit table** for post-mortem analysis
6. **Set timeouts** on `dbutils.notebook.run()` calls to prevent hung sub-notebooks

> 💡 **Exam Tip**: Know the difference between `dbutils.notebook.exit()` and `raise`. `exit()` sets the task's output value (accessible to downstream tasks via `{{tasks.key.notebook_output}}`), while `raise` marks the task as failed. You can use both in the same except block — `exit()` first to capture the error details, then `raise` to signal failure.

### If/Else Condition Tasks

A **condition task** adds branching logic to your job DAG. It evaluates an expression and routes execution to one of two downstream paths.

```
bronze_ingestion ──> quality_check [condition_task]
                         │
                    ┌────┴────┐
                    ▼         ▼
                if PASS    if FAIL
                    │         │
                    ▼         ▼
              gold_load    alert_team
```

**JSON representation in the job definition:**
```json
{
  "task_key": "quality_check",
  "condition_task": {
    "left": "{{tasks.bronze_ingestion.values.row_count}}",
    "op": "GREATER_THAN",
    "right": "0"
  },
  "depends_on": [{"task_key": "bronze_ingestion"}]
}
```

Pass an array of conditions in the job's `condition_task` definition — the task evaluates the expression and subsequent tasks depend on it with `outcome = "true"` or `outcome = "false"`.

**Defining downstream branches:**
```json
{
  "task_key": "gold_load",
  "depends_on": [{
    "task_key": "quality_check",
    "outcome": "true"    // Only runs if quality_check passes
  }],
  "notebook_task": {"notebook_path": "/notebooks/gold_load"}
},
{
  "task_key": "alert_team",
  "depends_on": [{
    "task_key": "quality_check",
    "outcome": "false"   // Only runs if quality_check fails
  }],
  "notebook_task": {"notebook_path": "/notebooks/alert_team"}
}
```

> 💡 **Exam Tip**: Condition tasks evaluate `(left OPERATOR right)`. Valid operators include `EQUAL_TO`, `NOT_EQUAL_TO`, `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`, `LESS_THAN`, `LESS_THAN_OR_EQUAL`, `IS_NULL`, `NOT_NULL`. The downstream task dependency must include `outcome = "true"` or `outcome = "false"` to select the correct branch.

### Parameterization

Parameters make pipelines reusable across environments and scenarios. Three mechanisms exist:

1. **`dbutils.widgets`** — Notebook-level widgets that accept text, dropdown, or multiselect input. Read with `dbutils.widgets.get("param_name")`.

2. **Job parameters** — Defined at the job level or per task. Accessed in notebooks via `dbutils.widgets.get()` when the job parameter is also declared as a widget.

3. **Dynamic value references** — In job definitions, you can reference runtime values from upstream tasks:
   ```
   {{tasks.bronze_ingestion.values.rows_written}}
   {{tasks.previous_task.notebook_output.my_var}}
   {{job.id}}
   {{job.run_id}}
   {{job.trigger_time}}
   ```

---

## Module 4.2: Implement Lakeflow Jobs

This module dives deep into the types of Lakeflow Jobs, how to create and configure them, trigger strategies, notification setup, and job-run management.

### Job Types

Lakeflow Jobs come in four flavors depending on how and how often they run:

#### 1. Scheduled Jobs
Run on a cron-based schedule. The job definition includes a `schedule` block with a **Quartz cron expression** (6-field format) and a timezone.

```json
{
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  }
}
```

This example runs every day at 2:00 AM UTC. The 6-field format is: `[Seconds] [Minutes] [Hours] [Day-of-Month] [Month] [Day-of-Week]` — unlike standard Unix cron (5 fields), Quartz includes a seconds field.

#### 2. Triggered Jobs (Event-Based)
Start automatically when a specific event occurs:
- **File arrival** — A new file appears in a cloud storage path (Azure Data Lake Storage Gen2, Blob Storage)
- **Table update** — A Delta table is updated (data inserted, updated, or deleted)

**File arrival trigger example:**
```json
{
  "trigger": {
    "file_arrival": {
      "url": "abfss://container@storage.dfs.core.windows.net/landing/",
      "min_time_between_triggers_seconds": 600
    }
  }
}
```

**Table update trigger example:**
```json
{
  "trigger": {
    "table_update": {
      "table_names": ["globstay_hospitality.bronze_bookings"],
      "min_time_between_triggers_seconds": 300
    }
  }
}
```

> 💡 **Exam Tip**: `min_time_between_triggers_seconds` prevents runaway re-triggers. If a job writes back to the same table it's watching, the pipeline could loop infinitely without this guard.

#### 3. Continuous Jobs (Streaming)
Run permanently as a streaming application. The cluster stays alive and processes new data as it arrives. Commonly used with Lakeflow Declarative Pipelines that use `readStream` / `@dlt.table` with streaming sources.

```json
{
  "continuous": {
    "pause_status": "UNPAUSED"
  }
}
```

Continuous jobs use **serverless** or **job clusters** that auto-terminate only on failure — they never stop while running smoothly.

#### 4. One-Time (Manual) Jobs
Run once on demand — through the UI, API, or CLI. Used for backfills, ad-hoc reprocessing, and testing.

```bash
databricks jobs run-now --job-id 1234
databricks jobs run-now --job-id 1234 --jar-params '["--date","2025-01-01"]'
```

> 🔬 **Lab 11 – TelConnect (45 min)**: In this lab you create a Lakeflow Job that processes CDR (Call Detail Record) data for a telecom company. You configure a file-arrival trigger on a landing storage path, pass job parameters to the notebook using `{{param}}` dynamic syntax, add a nightly cron schedule as a fallback, set up Slack failure notifications, and configure retry policies with exponential backoff.

### Task Dependencies (DAG)

Tasks in a Lakeflow Job must be assembled into a DAG — a **Directed Acyclic Graph**. Each task lists its upstream dependencies in the `depends_on` array. The platform computes the execution order automatically.

**Rules:**
- If a task has no `depends_on`, it's a root task and runs immediately when the job starts.
- If a task depends on multiple parents, all parents must succeed before it starts.
- A task with multiple children fans out — children run in parallel.
- Cycles are not allowed (that's what makes it a DAG).

```json
"tasks": [
  {
    "task_key": "extract",
    "notebook_task": {"notebook_path": "/notebooks/extract"}
  },
  {
    "task_key": "validate",
    "notebook_task": {"notebook_path": "/notebooks/validate"},
    "depends_on": [{"task_key": "extract"}]
  },
  {
    "task_key": "transform_a",
    "notebook_task": {"notebook_path": "/notebooks/transform_a"},
    "depends_on": [{"task_key": "validate"}]
  },
  {
    "task_key": "transform_b",
    "notebook_task": {"notebook_path": "/notebooks/transform_b"},
    "depends_on": [{"task_key": "validate"}]
  },
  {
    "task_key": "load",
    "notebook_task": {"notebook_path": "/notebooks/load"},
    "depends_on": [
      {"task_key": "transform_a"},
      {"task_key": "transform_b"}
    ]
  }
]
```

In this DAG:
1. `extract` runs first.
2. `validate` runs after extract succeeds.
3. `transform_a` and `transform_b` run in parallel after validation.
4. `load` runs after both transforms finish.

#### DAG Design Patterns

Several common DAG patterns are useful for pipeline design:

**Fan-out/Fan-in pattern:**
```
          ┌──> transform_a ──┐
extract ──┤                  ├──> load
          └──> transform_b ──┘
```
- One upstream task fans out to N parallel tasks
- A downstream task fans in (waits for all N to complete)
- Efficient use of parallel compute resources
- Use when stages are independent (e.g., loading separate dimensions)

**Linear chain pattern:**
```
extract ──> validate ──> transform ──> load
```
- Each task depends on the previous one
- Simple, predictable execution order
- Slowest because no parallelism
- Use when each stage requires the complete output of the prior stage

**Condition branch pattern:**
```
          ┌──> gold_report (if quality passes)
extract ──┤
          └──> alert_team (if quality fails)
```
- Task → condition task → true/false branches
- Implemented with `condition_task` and `outcome` dependencies
- True path: continue to business-as-usual processing
- False path: run diagnostics, send alerts, or quarantine data

**Hybrid pattern (common in production):**
```
          ┌──> dim_customer ──┐
          │                   │
bronze ───┼──> dim_product ───┤──> gold_sales
          │                   │
          └──> fct_sales ─────┘
              │
              └──> quality_check ──> alert (if fails)
```
- Combines fan-out/fan-in for data processing with a condition branch for quality governance
- Most real-world pipelines use this hybrid approach

> 💡 **Exam Tip**: The exam may present a DAG diagram and ask you to identify the pattern or determine task dependencies. Understand that fan-out/fan-in enables parallelism, linear chains enforce sequential execution, and condition branches enable if/else logic. The hybrid pattern is the most common in production.

### Job Parameters and Dynamic Value References

**Job parameters** are key-value pairs defined at the job level. They can be overridden at run time.

```json
{
  "parameters": [
    {"name": "env", "default": "dev"},
    {"name": "source_path", "default": "abfss://landing@storage.dfs.core.windows.net/input/"},
    {"name": "target_table", "default": "globstay_hospitality.bronze_bookings"}
  ]
}
```

To access these in a notebook, declare matching widgets:
```python
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("target_table", "")
```

**Dynamic value references** use `{{ ... }}` syntax to inject runtime values elsewhere in the job configuration — for example, to pass an upstream task's output as a parameter to a downstream task:

```
{{tasks.extract.values.row_count}}
{{tasks.extract.notebook_output.my_var}}
{{job.id}}
{{job.run_id}}
{{job.trigger_time}}
{{job.trigger}}
```

### Cluster Selection for Job Tasks

Each task runs on a compute resource. Three options:

1. **Serverless compute** — Fully managed, no cluster configuration needed. Auto-scales from zero. Fastest startup (sub-second). Recommended for most workloads. Available only in certain regions.

2. **Job clusters** — Ephemeral clusters created for the job run and terminated when the job finishes. Configured in the job definition. Good for consistent, predictable workloads.

3. **Existing all-purpose clusters** — A permanent, shared cluster. Tasks run on the same cluster, so you must manage concurrency. Not recommended for production jobs because of resource contention.

> 💡 **Exam Tip**: The question may ask which compute to use. Serverless (fast startup, no management) is best for interactive/ad-hoc. Job clusters are best for scheduled production pipelines. Existing clusters are generally discouraged for automated jobs.

### Notifications

Notifications are configured in the job definition under `email_notifications` and `notification_settings`:

```json
{
  "email_notifications": {
    "on_failure": ["data-team-alerts@example.com"],
    "on_success": [],
    "no_alert_for_skipped_runs": false
  }
}
```

For Slack, PagerDuty, or webhooks, create a **notification destination** at the workspace level (Settings → Notifications → Notification Destinations), then reference it in the job:

```json
{
  "notification_settings": {
    "no_alert_for_skipped_runs": false,
    "no_alert_for_canceled_runs": false,
    "alert_on_last_attempt": true
  }
}
```

> 💡 **Exam Tip**: Remember that `email_notifications` supports separate lists for `on_failure`, `on_success`, and `on_start`. For third-party integrations (Slack, PagerDuty), you must configure a notification destination in workspace settings first — you can't just paste a webhook URL.

### Run Lifecycle

**Job run states:**

```
PENDING → RUNNING → TERMINATING → SUCCESS
                   → SKIPPED
                   → FAILED (can retry)
                   → TIMEDOUT
                   → CANCELED (manual)
```



#### Repair vs Rerun

**Repair:** Re-runs only the failed or canceled tasks of a multi-task job. Successful task outputs are preserved and not recomputed. This is ideal for long pipelines (e.g., 10-stage ETL) where only stage 7 failed — don't redo stages 1-6.

**Rerun:** Runs the entire job from scratch. All task outputs are discarded and recomputed. Use when data in upstream stages has changed or a structural fix is needed.

```bash
# Repair: only re-run failed tasks
databricks runs repair --run-id 12345

# Repair from a specific task onward
databricks runs repair --run-id 12345 --rerun-from-task-key silver_transform

# Rerun the entire job
databricks jobs run-now --job-id 6789
```

**When to use which:**

| Scenario | Recommended Action | Rationale |
|----------|-------------------|-----------|
| Transient failure (network blip, service timeout) | **Repair** | Data in earlier stages is valid; only the failed task needs re-execution |
| Logic bug fixed in a single task | **Repair** | If only that task failed and upstream data is unaffected |
| Schema change in upstream source | **Rerun** | All stages need fresh data with the new schema |
| Partial data written before failure | **Repair** (with caution) | Check data consistency first — repair may resume writing from checkpoint |
| Security patch or cluster config change | **Rerun** | All tasks need to run on the updated infrastructure |
| Data quality rule change in bronze layer | **Rerun** | Downstream tasks depend on the corrected bronze data |

**Repair with `--rerun-from-task-key`:** When you specify a task key, the repair re-runs that task and any downstream tasks that depend on it (or were skipped because of its failure). Tasks that executed successfully and are not in the downstream path of the specified task keep their results.

```python
# Repair via API (Python)
import requests

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]

response = requests.post(
    f"https://{DATABRICKS_HOST}/api/2.0/jobs/runs/repair",
    headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
    json={
        "run_id": 12345,
        "rerun_from_task_key": "silver_transform"
    }
)
print(f"Repair run ID: {response.json()['repair_run_id']}")
```

> 💡 **Exam Tip**: The exam will test whether you understand the difference between repair and rerun. Key point: **Repair preserves successful task outputs; rerun discards everything.** If a question says "only one task failed in a 20-task pipeline," the answer is repair — not rerun. The `--rerun-from-task-key` flag lets you start the repair from a specific task onward.

**Run history:** Every run is logged with start time, duration, status, task-level breakdown, and a link to the Spark UI for each task.

### Permissions

Lakeflow Jobs support a granular permission model:

| Permission Level | Can View | Can Run | Can Edit | Can Manage |
|---|---|---|---|---|
| **Can View** | ✅ | ❌ | ❌ | ❌ |
| **Can Manage Run** | ✅ | ✅ | ✅ | ❌ |
| **Can Manage** | ✅ | ✅ | ✅ | ✅ |

- **Can Manage Run**: The user can start, stop, and repair runs, but cannot modify the job definition.
- **Can Manage**: Full control — edit the job, change permissions, delete the job.

---

## Module 4.3: Implement Development Lifecycle Processes

This module covers how to bring software engineering rigor to your data pipelines — Git integration, testing, and **Databricks Asset Bundles (DAB)** for infrastructure-as-code deployments across dev, staging, and production.

### Git Integration with Git Folders

Azure Databricks **Git folders** let you link a workspace directory to a remote Git repository. The workspace becomes a Git client — you can pull, commit, push, create branches, and open pull requests without leaving the Databricks UI.

**Supported Git providers:**
- Azure DevOps (Azure Repos)
- GitHub / GitHub Enterprise
- GitLab / GitLab Enterprise
- Bitbucket Cloud / Bitbucket Server

**Setup process:**
1. In User Settings → Git Integration, link your Git account (OAuth or personal access token).
2. In the Workspace UI, select a folder → Git → Link to Remote Repo.
3. Choose the provider, repository, and branch.
4. After linking, the folder shows the current branch, provides Pull and Push buttons, and tracks file status (modified, added, deleted).

**Branching workflow example:**

```
main ───── feature/quality-enhancements
              │
              ├── Pull from origin
              ├── Edit notebooks/transform.py
              ├── Git → Commit → "Add null-check constraints"
              ├── Git → Push
              └── Create Pull Request → Review → Merge to main
```

> ⚠️ **Warning**: Git folders can only be linked to one branch at a time. To work on multiple branches, use multiple folders (e.g., `/Users/alice/main/` and `/Users/alice/feature/`) or switch branches in the Git folder panel.

### Branching Strategies

Three common patterns:

1. **Git Flow** — `main`, `develop`, `feature/*`, `release/*`, `hotfix/*`. Most structured, best for teams with scheduled releases. Can be heavy for small teams.

2. **GitHub Flow** — `main` plus short-lived `feature/*` branches. Merge to main via PR, deploy from main immediately. Simpler, common for CI/CD pipelines.

3. **Trunk-Based Development** — All developers commit to `main` multiple times a day. Short-lived feature flags control what's exposed. Fastest iteration, most automated testing required.

> 💡 **Exam Tip**: For exam purposes, know that GitHub Flow (main + feature branches with PRs into main) is the recommended starting point for Databricks projects. The exam may ask about resolving merge conflicts in Git folders — the resolution is similar to any Git merge: pull the latest, resolve conflict markers, commit the resolution.

### Testing Strategy for Data Pipelines

Data pipelines need a layered testing approach:

| Test Type | What It Tests | When | Tools |
|---|---|---|---|
| **Unit** | Individual functions, transforms, Spark UDFs | Before commit | pytest, chispa (PySpark DataFrame assertions), great_expectations |
| **Integration** | Pipeline stage interaction with real/canned data sources | After commit, pre-deploy to staging | Custom test notebooks, Lakeflow Declarative Pipeline expectations |
| **End-to-End** | Full pipeline from source to gold table with production-like data volumes | Pre-release | Lakeflow Jobs with sample data sets |
| **UAT / Data Quality** | Business rule validation on output tables | Post-deployment, ongoing | Lakeflow Declarative Pipeline constraints, Great Expectations, Delta Table history |

**Example unit test with chispa:**
```python
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from transforms import clean_customer_email

def test_email_cleaning(spark: SparkSession):
    input_df = spark.createDataFrame([
        (1, "  ALICE@EXAMPLE.COM  "),
        (2, "BOB@EXAMPLE"),
    ], ["id", "email"])
    
    expected_df = spark.createDataFrame([
        (1, "alice@example.com"),
        (2, None),
    ], ["id", "email"])
    
    result_df = clean_customer_email(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)
```

**Integration test example (test notebook):**
```python
# %run /testing/test_helpers
from test_helpers import run_pipeline_stage

# Arrange: seed test data
spark.sql("DROP TABLE IF EXISTS test_bronze")
spark.sql("""
  CREATE TABLE test_bronze
  USING delta
  AS SELECT 1 AS id, 'test@example.com' AS email
""")

# Act: run the silver transform notebook
run_pipeline_stage("/pipelines/globstay/silver_transform", {
    "source_table": "test_bronze",
    "target_table": "test_silver"
})

# Assert
result = spark.sql("SELECT COUNT(*) cnt FROM test_silver WHERE email IS NOT NULL")
assert result.collect()[0]["cnt"] == 1
```

**Additional unit test example with chispa (filter test):**
```python
from chispa import assert_basic_rows_are_equal

def test_filter_valid_customers(spark):
    input_df = spark.createDataFrame([
        (1, "alice@example.com", "active"),
        (2, None, "active"),
        (3, "bob@example.com", "inactive"),
    ], ["id", "email", "status"])
    
    result_df = filter_active_customers(input_df)
    expected_df = spark.createDataFrame([
        (1, "alice@example.com", "active"),
    ], ["id", "email", "status"])
    
    assert_basic_rows_are_equal(result_df, expected_df)
```

**chispa assertion functions reference:**
| Function | Use Case |
|----------|----------|
| `assert_df_equality` | Full DataFrame comparison (schema + data) |
| `assert_basic_rows_are_equal` | Row data comparison (ignores schema details) |
| `assert_approx_df_equality` | Floating-point tolerant comparison |
| `assert_contains_match` | Check that expected rows exist in the result |
| `assert_column_equality` | Compare a single column's values |

**Testing with Great Expectations:**
Great Expectations (GX) provides a framework for defining, validating, and documenting data quality expectations. It integrates with Databricks notebooks and Lakeflow Declarative Pipelines.

```python
import great_expectations as gx

# Create a Great Expectations context
gx_context = gx.data_context.DataContext()

# Connect to a Spark DataFrame as a data source
datasource = gx_context.sources.pandas_datasource("sales_datasource")

# Define an expectation suite
expectation_suite = gx_context.create_expectation_suite("sales_suite")
expectation_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="amount", min_value=0, max_value=100000
    )
)
expectation_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
)
expectation_suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="transaction_id")
)

# Validate a DataFrame
results = gx_context.run_validation(
    expectation_suite=expectation_suite,
    batch=datasource.get_batch(data=spark_df)
)

# Check results
if not results["success"]:
    print(f"Data quality validation failed: {results['statistics']['unsuccessful_expectations']} failures")
    # Route to error handling or quarantine
```

**Testing approach comparison:**
| Approach | Best For | When to Use |
|----------|----------|-------------|
| chispa (PySpark) | Unit testing transforms, UDFs, filters | Pre-commit, CI pipeline validation |
| Great Expectations | Data quality validation at pipeline boundaries | Post-deployment, monitoring |
| Lakeflow DLT constraints | Row-level quality enforcement within pipelines | Inline during pipeline execution |
| Custom assertion notebooks | Integration and end-to-end testing | Pre-release, staging environment |

### Databricks Asset Bundles (DAB)

**Databricks Asset Bundles** (also called **DAB** or **Declarative Automation Bundles**) are a YAML-based infrastructure-as-code framework. You define your entire Databricks workspace resources — jobs, pipelines, notebooks, clusters, schemas, external locations — in a `databricks.yml` file, then deploy them across environments with the Databricks CLI.

> 💡 **Exam Tip**: DAB is the **recommended deployment method** for production Databricks workloads. The exam emphasizes understanding the bundle structure, the `targets` concept, the `${var}` / `{{ var }}` syntax, and the CLI commands.

#### Bundle Structure

```
globstay_pipeline/
├── databricks.yml          # Main bundle definition
├── resources/
│   └── jobs/
│       └── globstay_etl_job.json   # Job resource definitions
├── src/
│   ├── bronze_ingestion.py
│   ├── silver_transform.py
│   └── gold_aggregate.py
├── databricks_template_schema.json  # Custom variables schema (optional)
└── .databricks/
    └── bundle.json          # Generated after deployment
```

#### The `databricks.yml` Configuration

```yaml
# databricks.yml
bundle:
  name: globstay_pipeline
  # Run in development mode by default: auto-generates 
  # user-scoped names to avoid collisions
  # In production mode, uses the exact resource names

targets:
  dev:
    mode: development        # Adds user prefix to resources
    default: true            # Default target when deploy is called
    workspace:
      host: https://adb-123456.7.azuredatabricks.net
      
  staging:
    mode: development
    workspace:
      host: https://adb-789012.7.azuredatabricks.net
      
  prod:
    mode: production         # Exact resource names — no prefix
    workspace:
      host: https://adb-345678.7.azuredatabricks.net

artifacts:
  whl:
    type: whl
    build: poetry build
    path: ./src

resources:
  jobs:
    globstay_etl_job:
      name: globstay_etl_job
      # Pass deployment variables to the job definition
      # ${bundle.target} resolves to "dev", "staging", or "prod"
      # ${workspace.current_user.username} resolves at deploy time
      
      schedule:
        quartz_cron_expression: 0 0 2 * * ?
        timezone_id: UTC
        
      email_notifications:
        on_failure:
          - alerts@example.com
          
      tasks:
        - task_key: bronze_ingestion
          notebook_task:
            notebook_path: /Users/${workspace.current_user.username}/notebooks/bronze_ingestion
          job_cluster_key: etl_cluster
          
        - task_key: silver_transform
          depends_on:
            - task_key: bronze_ingestion
          notebook_task:
            notebook_path: /Users/${workspace.current_user.username}/notebooks/silver_transform
          job_cluster_key: etl_cluster
          
        - task_key: gold_aggregate
          depends_on:
            - task_key: silver_transform
          notebook_task:
            notebook_path: /Users/${workspace.current_user.username}/notebooks/gold_aggregate
          job_cluster_key: etl_cluster
          
      job_clusters:
        - job_cluster_key: etl_cluster
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 4
            data_security_mode: SINGLE_USER
            azure_attributes:
              first_on_demand: 1
              availability: ON_DEMAND_AZURE
```

#### Bundle Template Variables

DAB supports variable substitution in two syntaxes:

| Syntax | Scope | Example |
|---|---|---|
| **`${var.name}`** | Custom variables defined in `databricks_template_schema.json` or `databricks.yml` `variables` block | `${var.source_path}`, `${var.environment}` |
| **`${workspace.current_user.username}`** | Resolved at deploy time from the workspace | `/Users/${workspace.current_user.username}/pipelines/` |
| **`${bundle.target}`** | Current deployment target name | `dev`, `staging`, `prod` |
| **`{{ tasks.extract.values.row_count }}`** | Runtime (job execution time) values | Only used inside job task configurations |

**Custom variables example:**

In `databricks_template_schema.json`:
```json
{
  "properties": {
    "source_path": {
      "type": "string",
      "default": "abfss://landing@storage.dfs.core.windows.net/input/"
    },
    "bronze_table": {
      "type": "string",
      "default": "bronze_bookings"
    }
  }
}
```

In the YAML:
```yaml
variables:
  source_path:
    default: "abfss://landing@storage.dfs.core.windows.net/input/"
  bronze_table:
    default: bronze_bookings

resources:
  jobs:
    my_job:
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: /pipelines/ingest
            base_parameters:
              source: ${var.source_path}
              table: ${var.bronze_table}
```

#### Databricks CLI Bundle Commands

```bash
# Initialize a new bundle from a template
databricks bundle init globstay_pipeline

# Validate the bundle YAML (syntax check + variable resolution)
databricks bundle validate

# Deploy resources to a target environment
databricks bundle deploy --target dev

# Run a job defined in the bundle
databricks bundle run globstay_etl_job --target dev

# Deploy with parameter overrides
databricks bundle deploy --target prod \
  --var source_path="abfss://prod-landing@storage.dfs.core.windows.net/input/"

# Destroy deployed resources
databricks bundle destroy --target dev

# Generate a deployment summary in JSON
databricks bundle summary --target dev

# Re-generate bundle config from scratch
databricks bundle generate --existing-job-id 1234
```

#### Deploying Bundles via REST API

Instead of using the Databricks CLI, you can deploy bundles using the Databricks REST API. This is useful for:
- CI/CD pipelines where installing the CLI is not possible or practical
- Custom automation tools that need programmatic control
- Cross-platform deployment scripts (PowerShell, Python, Node.js)
- Environments with restricted shell access

**Key API Endpoints for Bundles:**

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| Deploy | POST `/api/2.0/bundle/deploy` | Deploy a bundle to a target environment |
| Validate | POST `/api/2.0/bundle/validate` | Validate bundle configuration without deploying |
| Delete | POST `/api/2.0/bundle/delete` | Remove a deployed bundle |
| Run | POST `/api/2.0/jobs/run-now` | Trigger a job run (existing job, not bundle-specific) |

**Deploy via API (curl example):**
```bash
curl --request POST \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://github.com/myorg/databricks-bundle/archive/main.zip",
    "branch": "main",
    "target": "prod"
  }' \
  "https://$DATABRICKS_HOST/api/2.0/bundle/deploy"
```

**Use in Azure DevOps CI/CD (PowerShell):**
```powershell
# PowerShell script for Azure DevOps pipeline task
$body = @{
    url = "$(Build.ArtifactStagingDirectory)/bundle.zip"
    target = "prod"
} | ConvertTo-Json

Invoke-RestMethod `
    -Method Post `
    -Uri "https://$env:DATABRICKS_HOST/api/2.0/bundle/deploy" `
    -Headers @{Authorization = "Bearer $env:DATABRICKS_TOKEN"} `
    -Body $body `
    -ContentType "application/json"
```

**Use in GitHub Actions (Python script):**
```python
import os
import requests

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]

response = requests.post(
    f"https://{DATABRICKS_HOST}/api/2.0/bundle/deploy",
    headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
    json={
        "url": "https://github.com/myorg/databricks-bundle/archive/main.zip",
        "target": "prod"
    }
)
response.raise_for_status()
print(f"Deployment ID: {response.json()['deployment_id']}")
```

**Validate before deploy:**
```bash
curl -X POST \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"target": "staging"}' \
  "https://$DATABRICKS_HOST/api/2.0/bundle/validate"
```

> 💡 **Exam Tip**: The exam may ask which approach to use for bundle deployment. Answer: **CLI** for most cases (simpler, less code) but **REST API** when you need custom integration, the CLI is not available, or you're using a CI/CD platform that doesn't support the CLI well. Both the bundle operations (validate, deploy, delete) and job-run operations (run-now) are available via both methods.

> 🔬 **Lab 12 – Dev Lifecycle Processes (45 min)**: In this lab you link Databricks workspace folders to Git repos, create feature branches, make commits, raise PRs, and resolve merge conflicts. You then create a Databricks Asset Bundle (DAB) with `databricks.yml` targeting dev, staging, and prod. Finally, you deploy the bundle to each environment using the Databricks CLI and validate the deployment by running the job.

### CI/CD Integration

DAB integrates naturally with CI/CD pipelines. Here's a GitHub Actions workflow:

```yaml
# .github/workflows/deploy.yml
name: Deploy Databricks Bundle

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main

      - name: Validate Bundle
        run: databricks bundle validate

  deploy-prod:
    if: github.ref == 'refs/heads/main'
    needs: validate
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4

      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main

      - name: Configure Databricks CLI
        run: |
          databricks configure --token
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_PROD }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_PROD }}

      - name: Deploy Bundle
        run: databricks bundle deploy --target prod

      - name: Run Job
        run: databricks bundle run globstay_etl_job --target prod
```

For **Azure DevOps**:
```yaml
# azure-pipelines.yml
trigger:
  branches:
    include:
    - main

variables:
  databricks_host_prod: $(DATABRICKS_HOST_PROD)
  databricks_token_prod: $(DATABRICKS_TOKEN_PROD)

steps:
- script: |
    pip install databricks-cli
    databricks configure --token <<< $(databricks_token_prod)
    databricks bundle validate
  displayName: 'Validate Bundle'

- script: |
    databricks bundle deploy --target prod
    databricks bundle run globstay_etl_job --target prod
  displayName: 'Deploy & Run'
  condition: eq(variables['Build.SourceBranch'], 'refs/heads/main')
```

> 💡 **Exam Tip**: Know the `databricks bundle` CLI commands and their order: `init` → `validate` → `deploy` → `run`. Also know the two bundle modes: `development` (adds user prefix to resource names for isolation) and `production` (uses exact resource names).

---

## Module 4.4: Monitor, Troubleshoot, and Optimize Workloads

This module covers how to keep your pipelines healthy — what to watch, where to look when things break, and how to make pipelines faster and cheaper.

### Cluster Monitoring

Every Databricks cluster emits rich telemetry:

- **Cluster metrics** in the UI: CPU utilization, memory usage, executor count, shuffle spills, and I/O rates.
- **Auto-termination**: Configure a timeout for idle clusters to save costs.
  ```json
  {
    "autotermination_minutes": 30
  }
  ```
- **Cluster policies**: Enforce constraints on cluster configuration — max workers, allowed instance types, tags. Prevent runaway costs.
- **Event logs**: Every cluster state change (CREATING, RUNNING, RESIZING, TERMINATING) is recorded in the cluster event log with timestamps.
- **Budget policies**: Azure cost management budgets that trigger alerts or block cluster creation when spend exceeds thresholds.

### Job Monitoring

The **Job Runs** page in the workspace UI shows:

- **Run history** — All past runs with status, duration, triggering user/event
- **Task-level breakdown** — Expand any run to see individual task status, duration, logs
- **Repair** button — Re-run failed tasks without restarting the whole job
- **Export** — Download run metadata as JSON

**API-driven monitoring:**
```bash
# List recent runs
databricks runs list --job-id 1234 --max-results 10

# Get run details (including task status)
databricks runs get --run-id 5678

# Export run data for external monitoring
databricks runs export --run-id 5678 --views-all
```

### Spark UI Diagnosis

The **Spark UI** is the single most powerful diagnostic tool in the Databricks platform. It's accessible from the "Spark UI" button on any notebook cell, job run, or cluster.

**Key tabs:**

| Tab | What It Shows | Diagnose This |
|---|---|---|
| **SQL** | Each DataFrame/Dataset operation as a physical plan node. Shows input rows, duration, scan metrics. | Which stage consumes the most time? Are we reading more data than expected? |
| **Jobs** | High-level Spark jobs triggered by actions (`.show()`, `.write()`, `.collect()`). Not the same as Lakeflow Jobs. | How many jobs are created? Are they all necessary? |
| **Stages** | Each stage within a job. Shows number of tasks, shuffle read/write, spill metrics, speculative execution. | **Data skew**: Some tasks take much longer than others. **Shuffle**: Excessive shuffle read/write. |
| **Executors** | Per-executor metrics — memory, disk, shuffle, tasks completed, failed tasks. | **Spill**: Any executor shows spill to disk (memory pressure). **Failed tasks**: Executors crashing. |
| **Storage** | Contents of the RDD cache / persisted DataFrames. Shows cache hit ratios and memory usage. | Are our cached objects being evicted? Is caching helping? |

### Common Performance Issues

#### 1. Data Skew

**Symptoms**: In the Stages tab, most tasks finish quickly but a few tasks take 10x longer. One or two tasks process the majority of data.

**Causes**: A join or group-by key has a highly unbalanced distribution (e.g., joining on `country` when 90% of rows are "US").

**Fixes:**
- **Salting**: Add a random suffix to the skewed key to distribute across more partitions.
- **Adaptive Query Execution (AQE)** skew join: Enabled by default in Databricks Runtime 12.0+.
- **Broadcast join**: Push the smaller side of the join to all executors, eliminating shuffle.

#### 2. Excessive Shuffle

**Symptoms**: Spark UI shows large `Shuffle Write` and `Shuffle Read` metrics. The Shuffle stage dominates total job time.

**Causes**: Wide transformations (`.groupBy()`, `.join()`, `.distinct()`, `.repartition()`) that require data movement across the network.

**Fixes:**
- Reduce partitions before a wide transformation with `.coalesce()`.
- Use **broadcast joins** when one side is small (< 10 MB default threshold).
- Use **bucketing** to pre-organize data by join key (eliminates shuffle on subsequent joins).

#### 3. Spill to Disk

**Symptoms**: Stages tab shows `Spill (memory)` or `Spill (disk)` columns with non-zero values. Executor logs show memory pressure.

**Causes**: An executor's partition doesn't fit in memory — Spark writes intermediate data to disk.

**Fixes:**
- Increase executor memory (`spark.executor.memory`).
- Increase `spark.sql.files.maxPartitionBytes` (default 128 MB) to reduce partition count within each task.
- Increase `spark.sql.shuffle.partitions` (default 200) to make each partition smaller.
- Cache the DataFrame if it's reused across multiple queries.

#### 4. Small File Problem

**Symptoms**: Pipeline takes a long time despite small data volumes. Delta table directories contain thousands of tiny Parquet files.

**Causes**: Streaming micro-batches, frequent overwrites, or `coalesce(1)` patterns that create many small files.

**Fixes:**
- **OPTIMIZE** with **ZORDER** to compact small files:
  ```sql
  OPTIMIZE globstay_hospitality.bronze_bookings
  ZORDER BY (booking_id);
  ```
- Use `maxRecordsPerFile` to control output file size:
  ```python
  df.write \
    .option("maxRecordsPerFile", 1000000) \
    .saveAsTable("bronze_bookings")
  ```
- In streaming, use **auto-compaction** and **optimized writes**:
  ```python
  spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
  spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
  ```

### Adaptive Query Execution (AQE)

AQE is **enabled by default** in Databricks Runtime 12.0+. It dynamically optimizes query execution at runtime using statistics collected during earlier stages.

**Three key optimizations:**

1. **Dynamic coalescing of shuffle partitions**: If AQE detects that some partitions have very little data, it merges them to reduce the number of tasks and the scheduling overhead.
   ```sql
   SET spark.sql.adaptive.coalescePartitions.enabled = true;
   ```

2. **Dynamic skew join**: AQE detects when a join key is skewed and handles the skewed partition separately by splitting it into sub-partitions (salting), avoiding the single-executor bottleneck.
   ```sql
   SET spark.sql.adaptive.skewJoin.enabled = true;
   ```

3. **Dynamic switching of join strategies**: AQE switches from SortMergeJoin to BroadcastHashJoin at runtime if one side of the join turns out to be small enough.
   ```sql
   SET spark.sql.adaptive.localShuffleReader.enabled = true;
   ```

**AQE configuration best practices:**
```sql
SET spark.sql.adaptive.enabled = true;                                -- Master switch
SET spark.sql.adaptive.coalescePartitions.enabled = true;              -- Merge small partitions
SET spark.sql.adaptive.coalescePartitions.parallelismFirst = false;    -- Prioritize size over parallelism
SET spark.sql.adaptive.skewJoin.enabled = true;                        -- Auto-fix data skew
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 128MB;           -- Target partition size
```

> 💡 **Exam Tip**: AQE is enabled by default in modern Databricks Runtime. A question may present a scenario with a skewed join and ask how to fix it — the answer could be "enable AQE" or, if AQE is already on, "adjust `spark.sql.adaptive.advisoryPartitionSizeInBytes`".

### Broadcast Joins

A **broadcast join** sends a small table to every executor, avoiding the expensive shuffle of a standard SortMergeJoin. Spark automatically broadcasts tables under 10 MB (configurable via `spark.sql.autoBroadcastJoinThreshold`).

**Explicit hint syntax (SQL):**
```sql
SELECT /*+ BROADCAST(dim_table) */ *
FROM fact_table f
JOIN dim_table d ON f.dim_key = d.key
```

**Programmatic (PySpark):**
```python
from pyspark.sql.functions import broadcast

result = large_df.join(broadcast(small_df), "key")
```

**Configuring the threshold:**
```sql
-- Increase auto-broadcast threshold to 100 MB
SET spark.sql.autoBroadcastJoinThreshold = 104857600;  -- 100 MB in bytes
```

> ⚠️ **Warning**: Setting `spark.sql.autoBroadcastJoinThreshold` too high can cause driver OOM errors because the driver materializes the broadcast table in memory. Keep it under 200 MB unless you're sure about the driver's memory.

### Cache Management

Caching avoids recomputing an expensive DataFrame across multiple queries. Three approaches:

```python
# 1. In-memory cache (cache is lazy — evaluated on first action)
df = spark.sql("SELECT * FROM silver_bookings WHERE total_amount > 1000")
df.cache()
df.count()  # This triggers the cache

# 2. Persist with storage level
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)

# 3. SQL
CACHE TABLE cached_bookings AS
SELECT * FROM silver_bookings WHERE total_amount > 1000;
```

**Storage levels:**

| Level | Memory | Disk | Off-Heap | Serialized | When to Use |
|---|---|---|---|---|---|
| `MEMORY_ONLY` | ✅ | ❌ | ❌ | ❌ | Small datasets that fit in memory |
| `MEMORY_ONLY_SER` | ✅ | ❌ | ❌ | ✅ | Memory-constrained, CPU for (de)serialization is acceptable |
| `MEMORY_AND_DISK` | ✅ | ✅ (if spill) | ❌ | ❌ | Safe default — fits in memory, if not spills to disk |
| `DISK_ONLY` | ❌ | ✅ | ❌ | ✅ | Very large dataset, never fits in memory |
| `OFF_HEAP` | ❌ | ❌ | ✅ | ✅ | Dedicated off-heap memory region |

**Clearing cache:**
```sql
UNCACHE TABLE cached_bookings;
```
```python
df.unpersist()
# Or clear all cached data
spark.catalog.clearCache()
```

### Log Streaming and Diagnostics

Databricks can stream diagnostic logs to **Azure Log Analytics Workspace** or **Azure Storage** for long-term retention and analysis.

**Configuration steps:**
1. Enable **diagnostic settings** on the Databricks workspace in the Azure Portal.
2. Select log categories: `jobRun`, `clusterEvent`, `notebook`, `dbfs`, `deltaPipeline`.
3. Choose destination: Log Analytics workspace or storage account.
4. Use **KQL (Kusto Query Language)** to query job failures:
   ```kql
   databricks_jobs
   | where RunStatus == "FAILED"
   | summarize FailureCount = count() by JobName, bin(TimeGenerated, 1h)
   | order by FailureCount desc
   ```

**Diagnostic log categories:**

| Category | Contains | Use For |
|---|---|---|
| `jobRun` | Job start, end, status, duration, task breakdown | Pipeline monitoring, SLA tracking |
| `clusterEvent` | Cluster start, termination, resize, autoscale | Cost tracking, spotting cluster crashes |
| `notebook` | Notebook execution, cell output, errors | Debugging failed notebook steps |
| `deltaPipeline` | Lakeflow Declarative Pipeline events, data quality constraint violations | Data quality dashboards |
| `dbfs` | DBFS file operations | Auditing file access |

### Configuring Azure Monitor Alerts for Databricks

Beyond job-level email/Slack notifications, you can configure **Azure Monitor alerts** for broader observability across all your Databricks workloads:

**1. Metric Alerts** (real-time threshold-based):
- Failed job runs in the last 5 minutes > 0
- Cluster CPU utilization > 90% for 15 minutes
- Pipeline data quality violations exceeding threshold
- Job duration exceeding SLA (e.g., > 60 minutes)

**2. Log Alerts** (KQL-based queries against Log Analytics):
Queries the diagnostic log data for complex conditions:
```kql
// Alert rule: High job failure rate in the last hour
databricks_jobs
| where TimeGenerated > ago(1h)
| where RunStatus == "FAILED"
| summarize FailureCount = count() by bin(TimeGenerated, 5min)
| where FailureCount > 5
```

```kql
// Alert rule: Cluster auto-termination events (spot instance preemption)
databricks_clusterEvents
| where TimeGenerated > ago(1h)
| where EventType == "TERMINATING"
| where StatusMessage contains "spot" or StatusMessage contains "preempt"
| project ClusterName, TimeGenerated, StatusMessage
```

**3. Activity Log Alerts:**
Alert on workspace-level administrative operations:
- Cluster creation or deletion
- Permission changes on jobs or pipelines
- Workspace setting modifications

**Setting up alerts:**
1. In Azure Portal → **Monitor** → **Alerts** → **Create**
2. **Select scope** (Azure Databricks workspace resource)
3. **Choose signal** (e.g., "Failed job runs", custom log search)
4. **Define threshold** and evaluation frequency (e.g., every 5 minutes)
5. **Configure action group** (email, SMS, webhook, ITSM connector, Logic App)

**Action group destinations:**
| Destination | Use Case |
|-------------|----------|
| Email/SMS | Direct notification to on-call engineers |
| Webhook | Trigger custom automation (e.g., restart a job) |
| ITSM connector | Create tickets in ServiceNow, Jira |
| Logic App | Orchestrate complex responses (e.g., scale up cluster) |
| Azure Function | Run remediation code |

> 💡 **Exam Tip**: Job-level notifications (email_notifications, Slack webhooks) alert on individual job failures. Azure Monitor alerts provide cross-job, infrastructure-level monitoring. Use **both together** for comprehensive coverage. Know that Azure Monitor alerts require diagnostic logs to be enabled first.

> 🔬 **Lab 13 – Monitor, Troubleshoot, Optimize (45 min)**: In this lab you generate synthetic workloads that exhibit specific performance anti-patterns — data skew (one key dominates a join) and excessive shuffle (unnecessary `.repartition(1000)` calls). You use the Spark UI to identify the root cause: the SQL tab shows a SortMergeJoin with uneven task durations; the Stages tab shows massive shuffle write; the Executors tab shows spill to disk on some nodes. You then apply fixes: `/*+ BROADCAST(small_table) */` eliminates the shuffle, AQE coalesces partitions reduces the task count, and `.coalesce()` replaces `.repartition()` before the write. The lab concludes by comparing the optimized pipeline runtime against the baseline — typically 2-5x faster.

---

## Key Exam Tips

Summarizing the highest-weight topics from Learning Path 4:

### 🎯 High-Frequency Topics

1. **Medallion Architecture responsibilities**: Bronze = raw/immutable, Silver = cleaned/validated, Gold = aggregated/business-ready. Know which operations happen at each layer.

2. **Lakeflow Spark Declarative Pipelines vs notebook-based**: Declarative = `@dlt.table`, `LIVE.`, automatic incremental processing, built-in data quality constraints. Notebook = full procedural control, custom error handling. The exam may describe a scenario and ask which approach is appropriate.

3. **Lakeflow Job DAG definition**: How to define tasks with `depends_on`, `condition_task` branching (`outcome = "true"` / `"false"`), and the Quartz cron format (`Seconds Minutes Hours Day-of-Month Month Day-of-Week` — note the leading seconds field).

4. **If/else condition tasks**: Know the left/op/right pattern, valid operators, and that downstream tasks reference `outcome`: "true" or "false".

5. **Trigger types**: Schedule (cron), file arrival, table update, continuous, manual. Know `min_time_between_triggers_seconds` to prevent loops.

6. **DAB YAML structure**: `bundle.name`, `targets` (dev/staging/prod with `mode: development` vs `mode: production`), `resources.jobs`, variable syntax `${var.name}` and `${workspace.current_user.username}`.

7. **CLI command order**: `bundle init` → `bundle validate` → `bundle deploy --target <env>` → `bundle run <job_name>`.

8. **Spark UI diagnosis**: Know which tab to use for which problem — Stages for skew, SQL for plan analysis, Executors for spill, Storage for cache analysis.

9. **Error handling in notebooks**: Use `dbutils.notebook.exit()` to set task outputs (structured JSON), `raise` to mark tasks as failed, and `dbutils.notebook.run()` to invoke sub-notebooks. Condition tasks can route failures to error-handling notebooks.

10. **DAG design patterns**: Fan-out/fan-in (parallel processing), linear chain (sequential), condition branch (if/else), and hybrid (production). Know when each pattern is appropriate.

11. **Performance anti-patterns and fixes**:
   - **Data skew** → AQE skew join, broadcast join, salting
   - **Excessive shuffle** → broadcast join, bucketing, coalesce
   - **Spill to disk** → increase executor memory, adjust partitions
   - **Small files** → OPTIMIZE, ZORDER, auto-compact

12. **Broadcast join syntax**: Both SQL hint `/*+ BROADCAST(t) */` and PySpark `broadcast(df)`. Default threshold 10 MB, configurable via `spark.sql.autoBroadcastJoinThreshold`.

13. **AQE settings**: Enabled by default. Know the three optimizations (coalesce partitions, skew join, join strategy switching) and how to configure them.

14. **Job run repair**: `repair_run` re-runs only failed/canceled tasks, not the entire job. Use `--rerun-from-task-key` to start repair from a specific task. This is distinct from rerun (which discards all results).

15. **REST API bundle deployment**: Know that bundles can be deployed via the Databricks REST API (`POST /api/2.0/bundle/deploy`) as an alternative to the CLI. Use the API when custom CI/CD integration is needed or the CLI is not available.

16. **Azure Monitor alerts**: Three types — metric alerts (threshold-based), log alerts (KQL queries), and activity log alerts (workspace operations). Requires diagnostic logs to be enabled. Complements job-level notifications.

17. **COMMENT ON for metadata**: Use SQL `COMMENT ON TABLE` and `COMMENT ON COLUMN` to document medallion layer metadata. Comments persist in Delta metadata and are visible in Catalog Explorer.

### 📋 Exam Language to Know

| Term | Meaning |
|---|---|
| `depends_on` | Task dependency array in a DAG |
| `notebook_task` | A task that runs a Databricks notebook |
| `pipeline_task` | A task that runs a Lakeflow Spark Declarative Pipeline |
| `condition_task` | If/else branching task |
| `outcome` | The branch selector ("true" or "false") for condition tasks |
| `file_arrival` | Event trigger for new files in cloud storage |
| `table_update` | Event trigger for Delta table changes |
| `notification_destination` | Workspace-level webhook config for Slack/PagerDuty |
| `development` | Bundle mode that prefixes resource names with the user's identity |
| `production` | Bundle mode that uses exact resource names |
| `${var.source_path}` | DAB template variable (resolved at deploy time) |
| `{{tasks.extract.row_count}}` | DAB runtime value (resolved at job run time) |
| `POST /api/2.0/bundle/deploy` | REST API endpoint for deploying bundles |
| `dbutils.notebook.exit()` | Sets the task output value (structured JSON) |
| `dbutils.notebook.run()` | Invokes a sub-notebook and captures its output |
| `COMMENT ON` | SQL statement to add metadata to tables and columns |
| `Azure Monitor alerts` | Cross-job infrastructure monitoring via metric, log, or activity alerts |
| `Spark UI` | Browser-based debugging UI for Spark jobs |
| `AQE` | Adaptive Query Execution — runtime query optimizer |
| `OPTIMIZE` | Delta Lake command to compact small files |
| `ZORDER BY` | Clustering within Delta Lake to co-locate related data |
| `repair_run` | Re-run only failed tasks of a partially successful job |
| `spill` | Data written to disk when it doesn't fit in executor memory |
| `shuffle` | Data movement across the network between stages |
| `fan-out/fan-in` | DAG pattern where one task spawns parallel children that converge |
| `linear chain` | DAG pattern where tasks execute one after another sequentially |

---

*This concludes Learning Path 4: Deploy and Maintain Data Pipelines and Workloads. Combine this material with hands-on practice in the referenced labs and you'll be well-prepared for the ~30-35% of the DP-750 exam that covers this domain.*
