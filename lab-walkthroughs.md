# DP-750 Lab Walkthroughs — Complete Guide

> **Microsoft Azure Databricks Data Engineer Associate (DP-750)**
> Comprehensive walkthrough for all 14 labs (00–13) covering setup, Unity Catalog, governance, data modeling, ingestion, pipelines, lifecycle, and performance optimization.

---

# Lab 00: Set Up Your Azure Databricks Environment

| Detail | Value |
|---|---|
| **Duration** | 15 minutes |
| **Scenario** | Provision an Azure Databricks Premium workspace in your Azure subscription |
| **Learning Path** | LP1 — Set Up and Configure Environment |

## Scenario Summary

Before you can run any of the lab exercises, you need an **Azure Databricks Premium workspace** provisioned in your Azure subscription. This lab walks you through provisioning using **Azure Cloud Shell** — no local tools required.

## Prerequisites

- An Azure subscription with permissions to create resources
- Azure login credentials provided for the course
- A web browser

## Step-by-Step Walkthrough

### Task 1: Open Azure Cloud Shell

1. Sign in to the **Azure portal** at `https://portal.azure.com`.
2. Click the **Cloud Shell** button (**>_**) in the top toolbar. If prompted, select **Bash** as the shell type.
3. If this is your first time using Cloud Shell, select **No storage account required**, choose your subscription, and click **Apply**.
4. Wait for the `username@Azure:~$` prompt to appear.

> ⚠️ **Troubleshooting:** If Cloud Shell doesn't appear, navigate directly to `https://shell.azure.com` in a new tab.

### Task 2: Run the Provisioning Script

```bash
curl -sL https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Instructions/Labs/00-setup.sh | bash
```

- The script creates a **resource group** named `rg-dp750` and an **Azure Databricks workspace** named `adb-dp750` in a random supported region.
- Deployment takes approximately **5 minutes**.

### Task 3: Open the Azure Databricks Workspace

1. In the Azure portal, search for **Azure Databricks** in the top search bar.
2. Select the **adb-dp750** workspace from the list.
3. Click **Launch workspace**. The Azure Databricks UI opens in a new tab.
4. Confirm you can see the Azure Databricks home page.

## Expected Outcomes

- A resource group `rg-dp750` containing an Azure Databricks Premium workspace `adb-dp750`
- Ability to access the Azure Databricks workspace UI

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **Provision Azure Databricks** | Understand the workspace creation process, which is a foundational step for any Azure Databricks deployment |
| **Azure Cloud Shell** | Familiarity with CLI-based provisioning — relevant for automation scenarios |
| **Workspace types** | Knowing that DP-750 requires **Premium** tier (needed for Unity Catalog, Delta Sharing, etc.) |

🔑 **Key Takeaway:** The `rg-dp750` resource group is used by **all subsequent labs**. Keep note of it for cleanup.

---

# Lab 01: Explore Azure Databricks

| Detail | Value |
|---|---|
| **Duration** | 45 minutes |
| **Scenario** | CityMoves Transit — get familiar with the Azure Databricks workspace UI |
| **Learning Path** | LP1 — Set Up and Configure Environment |

## Scenario Summary

You're a newly onboarded data engineer at **CityMoves Transit**, a regional public transportation authority managing bus, tram, and train services. Your first task is to get comfortable with the Azure Databricks workspace — navigating the UI, uploading data, and working with notebooks.

## Prerequisites

- Lab 00 completed (Azure Databricks Premium workspace provisioned)

## Step-by-Step Walkthrough

### Exercise 1: Navigate the Workspace UI

**Task 1: Explore the sidebar**
- Identify key sections: **Workspace** (notebooks/files), **Recents**, **Catalog** (Unity Catalog assets), **Jobs & Pipelines**, **Compute**, **Marketplace**
- Click **+ New** to see what objects you can create (notebooks, queries, clusters, dashboards)

**Task 2: Explore Genie Code**
- Click the Genie Code icon (top-right) to open the AI assistant panel
- Ask: *"What can I do with Azure Databricks as a data engineer?"*
- Genie Code provides contextual, workspace-aware guidance — you'll use it throughout all labs

**Task 3: Upload a sample dataset**
1. In **Catalog**, expand `adb-dp750` > `default`
2. Click the **⋮** menu next to `default`, select **Create** > **Volume**, name it `lab_data`
3. Go to **+ New** > **Add or upload data** > **Upload files**
4. Download `routes.csv` from the provided URL and upload it to the `lab_data` volume

### Import and Run the Notebook

1. Go to **Workspace** > create a folder (e.g., `Labs/01`)
2. Right-click > **Import** > **URL** > paste the notebook URL
3. Open the notebook, select **Serverless** compute
4. Complete the coding exercises inside the notebook:
   - **Exercise 1:** Write Python code defining route variables and printing formatted messages
   - **Exercise 2:** Use `%sql` magic to run SQL queries in the same notebook
   - **Exercise 3:** Edit a Markdown cell with headings, paragraphs, and bullet lists

## Key Code Snippets

### Python variable definitions (Exercise 1)
```python
route_name = "City Express"
origin = "Central Station"
destination = "Airport Terminal"
print(f"Route: {route_name} | From: {origin} → To: {destination}")
```

### SQL magic command (Exercise 2)
```sql
%sql
SELECT CURRENT_TIMESTAMP AS current_time, 'CityMoves Transit' AS system_name
```

### Markdown documentation (Exercise 3)
```markdown
# CityMoves Transit — Route Explorer

This notebook provides an overview of the CityMoves Transit route network...
- Route detail summaries
- SQL-based analytics
- Markdown documentation
```

## Expected Outcomes

- Successfully created a Unity Catalog volume and uploaded a CSV file
- Ran Python and SQL code in the same notebook using magic commands
- Documented notebook content with Markdown

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **Workspace navigation** | Exam expects familiarity with the Databricks UI — sidebar, search, Catalog Explorer |
| **Notebook basics** | Multi-language notebooks (Python + SQL) are fundamental to Databricks |
| **Genie Code** | AI-assisted coding is a major theme — you're expected to use it efficiently |
| **Unity Catalog volumes** | Understanding volumes as landing zones for raw files |

🔑 **Key Takeaway:** Notebooks in Azure Databricks support **Python**, **SQL** (via `%sql` magic), **Scala**, and **R** — all within the same notebook. The `%sql` magic command switches the cell language.

---

# Lab 02: Select and Configure Compute in Azure Databricks

| Detail | Value |
|---|---|
| **Duration** | 30 minutes |
| **Scenario** | HealthBridge Analytics — create and configure clusters for a healthcare analytics workload |
| **Learning Path** | LP1 — Set Up and Configure Environment |

## Scenario Summary

You're a data engineer at **HealthBridge Analytics**, a regional healthcare organization processing millions of patient records daily. You need to configure compute resources — an all-purpose cluster with proper settings and libraries — so your team can run data engineering pipelines and ad hoc analysis.

## Prerequisites

- Lab 00 completed
- Metastore admin or workspace admin permissions (or cluster creation rights)

## Step-by-Step Walkthrough

### Exercise 1: Create an All-Purpose Cluster

1. Go to **Compute** > **Create compute**
2. Set **Simple form** to **OFF** (advanced mode)
3. Configure:
   - **Cluster name:** `healthbridge-dev`
   - **Cluster mode:** Multi node
   - **Access mode:** Standard (Shared)
4. Under **Databricks Runtime**, select the latest **LTS** version
5. **Worker type:** Memory-optimized (E-series) — healthcare joins benefit from higher memory-to-core ratios
6. **Autoscaling:** Min 1, Max 3
7. **Auto termination:** 15 minutes (cost control)
8. Ensure **Photon Acceleration** is **enabled**
9. Click **Create compute** and wait for it to reach **Running** state

### Exercise 2: Install a Cluster-Scoped Library

1. Select the `healthbridge-dev` cluster > **Libraries** tab > **Install new**
2. **Library source:** PyPI, **Package:** `faker==40.8.0`
3. Click **Install** and wait for status to show **Installed**

> ⚠️ **Why pin versions?** In healthcare, reproducibility is critical. Pinning the exact library version ensures every team member uses the same code.

### Stop/Delete the Cluster

After exploring configuration, terminate or delete the cluster (the notebook runs on Serverless later).

### Exercise 3: Notebook-Scoped Libraries

1. Attach the imported notebook to **Serverless** compute
2. Run cells that demonstrate `%pip install faker` inside the notebook
3. Generate synthetic patient data with the `faker` library

## Key Code Snippets

### Notebook-scoped library installation
```python
%pip install faker==40.8.0
```

### Generating synthetic patient data
```python
from faker import Faker
fake = Faker('en_GB')

# Generate patient records
patients = [(fake.unique.uuid4(), fake.name(), fake.date_of_birth(minimum_age=0, maximum_age=90),
             fake.address().replace('\n', ', '), fake.phone_number()) for _ in range(100)]

df = spark.createDataFrame(patients, schema=['patient_id', 'name', 'date_of_birth', 'address', 'phone'])
df.display()
```

## Expected Outcomes

- Created a multi-node shared cluster with LTS runtime, autoscaling, and auto-termination
- Installed `faker` cluster-scoped and verified it appeared in the Libraries tab
- Successfully generated synthetic patient data in the notebook

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **Cluster types** | All-purpose vs. job clusters — understand when to use each |
| **Cluster configuration** | Runtime selection, worker types, autoscaling, auto-termination |
| **Photon** | Vectorized query engine that accelerates SQL and DataFrame operations |
| **Library management** | Cluster-scoped vs. notebook-scoped — exam covers both |
| **Cost management** | Auto-termination and autoscaling are exam-relevant cost controls |

🔑 **Key Takeaway:** Cluster-scoped libraries are available to **all notebooks** on the cluster and persist across restarts. Notebook-scoped libraries (`%pip install`) are temporary and scoped to the session — ideal for testing dependencies before making them permanent.

---

# Lab 03: Create and Organize Objects in Unity Catalog

| Detail | Value |
|---|---|
| **Duration** | 45 minutes |
| **Scenario** | Lakeside University — build a complete Unity Catalog namespace with medallion architecture |
| **Learning Path** | LP1 — Set Up and Configure Environment |

## Scenario Summary

You're a data engineer at **Lakeside University**, digitizing academic operations. You build the full Unity Catalog namespace: catalogs, medallion schemas (bronze/silver/gold), managed tables with constraints, views, volumes, and reusable SQL functions.

## Prerequisites

- Lab 00 completed
- Active Unity Catalog metastore attached to workspace
- `CREATE CATALOG` privilege on the metastore

## Step-by-Step Walkthrough

### Exercise 1: Set Up the Catalog Structure

```sql
-- Create catalog with comment
CREATE CATALOG IF NOT EXISTS edu_dev
COMMENT 'Development catalog for Lakeside University';

-- Create medallion schemas
CREATE SCHEMA IF NOT EXISTS edu_dev.bronze
  COMMENT 'Raw ingested data — unmodified source files';
CREATE SCHEMA IF NOT EXISTS edu_dev.silver
  COMMENT 'Cleaned, validated, and enriched data';
CREATE SCHEMA IF NOT EXISTS edu_dev.gold
  COMMENT 'Aggregated, analytics-ready data';
```

### Exercise 2: Create Tables with Constraints

```sql
-- Students table with PRIMARY KEY
CREATE TABLE IF NOT EXISTS edu_dev.silver.students (
  student_id     BIGINT NOT NULL,
  first_name     STRING,
  last_name      STRING,
  email          STRING,
  enrollment_year INT,
  program        STRING,
  CONSTRAINT pk_students PRIMARY KEY (student_id)
);

-- Courses table with PRIMARY KEY
CREATE TABLE IF NOT EXISTS edu_dev.silver.courses (
  course_id   BIGINT NOT NULL,
  course_name STRING,
  department  STRING,
  credits     INT,
  CONSTRAINT pk_courses PRIMARY KEY (course_id)
);

-- Enrollments table with FOREIGN KEYS
CREATE TABLE IF NOT EXISTS edu_dev.silver.enrollments (
  enrollment_id  BIGINT NOT NULL,
  student_id     BIGINT,
  course_id      BIGINT,
  semester       STRING,
  grade          DECIMAL(4,2),
  CONSTRAINT pk_enrollments PRIMARY KEY (enrollment_id),
  CONSTRAINT fk_enrollments_student
    FOREIGN KEY (student_id) REFERENCES edu_dev.silver.students(student_id),
  CONSTRAINT fk_enrollments_course
    FOREIGN KEY (course_id) REFERENCES edu_dev.silver.courses(course_id)
);
```

### Exercise 3: Create Views

```sql
-- Standard view joining students + courses + enrollments
CREATE OR REPLACE VIEW edu_dev.silver.vw_student_enrollments AS
SELECT s.student_id, s.first_name, s.last_name, c.course_name,
       c.department, e.semester, e.grade
FROM edu_dev.silver.students s
JOIN edu_dev.silver.enrollments e ON s.student_id = e.student_id
JOIN edu_dev.silver.courses c ON e.course_id = c.course_id;
```

### Exercise 4: Create a Materialized View

```sql
CREATE OR REPLACE MATERIALIZED VIEW edu_dev.gold.vw_department_enrollment_stats AS
SELECT c.department,
       COUNT(DISTINCT e.student_id) AS distinct_students,
       COUNT(*) AS total_enrollments,
       ROUND(AVG(e.grade), 2) AS avg_grade
FROM edu_dev.silver.enrollments e
JOIN edu_dev.silver.courses c ON e.course_id = c.course_id
GROUP BY c.department;
```

### Exercise 5: Create a Volume and Load CSV

```sql
CREATE VOLUME IF NOT EXISTS edu_dev.bronze.raw_files;
```

Then use Python to write CSV data to the volume path `/Volumes/edu_dev/bronze/raw_files/`.

### Exercise 6: Create a SQL Function

```sql
CREATE OR REPLACE FUNCTION edu_dev.silver.get_grade_classification(grade DECIMAL(4,2))
RETURNS STRING
COMMENT 'Classifies a numeric grade into letter grades (A/B/C/D/F)'
RETURN CASE
  WHEN grade >= 8.5 THEN 'A'
  WHEN grade >= 7.0 THEN 'B'
  WHEN grade >= 5.5 THEN 'C'
  WHEN grade >= 4.0 THEN 'D'
  ELSE 'F'
END;
```

### Optional: Configure an AI/BI Genie Space

1. Go to **+ New** > **Genie space**
2. Add tables: `edu_dev.silver.students`, `edu_dev.silver.courses`, `edu_dev.silver.enrollments`, etc.
3. Ask: *"Which department has the highest average grade?"*

## Expected Outcomes

- Full medallion architecture (bronze → silver → gold) in Unity Catalog
- Tables with primary/foreign key constraints that enforce referential integrity
- Standard and materialized views for analytics
- A reusable SQL function for grade classification
- Genie Space for natural language querying

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **Unity Catalog hierarchy** | Catalog → Schema → Objects — understand the 3-level namespace |
| **Medallion architecture** | Bronze (raw), Silver (cleaned), Gold (aggregated) — core exam concept |
| **DDL operations** | `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE TABLE`, `ALTER TABLE` |
| **Constraints** | Primary keys, foreign keys, and how Unity Catalog enforces them |
| **Views vs. Materialized Views** | When to use each — materialized views store results physically |
| **Functions** | Reusable SQL UDFs for business logic |
| **Genie Spaces** | Natural language querying — new feature in Databricks |

🔑 **Key Takeaway:** Unity Catalog uses a **3-level namespace**: `catalog.schema.object`. The medallion architecture (bronze/silver/gold) is the recommended pattern for organizing data pipelines.

## Cleanup

```sql
DROP CATALOG IF EXISTS edu_dev CASCADE;
```

---

# Lab 04: Secure Unity Catalog Objects

| Detail | Value |
|---|---|
| **Duration** | 35–40 minutes |
| **Scenario** | NorthMart Retail — implement row-level security, column masking, and Azure Key Vault secrets |
| **Learning Path** | LP2 — Secure and Govern Unity Catalog |

## Scenario Summary

You're a data engineer at **NorthMart Retail**, a national supermarket chain. The security team has three concerns: regional analysts should only see their region's data, customer emails (PII) must be masked, and a third-party API key must never appear in code. You address all three with access control, row filters, column masks, and Azure Key Vault-backed secrets.

## Prerequisites

- Lab 00 completed
- CREATE CATALOG privilege
- Azure subscription (for Key Vault)

## Step-by-Step Walkthrough

### Before the Notebook: Create a Databricks Group

1. Go to **Settings** (your username, top-right) > **Identity and access** > **Groups** > **Add group**
2. Name: `retail-analysts` — add your user as a member

### Before Exercise 4: Create Azure Key Vault

1. In Azure Portal, create a Key Vault: `kv-northmart-<initials>`
2. Set **Permission model** to **Vault access policy**
3. Add an access policy granting your user **Get** and **List** secret permissions
4. Create a secret: name `loyalty-api-key`, value `NORTHMART-LOYALTY-2026-SECURE`
5. Note the **Vault URI** and **Resource ID**

### Create Databricks Secret Scope

Navigate to `https://<workspace-url>#secrets/createScope` (note: uppercase **S** in createScope)
- **Scope name:** `retail-kv-scope`
- **Manage Principal:** All workspace users
- **DNS Name:** Vault URI from Azure
- **Resource ID:** Resource ID from Azure

### Exercise 1: Access Control (in notebook)

```sql
-- Grant permissions to the group
GRANT USE CATALOG ON CATALOG retail_catalog TO `retail-analysts`;
GRANT USE SCHEMA ON SCHEMA retail_catalog.security_lab TO `retail-analysts`;
GRANT SELECT ON TABLE retail_catalog.security_lab.customers TO `retail-analysts`;

-- Verify grants
SHOW GRANTS ON TABLE retail_catalog.security_lab.customers;
```

### Exercise 2: Row Filtering

Create a function that returns `true` only for the user's region, then attach it as a row filter:

```sql
-- Create row filter function
CREATE OR REPLACE FUNCTION retail_catalog.security_lab.region_filter(region STRING)
RETURN IF(IS_MEMBER('retail-analysts'), true, region = CURRENT_USER());

-- Apply row filter to table
ALTER TABLE retail_catalog.security_lab.customers
SET ROW FILTER retail_catalog.security_lab.region_filter ON (region);
```

### Exercise 3: Column Masking

```sql
-- Create column mask function (masks email with '***')
CREATE OR REPLACE FUNCTION retail_catalog.security_lab.mask_email(email STRING)
RETURN IF(IS_MEMBER('retail-analysts'), email, CONCAT('***', SPLIT(email, '@')[1]));

-- Apply column mask
ALTER TABLE retail_catalog.security_lab.customers
ALTER COLUMN email SET MASK retail_catalog.security_lab.mask_email;
```

### Exercise 4: Key Vault Secrets

```python
# Retrieve secret securely in notebook (no plaintext API key in code!)
api_key = dbutils.secrets.get(scope="retail-kv-scope", key="loyalty-api-key")
print(f"API key retrieved: {api_key[:5]}...")  # Only show first 5 chars
```

## Expected Outcomes

- `retail-analysts` group can SELECT from the customers table
- Users outside the group see only their own region's rows (row filter)
- Email addresses are masked for non-privileged users (column mask)
- API key is retrieved from Azure Key Vault via Databricks secret scope — never exposed in code

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **Access control** | `GRANT` / `REVOKE` at catalog, schema, and table levels |
| **Row filters** | Fine-grained row-level security — exam expects SQL syntax and behavior |
| **Column masks** | PII protection at the column level — masks are transparent to authorized users |
| **Azure Key Vault** | Integration with Databricks secret scopes — how secrets flow securely |
| **IS_MEMBER()** | Built-in function to check group membership at runtime |

🔑 **Key Takeaway:** Row filters and column masks are **transparent** to queries — users don't need to modify their SQL. The mask/filter is applied automatically by Unity Catalog at query time.

---

# Lab 05: Govern Unity Catalog Objects

| Detail | Value |
|---|---|
| **Duration** | 30 minutes |
| **Scenario** | AutoSphere AG — apply tags, retention policies, lineage tracking, and audit log analysis |
| **Learning Path** | LP2 — Secure and Govern Unity Catalog |

## Scenario Summary

You're a data engineer at **AutoSphere AG**, a global automotive manufacturer. The data governance team needs retention policies for vehicle telemetry, full lineage visibility, and queryable audit logs. You implement tagging, `VACUUM`, predictive optimization, lineage queries, and audit log analysis.

## Prerequisites

- Lab 00 completed
- CREATE CATALOG privilege

## Step-by-Step Walkthrough

### Exercise 1: Set Up the AutoSphere Data Platform

Run the provided setup cells to create `automotive_catalog.governance_lab` with `customer_registrations`, `vehicle_telemetry`, and `service_records` tables.

### Exercise 2: Add Comments and Tags

```sql
-- Add table-level comment
COMMENT ON TABLE automotive_catalog.governance_lab.vehicle_telemetry IS
  'Continuous telemetry events from connected vehicles';

-- Add column-level comments
COMMENT ON COLUMN automotive_catalog.governance_lab.vehicle_telemetry.speed_kmh IS
  'Instantaneous vehicle speed in km/h';
COMMENT ON COLUMN automotive_catalog.governance_lab.vehicle_telemetry.battery_level_pct IS
  'State of charge as a percentage (0–100)';

-- Apply governance tags
ALTER TABLE automotive_catalog.governance_lab.customer_registrations
SET TAGS ('classification' = 'pii', 'retention' = '7yrs');
ALTER TABLE automotive_catalog.governance_lab.vehicle_telemetry
SET TAGS ('classification' = 'non-pii', 'retention' = '90days');
```

### Exercise 3: Retention and VACUUM

```sql
-- Set Delta retention period (30 days of history)
ALTER TABLE automotive_catalog.governance_lab.vehicle_telemetry
SET TBLPROPERTIES ('delta.logRetentionDuration' = '30 days');

-- Run VACUUM to physically purge old data files
VACUUM automotive_catalog.governance_lab.vehicle_telemetry
RETAIN 30 HOURS;
```

### Exercise 4: Enable Predictive Optimization

```sql
ALTER TABLE automotive_catalog.governance_lab.vehicle_telemetry
SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                   'delta.autoOptimize.autoCompact' = 'true');
```

### Exercise 5: View Lineage (UI + System Tables)

**In Catalog Explorer:** Navigate to `automotive_catalog.governance_lab.vehicle_telemetry` > **Lineage** tab > **See Lineage Graph**.

**Programmatic lineage queries:**

```sql
-- Query lineage system table
SELECT * FROM system.access.lineage
WHERE table_name = 'vehicle_telemetry'
ORDER BY timestamp DESC;
```

### Exercise 6: Query Audit Logs

```sql
-- Who accessed what data and when?
SELECT user_identity, action_name, object_name, timestamp
FROM system.access.audit
WHERE object_name LIKE '%vehicle_telemetry%'
  AND timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY timestamp DESC;
```

## Expected Outcomes

- Tables and columns documented with comments and governance tags
- Delta retention policy configured and VACUUM executed
- Predictive optimization enabled on vehicle_telemetry
- Data lineage visible in Catalog Explorer and queryable via system tables
- Audit log queries returning access patterns

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **Tags** | Unity Catalog tags for data classification — searchable in Catalog Explorer |
| **VACUUM** | Physically deletes old Delta files — understand retention period semantics |
| **Delta retention** | `delta.logRetentionDuration`, `delta.deletedFileRetentionDuration` |
| **Lineage** | Both visual (Catalog Explorer) and programmatic (`system.access.lineage`) |
| **System tables** | `system.access.audit` and `system.access.lineage` — queryable audit trails |
| **Predictive optimization** | `autoOptimize` and `autoCompact` — automatic maintenance |

🔑 **Key Takeaway:** System tables (`system.access.*`) provide **queryable audit and lineage** data — no need for third-party tools. They're available in workspaces with the Premium tier and Unity Catalog enabled.

---

# Lab 06: Design and Implement Data Modeling with Azure Databricks

| Detail | Value |
|---|---|
| **Duration** | 45 minutes |
| **Scenario** | Northbank Financial — SCD Type 2, liquid clustering, CDF, and time travel |
| **Learning Path** | LP3 — Prepare and Process Data |

## Scenario Summary

You're a data engineer at **Northbank Financial**, a retail bank in the UK. You need to track customer attribute changes over time (SCD Type 2), store millions of transactions efficiently (liquid clustering), maintain a queryable audit trail (Change Data Feed), and support point-in-time recovery (time travel).

## Prerequisites

- Lab 00 completed
- CREATE CATALOG privilege

## Step-by-Step Walkthrough

### Exercise 1: Create the Data Model

**Create `dim_customer` with SCD Type 2 columns:**

```sql
CREATE OR REPLACE TABLE banking_lab.silver.dim_customer (
  customer_sk   BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id   STRING NOT NULL,
  full_name     STRING,
  email         STRING,
  city          STRING,
  segment       STRING,
  account_type  STRING,
  valid_from    TIMESTAMP NOT NULL,
  valid_to      TIMESTAMP NOT NULL,
  is_current    BOOLEAN,
  CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
)
CLUSTER BY (customer_id)
TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

**Create `fact_transactions` with liquid clustering:**

```sql
CREATE OR REPLACE TABLE banking_lab.silver.fact_transactions (
  transaction_id   STRING NOT NULL,
  account_id       STRING,
  customer_id      STRING,
  transaction_type STRING,
  amount           DECIMAL(15,2),
  currency         STRING DEFAULT 'GBP',
  transaction_date DATE,
  description      STRING
)
CLUSTER BY (customer_id, transaction_date)
TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

### Exercise 2: Implement SCD Type 2 with MERGE

```sql
MERGE INTO banking_lab.silver.dim_customer AS target
USING updates AS source
ON target.customer_id = source.customer_id AND target.is_current = true
WHEN MATCHED AND (
  target.city != source.city OR
  target.segment != source.segment OR
  target.account_type != source.account_type
) THEN UPDATE SET
  target.valid_to = CURRENT_TIMESTAMP(),
  target.is_current = false
WHEN NOT MATCHED THEN INSERT (
  customer_id, full_name, email, city, segment,
  account_type, valid_from, valid_to, is_current
) VALUES (
  source.customer_id, source.full_name, source.email, source.city,
  source.segment, source.account_type,
  CURRENT_TIMESTAMP(), TIMESTAMP('9999-12-31'), true
);
```

### Exercise 3: Query Historical Customer Records

```sql
-- Point-in-time query: what was the customer's segment at transaction time?
SELECT t.*, c.segment, c.city
FROM banking_lab.silver.fact_transactions t
LEFT JOIN banking_lab.silver.dim_customer c
  ON t.customer_id = c.customer_id
  AND t.transaction_date BETWEEN c.valid_from AND c.valid_to;
```

### Exercise 4: Query Change Data Feed

```sql
-- See all changes to fact_transactions within a time window
SELECT * FROM table_changes('banking_lab.silver.fact_transactions', 0);
```

### Exercise 5: Delta Time Travel

```sql
-- Query a previous version
SELECT COUNT(*) FROM banking_lab.silver.fact_transactions VERSION AS OF 2;

-- Or by timestamp
SELECT * FROM banking_lab.silver.fact_transactions
TIMESTAMP AS OF '2026-03-15T10:00:00.000Z';

-- Restore a previous version
RESTORE TABLE banking_lab.silver.fact_transactions TO VERSION AS OF 2;
```

## Expected Outcomes

- SCD Type 2 dimension tracks customer changes with `valid_from`, `valid_to`, `is_current`
- Liquid clustering on `customer_id, transaction_date` for efficient range scans
- Change Data Feed enabled — `table_changes()` returns row-level changes
- Time travel queries work against any Delta version or timestamp

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **SCD Type 2** | Core data modeling pattern — exam expects MERGE-based implementation |
| **Liquid clustering** | Replaces Z-order — `CLUSTER BY` with up to 4 keys |
| **Change Data Feed (CDF)** | `delta.enableChangeDataFeed` + `table_changes()` function |
| **Time travel** | `VERSION AS OF`, `TIMESTAMP AS OF`, `RESTORE TABLE` |
| **IDENTITY columns** | `GENERATED ALWAYS AS IDENTITY` for surrogate keys |

🔑 **Key Takeaway:** SCD Type 2 with **MERGE** is the most exam-tested pattern for dimension management. The `WHEN MATCHED AND` condition checks for attribute changes; `WHEN NOT MATCHED` inserts new records.

---

# Lab 07: Ingest Data into Unity Catalog

| Detail | Value |
|---|---|
| **Duration** | 45 minutes |
| **Scenario** | Solaris Energy — batch ingestion with DataFrames, COPY INTO, CTAS, and Auto Loader |
| **Learning Path** | LP3 — Prepare and Process Data |

## Scenario Summary

You're a data engineer at **Solaris Energy**, a renewable energy company. You practice four core ingestion techniques: PySpark DataFrames, SQL COPY INTO, CREATE TABLE AS SELECT, and Auto Loader for incremental file detection.

## Prerequisites

- Lab 00 completed
- Data Engineer role with catalog/volume creation permissions

## Step-by-Step Walkthrough

### Exercise 1: Set Up Catalog Hierarchy

```sql
CREATE CATALOG IF NOT EXISTS solaris_lab
  COMMENT 'Solaris Energy analytics platform';
CREATE SCHEMA IF NOT EXISTS solaris_lab.bronze;
CREATE SCHEMA IF NOT EXISTS solaris_lab.silver;
CREATE VOLUME IF NOT EXISTS solaris_lab.bronze.raw_files;
```

### Exercise 2: Batch Ingestion with PySpark DataFrames

```python
# Read CSV from volume
df = (spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/Volumes/solaris_lab/bronze/raw_files/solar_readings.csv"))

# Write to Delta table
df.write.mode("overwrite").saveAsTable("solaris_lab.bronze.solar_readings")
```

### Exercise 3: SQL COPY INTO

```sql
-- Incremental load with deduplication
COPY INTO solaris_lab.bronze.turbine_events
FROM '/Volumes/solaris_lab/bronze/raw_files/turbine_events.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
```

**CREATE TABLE AS SELECT (CTAS):**

```sql
CREATE OR REPLACE TABLE solaris_lab.silver.site_summary AS
SELECT site_id, AVG(power_kw) AS avg_power,
       COUNT(*) AS reading_count, MAX(temperature_c) AS max_temp
FROM solaris_lab.bronze.solar_readings
GROUP BY site_id;
```

### Exercise 4: Auto Loader (Streaming Ingestion)

```python
# Configure Auto Loader for incremental file detection
df_stream = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("header", "true")
  .option("cloudFiles.schemaLocation",
          "/Volumes/solaris_lab/bronze/raw_files/_schema")
  .option("cloudFiles.inferColumnTypes", "true")
  .load("/Volumes/solaris_lab/bronze/raw_files/"))

# Write streaming data to Delta table
(df_stream.writeStream
  .format("delta")
  .option("checkpointLocation",
          "/Volumes/solaris_lab/bronze/raw_files/_checkpoints")
  .trigger(once=True)  # Process existing files once
  .toTable("solaris_lab.silver.streaming_readings"))
```

## Expected Outcomes

- CSV files ingested into Delta tables via PySpark DataFrames
- COPY INTO loaded files with deduplication (skips already-loaded files)
- CTAS created aggregated summary table
- Auto Loader detected new files and processed them incrementally

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **COPY INTO** | Idempotent file loading with built-in deduplication — exam favorite |
| **CTAS** | `CREATE TABLE AS SELECT` for ETL-style table creation |
| **Auto Loader** | `cloudFiles` format — exactly-once guarantees, schema inference, evolution |
| **DataFrames** | PySpark `spark.read` / `df.write` for batch ingestion |
| **Volumes** | Unity Catalog volumes as landing zones for raw files |

🔑 **Key Takeaway:** **COPY INTO** is the recommended approach for incremental batch loading — it tracks which files have already been processed and skips them on subsequent runs. It's more reliable than custom deduplication logic.

---

# Lab 08: Cleanse, Transform, and Load Data into Unity Catalog

| Detail | Value |
|---|---|
| **Duration** | 45 minutes |
| **Scenario** | Pristine Properties — data profiling, type casting, dedup, null handling, joins, PIVOT/UNPIVOT |
| **Learning Path** | LP3 — Prepare and Process Data |

## Scenario Summary

You're a data engineer at **Pristine Properties**, a Dutch real estate brokerage. Raw property listings data is messy: wrong data types, duplicates, nulls, and wide-format market statistics. You clean and reshape it for analytics.

## Prerequisites

- Lab 00 completed

## Step-by-Step Walkthrough

### Exercise 1: Set Up the Environment

Create `realestate_lab` with bronze/silver schemas and a `raw_files` volume. Load sample CSVs.

### Exercise 2: Profile the Listings Data

```sql
-- Quick profiling
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT listing_id) AS unique_listings,
       SUM(CASE WHEN list_price IS NULL THEN 1 ELSE 0 END) AS null_prices,
       MIN(list_price) AS min_price,
       MAX(list_price) AS max_price
FROM realestate_lab.bronze.listings;
```

### Exercise 3: Choose the Right Data Types

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

# Cast columns to proper types
df_typed = (df
  .withColumn("list_price", col("list_price").cast("double"))
  .withColumn("sqm", col("sqm").cast("double"))
  .withColumn("listing_date", col("listing_date").cast("date"))
  .withColumn("last_updated", col("last_updated").cast("timestamp")))
```

### Exercise 4: Handle Duplicates and Missing Values

```python
# Remove exact duplicates
df_dedup = df.dropDuplicates()

# Handle nulls
df_clean = (df_dedup
  .filter(col("listing_id").isNotNull())
  .fillna({"bedrooms": 0, "bathrooms": 0}))
```

### Exercise 5: Join Data

```python
# Inner join: listings with agents
df_listings_with_agents = (listings_df
  .join(agents_df, "agent_id", "left"))

# More complex join with sales
result = (df_listings_with_agents
  .join(sales_df, "listing_id", "left"))
```

### Exercise 6: PIVOT and UNPIVOT

```sql
-- PIVOT: Convert wide market stats to long format
SELECT city, month, avg_price
FROM (
  SELECT city, jan_avg, feb_avg, mar_avg
  FROM realestate_lab.bronze.market_stats_wide
)
UNPIVOT (
  avg_price FOR month IN (jan_avg AS 'January',
                          feb_avg AS 'February',
                          mar_avg AS 'March')
);
```

## Expected Outcomes

- Data profiled with null counts, distinct values, and range statistics
- Columns cast to appropriate types (DATE, TIMESTAMP, DOUBLE, INTEGER)
- Duplicate rows removed and nulls filled with sensible defaults
- Tables joined with inner and left joins
- Market statistics transformed from wide to long format via PIVOT/UNPIVOT

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **Data profiling** | Understanding data quality before transformation — check nulls, distinct values |
| **Type casting** | `col().cast()` — exam expects knowledge of type coercion behavior |
| **Deduplication** | `dropDuplicates()` vs. `dropDuplicates([subset])` |
| **Null handling** | `fillna()`, `dropna()`, `filter(isNotNull())` |
| **Joins** | Inner, left, right, full outer — and when to use each |
| **PIVOT / UNPIVOT** | Row-to-column and column-to-row transformations |

🔑 **Key Takeaway:** When casting string to date/numeric types, invalid values become **NULL** (not errors). Always check for unexpected nulls after casting.

---

# Lab 09: Implement and Manage Data Quality Constraints in Unity Catalog

| Detail | Value |
|---|---|
| **Duration** | 45 minutes |
| **Scenario** | ClearCover Insurance — Lakeflow Pipeline with expectations, type validation, schema drift handling |
| **Learning Path** | LP3 — Prepare and Process Data |

## Scenario Summary

You're a data engineer at **ClearCover Insurance**. Raw claims data arrives with quality issues: missing IDs, string-formatted amounts, negative values, invalid dates, and unexpected columns. You build a **Lakeflow Spark Declarative Pipeline** (formerly Delta Live Tables) that enforces data quality at every layer.

## Prerequisites

- Lab 00 completed
- Familiarity with Python and PySpark

## Step-by-Step Walkthrough

### Exercise 1: Set Up ClearCover Data Platform

Run the setup notebook to create `insurance_lab` with bronze/silver/gold schemas, `raw_files` volume, and a `claims_raw` table with 20 records containing intentional quality issues.

### Exercise 2: Explore Data Quality Issues

```sql
-- Inspect raw data
SELECT * FROM insurance_lab.bronze.claims_raw ORDER BY claim_id NULLS LAST;

-- Check schema
DESCRIBE TABLE insurance_lab.bronze.claims_raw;
```

### Exercise 3: Create the Lakeflow Pipeline

1. Import the pipeline Python file from the provided URL
2. In **Jobs & Pipelines** > **Create ETL pipeline**:
   - Name: `ClearCover Claims Quality Pipeline`
   - Mode: **Triggered**
   - Target: `insurance_lab.silver`
   - Compute: **Serverless**
3. Import the pipeline file as the source code

### Exercise 4: Add Expectations

```python
@dp.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_status", "status IN ('OPEN', 'PENDING', 'CLOSED')")
@dp.expect_or_fail("valid_coverage", "coverage_amount > 0")
@dp.table()
def claims_validated():
    return (
        spark.readStream.format("delta")
        .table("insurance_lab.bronze.claims_raw")
        .withColumn("claim_date", col("claim_date").cast("date"))
        .withColumn("claim_amount", col("claim_amount").cast("decimal(12,2)"))
    )
```

### Exercise 5: Handle Schema Drift with Rescued Data

```python
@dp.table()
def claims_rescued():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation",
                "/Volumes/insurance_lab/bronze/raw_files/_schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .load("/Volumes/insurance_lab/bronze/raw_files/")
    )
```

### Exercise 6: Run and Monitor

1. Click **Start** to trigger a full pipeline run
2. Observe the DAG: `claims_validated` → `claims_rescued` → `claims_summary`
3. Click each dataset node to see **Data quality** metrics — expectation counts
4. Query output tables:

```sql
-- Validated claims count
SELECT COUNT(*) FROM insurance_lab.silver.claims_validated;

-- Check for rescued data
SELECT claim_id, _rescued_data
FROM insurance_lab.silver.claims_rescued
WHERE _rescued_data IS NOT NULL;
```

## Expected Outcomes

- Pipeline with three datasets: validated, rescued, and summary
- Data quality metrics visible per expectation (dropped, warned, failed)
- Invalid records filtered by expectations
- Schema drift captured in `_rescued_data` instead of crashing the pipeline

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **Lakeflow Spark Declarative Pipelines** | Formerly DLT — the recommended pipeline framework |
| **Expectations** | `@dp.expect` (warn), `@dp.expect_or_drop` (drop), `@dp.expect_or_fail` (fail) |
| **Type validation** | `col().cast()` — cast failures produce nulls, which expectations catch |
| **Schema drift** | `schemaEvolutionMode = "rescue"` with `_rescued_data` column |
| **Pipeline monitoring** | Data quality tab in pipeline UI — expectation pass/fail/drop counts |

🔑 **Key Takeaway:** The three expectation types are **warn** (keep but log), **drop** (remove violating rows), and **fail** (stop the pipeline). Choose wisely based on business criticality.

## Cleanup

```sql
DROP CATALOG IF EXISTS insurance_lab CASCADE;
```

---

# Lab 10: Design and Implement Data Pipelines with Azure Databricks

| Detail | Value |
|---|---|
| **Duration** | 45 minutes |
| **Scenario** | GlobStay Hospitality — medallion pipeline, parameterized notebooks, error handling, Lakeflow Job with If/else |
| **Learning Path** | LP4 — Deploy and Maintain Pipelines |

## Scenario Summary

You're a data engineer at **GlobStay**, a hospitality group managing five hotel properties. You build a medallion pipeline (Bronze → Silver → Gold) for booking data, implement error handling with `try/except`, parameterize notebooks for reuse, and orchestrate everything as a Lakeflow Job with task dependencies, retries, and an If/else condition.

## Prerequisites

- Lab 00 completed
- Permission to create catalogs and jobs

## Step-by-Step Walkthrough

### Part 1: Notebook Exercises

**Exercise 1: Ingest Bronze data** — Read raw booking data into `hospitality_lab.bronze.bookings_bronze`

**Exercise 2: Clean Silver data** — Apply cleaning rules (dedup, null filter, date validation, non-positive rate filter) and write to `hospitality_lab.silver.bookings_silver`

```python
df_silver = (df_bronze
  .dropDuplicates(["booking_id"])
  .filter(col("room_type").isNotNull())
  .filter(col("check_out_date").cast("date") > col("check_in_date").cast("date"))
  .filter(col("rate_per_night") > 0))
```

**Exercise 3: Aggregate Gold data** — Create `hospitality_lab.gold.property_revenue` and `hospitality_lab.gold.channel_performance`

**Exercise 4: Add error handling** — Wrap transform logic in `try/except` blocks and use `dbutils.notebook.exit()` to signal status

```python
try:
    # Transformation logic
    dbutils.notebook.exit("SUCCESS")
except Exception as e:
    print(f"ERROR: {str(e)}")
    dbutils.notebook.exit(f"FAILURE: {str(e)}")
```

**Exercise 5: Parameterize notebooks** — Use `dbutils.widgets` to accept a `layer` parameter

```python
dbutils.widgets.text("layer", "bronze")
layer = dbutils.widgets.get("layer")
print(f"Running {layer} layer...")
```

### Part 2: Lakeflow Job Orchestration (UI)

1. Go to **Jobs & Pipelines** > **Create job**
2. Name: `GlobStay Booking Pipeline`
3. Add three tasks:
   - **Task 1:** `ingest_bronze` — Notebook, Serverless
   - **Task 2:** `clean_silver` — depends on Task 1, parameter `layer=silver`, retries: 2 × 60s
   - **Task 3:** `aggregate_gold` — depends on Task 2, parameter `layer=gold`
4. Add failure notification (email)
5. **If/else condition task:**
   - Add `check_quality` task depending on `clean_silver`
   - Add `quality_gate` (If/else) depending on `check_quality`
   - Condition: `{{tasks.check_quality.values.invalid_count}} > 5`
   - **If true:** `alert_data_issues` task
   - **If false:** `proceed_gold` task
6. Click **Run now** to test

## Expected Outcomes

- Medallion architecture pipeline creates bronze → silver → gold tables
- Notebooks parameterized with `dbutils.widgets` for reuse across layers
- Error handling with `try/except` and `dbutils.notebook.exit()` signals
- Lakeflow Job with sequential dependencies, retries, and failure notifications
- If/else branching based on data quality threshold

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **Medallion architecture** | Bronze (raw), Silver (cleaned), Gold (aggregated) — the standard pipeline pattern |
| **Parameterized notebooks** | `dbutils.widgets` for reusable, parameter-driven tasks |
| **dbutils.notebook.exit()** | How tasks signal results to downstream tasks |
| **Lakeflow Jobs** | Multi-task job orchestration with dependencies |
| **If/else conditions** | Conditional task branching based on previous task values |
| **Retry policies** | Transient failure handling — interval and count |

🔑 **Key Takeaway:** **`dbutils.jobs.taskValues.set()`** in Python and **`{{tasks.<task_key>.values.<key>}}`** in the UI let you pass values between tasks — essential for data quality gating.

---

# Lab 11: Implement Lakeflow Jobs with Azure Databricks

| Detail | Value |
|---|---|
| **Duration** | 45 minutes |
| **Scenario** | TelConnect — automated CDR pipeline with file arrival trigger, cron schedule, notifications, retries |
| **Learning Path** | LP4 — Deploy and Maintain Pipelines |

## Scenario Summary

You're a data engineer at **TelConnect**, a telecom provider. You automate a Call Detail Record (CDR) pipeline using Lakeflow Jobs with file arrival triggers, scheduled runs, failure notifications, and automatic retries.

## Prerequisites

- Lab 00 completed
- Permission to create jobs

## Step-by-Step Walkthrough

### Part 1: Run the Pipeline Notebook

1. Import and run the notebook (all cells provided — exercises 1–5)
2. Creates `telconnect_lab` catalog with bronze/silver/gold schemas
3. Downloads `telecom_cdrs.csv` to the `raw_uploads` volume
4. Processes data through bronze → silver → gold medallion layers

### Part 2: Lakeflow Job Orchestration

**Exercise 1: Create the Job**
1. **Jobs & Pipelines** > **Create job** > name `TelConnect CDR Pipeline`
2. Add `process_cdrs` task (Notebook, Serverless)
3. Add **job parameter**: `processing_date` = `{{job.trigger.time.iso_date}}`
4. Test: Click **Run now** and verify it succeeds

**Exercise 2: Add Downstream SQL Task**
1. In **SQL Editor**, create a query:

```sql
SELECT region, network_type, total_calls, drop_rate_pct
FROM telconnect_lab.gold.network_summary
ORDER BY drop_rate_pct DESC;
```

2. Save as `summarize_gold_query`
3. Add `summarize_gold` task to the job (Type: **SQL query**, depends on `process_cdrs`)

**Exercise 3: File Arrival Trigger**
1. **Add trigger** > **File arrival**
2. Path: `/Volumes/telconnect_lab/bronze/raw_uploads/`
3. **Wait after last change:** 60 seconds
4. Pause the trigger (you'll add a schedule instead)

**Exercise 4: Nightly Schedule**
1. **Add trigger** > **Scheduled** > **Advanced**
2. Cron: `0 0 2 * * ?` (02:00 daily)
3. Time zone: `Europe/Amsterdam`

**Exercise 5: Failure Notifications**
1. Add **job-level** email notification on failure
2. Add **task-level** notification with **Mute until last retry** enabled

**Exercise 6: Automatic Retries**
1. On `process_cdrs` task: retry count = 2, interval = 60 seconds
2. **Metric thresholds:** Warning at 20 min, Timeout at 30 min

**Exercise 7: Run and Monitor**
1. Click **Run now**
2. Watch both tasks execute in sequence
3. Verify `processing_date` parameter was passed correctly

## Expected Outcomes

- Job processes CDR data through medallion architecture
- Downstream SQL query task runs after processing completes
- File arrival trigger configured (paused for lab)
- Nightly cron schedule at 02:00 CET
- Failure notification emails configured with muted retry alerts
- Task-level retries (2 attempts, 60s interval) with timeout threshold

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **Lakeflow Jobs** | Multi-task orchestration — the core of pipeline automation |
| **Job parameters** | Dynamic values using `{{job.trigger.time.iso_date}}` |
| **File arrival trigger** | Event-based triggering when files land in cloud storage |
| **Cron scheduling** | Schedule syntax and time zone configuration |
| **Notifications** | Job-level and task-level, mute during retries |
| **Retry policies** | Count, interval, timeout, and metric thresholds |

🔑 **Key Takeaway:** The **file arrival trigger** automatically fires the job when new files land in the monitored path. Combined with a **scheduled trigger**, you get both real-time and catch-all coverage — but in the lab you pause one to avoid duplicate runs.

---

# Lab 12: Implement Development Lifecycle Processes in Azure Databricks

| Detail | Value |
|---|---|
| **Duration** | 45 minutes |
| **Scenario** | Warehouse order processing pipeline — unit testing with pytest, Declarative Automation Bundle, Databricks CLI |
| **Learning Path** | LP4 — Deploy and Maintain Pipelines |

## Scenario Summary

You implement proper SDLC for an order processing pipeline: write unit and integration tests with pytest, package the pipeline as a Declarative Automation Bundle (DAB), and deploy it using the Databricks CLI.

## Prerequisites

- Lab 00 completed
- Permission to create jobs
- Databricks CLI installed (PowerShell on local machine)

## Step-by-Step Walkthrough

### Part 1: Implement Testing Strategy (Notebook)

**Exercise 1: Install pytest and review transforms.py**

```python
%pip install pytest
```

The notebook provides a `transforms.py` module with functions:
- `clean_row()` — strips whitespace, validates required fields
- `normalize_status()` — maps status variants to standard values
- `compute_total()` — calculates tax-inclusive totals

**Exercise 2: Write unit tests with pytest fixtures**

```python
import pytest
from transforms import clean_row, normalize_status, compute_total

def test_clean_row_removes_whitespace():
    assert clean_row("  ABC  ") == "ABC"

def test_normalize_status_pending():
    assert normalize_status("PENDING") == "PENDING"
    assert normalize_status("pending") == "PENDING"
    assert normalize_status("Pend") == "UNKNOWN"

def test_compute_total():
    assert compute_total(100.0, 0.21) == 121.0
    assert compute_total(0, 0.21) == 0.0
```

**Exercise 3: Integration test with Spark session**

```python
def test_pipeline_integration(spark):
    test_data = [("123", "2026-01-15", "confirmed", 150.00, 0.10)]
    df = spark.createDataFrame(test_data, ["order_id", "order_date", "status", "amount", "tax_rate"])
    result = run_pipeline(df)
    assert result.count() == 1
    assert result.select("total").collect()[0][0] == 165.0
```

### Part 2: Configure a Declarative Automation Bundle

1. **Install Databricks CLI:**

```powershell
winget install Databricks.DatabricksCLI
```

2. **Authenticate:**

```powershell
databricks auth login --host https://<your-workspace-url>
```

3. **Create bundle structure:**

```powershell
mkdir ~/order-pipeline-bundle; cd ~/order-pipeline-bundle
mkdir notebooks, resources
```

4. **Create `databricks.yml`:**

```yaml
bundle:
  name: order-pipeline-bundle

variables:
  environment:
    description: The deployment environment name
    default: development
  cluster_policy_id:
    description: Optional cluster policy
    default: ""

resources:
  jobs:
    order-pipeline-job:
      name: ${var.environment}-order-pipeline
      tasks:
        - task_key: validate-data
          notebook_task:
            notebook_path: ./notebooks/validate.py
        - task_key: transform-data
          depends_on:
            - task_key: validate-data
          notebook_task:
            notebook_path: ./notebooks/transform.py

targets:
  dev:
    default: true
    mode: development
    variables:
      environment: development
  prod:
    mode: production
    workspace:
      host: https://your-prod-workspace.databricks.net
    variables:
      environment: production
```

5. **Create placeholder notebooks:**

```powershell
[System.IO.File]::WriteAllLines("$PWD\notebooks\validate.py",
  @("# Databricks notebook source", "# validate"))
[System.IO.File]::WriteAllLines("$PWD\notebooks\transform.py",
  @("# Databricks notebook source", "# transform"))
```

### Part 3: Deploy and Verify

```powershell
# Validate the bundle
databricks bundle validate

# Preview what will be deployed
databricks bundle plan -t dev

# Deploy to dev
databricks bundle deploy -t dev

# Verify deployment
databricks bundle summary
```

5. Open the job URL in the workspace — confirm both tasks exist with proper dependencies

6. **Clean up:**

```powershell
databricks bundle destroy -t dev
```

## Expected Outcomes

- Unit tests pass for individual transformation functions
- Integration test validates the full pipeline with Spark
- Declarative Automation Bundle with variables, job resources, and multi-environment targets
- Successful validation, plan, and deployment via Databricks CLI
- Job appears in workspace with both tasks and correct dependency chain

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **pytest integration** | Testing within Databricks notebooks — exam covers testing strategy |
| **Declarative Automation Bundles** | Infrastructure-as-code for Databricks resources — core exam topic |
| **Databricks CLI** | `bundle validate`, `bundle plan`, `bundle deploy`, `bundle destroy` |
| **Multi-environment targets** | Dev vs. prod configurations in the same bundle |
| **Development mode** | Auto-prefixes job names with `[dev <username>]`, disables schedules |

🔑 **Key Takeaway:** Notebook files in a DAB must start with the magic header `# Databricks notebook source` and be saved as **UTF-8 without BOM**. PowerShell's `Set-Content` adds a BOM — use `[System.IO.File]::WriteAllLines()` instead.

---

# Lab 13: Monitor, Troubleshoot, and Optimize Workloads in Azure Databricks

| Detail | Value |
|---|---|
| **Duration** | 45 minutes |
| **Scenario** | Performance lab — data skew, Spark UI analysis, broadcast joins, AQE, shuffle optimization |
| **Learning Path** | LP4 — Deploy and Maintain Pipelines |

## Scenario Summary

You reproduce deliberate performance problems — data skew and excessive shuffle — then diagnose them using the **Spark UI** and apply targeted fixes: broadcast joins, Adaptive Query Execution (AQE), and shuffle partition tuning.

## Prerequisites

- Lab 00 completed
- Classic compute cluster with **Photon disabled** (Serverless won't work)

## Step-by-Step Walkthrough

### Create the Compute Cluster

Create a **classic** (non-Serverless) cluster named `perf-lab`:
- **Single node** enabled, **Access mode:** Single user
- **Runtime:** Latest LTS (non-ML, non-Photon)
- **Spark config:** `spark.databricks.photon.enabled false`
- **Auto-terminate:** 30 minutes

> ⚠️ **Critical:** Photon automatically optimizes away skew and shuffle problems. You won't see anything abnormal in the Spark UI if Photon is enabled. This cluster must be a non-Photon runtime.

### Exercise 1: Set Up Environment

Creates `perf_lab.data.transactions` (500K rows, 70% belonging to `customer_001`) and `perf_lab.data.customers` (200 rows).

### Exercise 2: Expose Data Skew

Run GroupBy aggregation and sort-merge join on skewed data:

```python
# Skewed aggregation
df_skewed = (spark.table("perf_lab.data.transactions")
  .groupBy("customer_id")
  .agg(F.sum("amount").alias("total_spent")))

df_skewed.collect()  # Force action — observe slow tasks
```

### Part 2: Investigate in Spark UI

1. Go to **Compute** > `perf-lab` > **Spark UI** tab
2. Find the GroupBy aggregation job
3. Look at **Stage** with the longest duration — examine **Summary Metrics**
4. Check **Duration** row: if **Max >> 75th percentile** → data skew
5. Sort tasks by duration descending — see 1-2 tasks taking much longer

### Exercise 3: Fix Data Skew

```python
# Apply salt key to distribute skewed values
from pyspark.sql import functions as F

# Option 1: Enable AQE (usually default in DBR 12+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Option 2: Broadcast hint for small table joins
df_fixed = (df_transactions
  .join(F.broadcast(df_customers), "customer_id"))
```

### Exercise 4: Expose Excessive Shuffle

Run a pipeline with multiple GroupBy + Join + Sort operations — observe shuffle write/read metrics.

### Part 4: Spark UI Shuffle Analysis

1. Go to **Stages** tab — look at **Shuffle Read/Write** columns
2. Click a stage with high shuffle — expand **DAG Visualization**
3. Count **Exchange** nodes (each = a full shuffle)
4. Compare **Input Size/Records** vs. **Shuffle Read Size/Records**

### Exercise 5: Reduce Shuffle Overhead

```python
# Tune shuffle partitions for workload size
spark.conf.set("spark.sql.shuffle.partitions", "8")  # Small dataset

# Enable AQE's coalescing
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Reduce unnecessary column shuffling
df_optimized = df.select("customer_id", "amount", "txn_type")  # Select only needed columns
```

## Expected Outcomes

- Data skew visible in Spark UI: one task takes 10× longer than others
- AQE fixes skew automatically when enabled
- Broadcast join eliminates shuffle for small table joins
- Shuffle metrics clearly show excessive data movement before optimization
- Shuffle partition tuning reduces overhead

## 💡 Exam Relevance

| Topic | How This Lab Prepares You |
|---|---|
| **Spark UI** | Jobs, Stages, Tasks, Summary Metrics — essential troubleshooting tool |
| **Data skew** | Recognizing the symptom (few tasks much slower than rest) |
| **AQE** | Adaptive Query Execution — `skewJoin`, `coalescePartitions` |
| **Broadcast joins** | `F.broadcast()` hint — for small dimension tables |
| **Shuffle partitions** | `spark.sql.shuffle.partitions` — tuning for data size |
| **Exchange nodes** | Each exchange = one shuffle — minimize for performance |

🔑 **Key Takeaway:** The Spark UI is your primary diagnostic tool. Key metrics to check: **Duration** distribution (for skew), **Shuffle Read/Write** (for shuffle problems), and **Exchange nodes** in the DAG (for shuffle count). Fixes: **AQE** for automatic optimization, **broadcast joins** for small tables, and **partition tuning** for shuffle reduction.

---

# Quick Reference: All Lab Catalogs & Cleanup Commands

| Lab | Catalog | Cleanup SQL |
|---|---|---|
| 01 | `adb-dp750.default.lab_data` (volume) | Drop volume in UI |
| 02 | (cluster-based, no catalog) | Terminate/delete `healthbridge-dev` cluster |
| 03 | `edu_dev` | `DROP CATALOG IF EXISTS edu_dev CASCADE;` |
| 04 | `retail_catalog` | `DROP CATALOG IF EXISTS retail_catalog CASCADE;` |
| 05 | `automotive_catalog` | `DROP CATALOG IF EXISTS automotive_catalog CASCADE;` |
| 06 | `banking_lab` | `DROP CATALOG IF EXISTS banking_lab CASCADE;` |
| 07 | `solaris_lab` | `DROP CATALOG IF EXISTS solaris_lab CASCADE;` |
| 08 | `realestate_lab` | `DROP CATALOG IF EXISTS realestate_lab CASCADE;` |
| 09 | `insurance_lab` | `DROP CATALOG IF EXISTS insurance_lab CASCADE;` |
| 10 | `hospitality_lab` | `DROP CATALOG IF EXISTS hospitality_lab CASCADE;` |
| 11 | `telconnect_lab` | `DROP CATALOG IF EXISTS telconnect_lab CASCADE;` |
| 12 | (DAB-based, no catalog) | `databricks bundle destroy -t dev` |
| 13 | `perf_lab` | `DROP CATALOG IF EXISTS perf_lab CASCADE;` |

---

## 📚 Exam Topics Coverage Map

| Exam Domain | Weight | Labs |
|---|---|---|
| **Set up Azure Databricks** | 10–15% | 00, 01, 02 |
| **Organize Unity Catalog** | 15–20% | 03 |
| **Secure & Govern** | 15–20% | 04, 05 |
| **Model & Ingest Data** | 20–25% | 06, 07 |
| **Transform & Cleanse** | 15–20% | 08, 09 |
| **Build & Deploy Pipelines** | 15–20% | 10, 11 |
| **CI/CD & Lifecycle** | 5–10% | 12 |
| **Monitor & Optimize** | 5–10% | 13 |

---

## ⚠️ Common Pitfalls to Avoid

1. **Photon must be OFF** in Lab 13 — you won't see skew/shuffle problems with it enabled
2. **Secret scope URL** in Lab 04 — the `S` in `createScope` must be **uppercase**
3. **Notebook header** in Lab 12 — `# Databricks notebook source` must be first line, file must be UTF-8 without BOM
4. **Serverless vs. Classic** — Lab 13 requires classic cluster; all other labs use Serverless
5. **DROP CATALOG CASCADE** is destructive — removes all schemas, tables, views, volumes, and functions
6. **Genie Code** is expected in all labs — use it to generate code, explain errors, and explore APIs
7. **Lab 00** must be completed first — all labs depend on the provisioning from setup
