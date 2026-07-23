# Learning Path 2: Secure and Govern Unity Catalog Objects

**DP-750 — Azure Databricks Data Engineer Associate**

Welcome to the deep-dive study resource for Learning Path 2, which covers approximately **15–20% of the DP-750 exam**. This guide is designed as a complete teaching resource — not an outline. Every concept is explained with the "why" behind it, the "how" to implement it, and the exam context you need to pass. Work through each module sequentially, try every code example, and review the exam tips at the end.

---

## Module 2.1: Secure Unity Catalog Objects

### Learning Objectives

By the end of this module you will be able to:

- Explain the Unity Catalog **privilege model** and **inheritance hierarchy**
- Grant and revoke privileges on all **securable object types**
- Create and apply **row filter functions** to restrict data access at the row level
- Create and apply **column masks** to protect sensitive columns
- Configure **Azure Key Vault-backed secret scopes** for secure credential storage
- Use **service principals** and **managed identities** for automated workload authentication
- Troubleshoot insufficient-privilege errors by understanding the permission model

---

### 2.1.1 The Unity Catalog Privilege Model

#### What Is a Privilege?

A **privilege** is a permission that grants a principal (user, service principal, or group) the right to perform a specific action on a **securable object**. Unity Catalog follows an **RBAC (Role-Based Access Control)** model, where privileges are granted to principals, not inherited by ownership.

#### Securable Object Hierarchy

Unity Catalog organizes data assets in a strict three-level namespace:

```
Metastore
  └── Catalog
       └── Schema (also called "database")
            ├── Table
            ├── View
            ├── Volume
            ├── Function
            └── Model
```

**External to this hierarchy** (but still securable):
- **External Locations** — paths in cloud storage registered with Unity Catalog
- **Storage Credentials** — cloud IAM entities used to authenticate to external storage
- **Connections** — remote data source connections for Lakehouse Federation

#### Privilege Types — The Full List

| Privilege | Scope | What It Allows |
|---|---|---|
| `USAGE` | Catalog, Schema | Enumerating objects inside and reading metadata. **Required** before any other privilege takes effect. |
| `SELECT` | Table, View, Materialized View | Reading data (running `SELECT` queries). |
| `MODIFY` | Table, View, Materialized View | Writing data (`INSERT`, `UPDATE`, `DELETE`, `MERGE`). |
| `CREATE` | Catalog, Schema | Creating new securable objects inside (tables, views, functions, volumes). |
| `READ_MATERIALIZED_VIEW` | Materialized View | Reading from a materialized view (separate from `SELECT` for MV-specific access control). |
| `REFRESH` | Materialized View | Triggering a manual refresh of a materialized view. |
| `CREATE_MANAGED_STORAGE` | Catalog | Using catalog-managed storage locations when creating managed tables. |
| `WRITE` | Volume | Writing files into a volume. |
| `READ` | Volume | Reading files from a volume. |
| `EXECUTE` | Function | Running a UDF or stored procedure. |
| `ALL PRIVILEGES` | Any object | Grants every available privilege on that object at once (like `ALL` in standard SQL). |

💡 **Exam Tip:** `USAGE` is the most foundational privilege. A user who has `SELECT` on a table but no `USAGE` on the containing schema or catalog *cannot query the table*. `USAGE` is a prerequisite, not a nice-to-have. This is a common exam trap.

#### The Inheritance Model

Privileges **flow downward** through the hierarchy:

```
CATALOG: USAGE, CREATE   (granted to data_engineers)
  └── SCHEMA: USAGE        (inherited from catalog — yes, even USAGE is inherited)
       └── TABLE: SELECT   (explicitly granted — this takes effect because USAGE flows down)
```

**Key rule:** Privileges granted on a catalog or schema are automatically inherited by all child objects (schemas, tables, views, etc.) *at the time they are created*. If you grant `SELECT` on a catalog, every existing and future table in every schema under that catalog is accessible.

**Caveat:** Privileges on **external locations** and **storage credentials** do NOT inherit through the catalog/schema hierarchy. They are independent securable objects.

⚠️ **Warning:** Inheritance is powerful but dangerous. Granting `ALL PRIVILEGES` on a catalog is often too broad. In enterprise environments, prefer granting at the schema level for granularity.

#### How Grants Actually Work

The `GRANT` command follows standard SQL syntax:

```sql
GRANT <privilege> ON <securable_object> TO <principal>;
```

And `REVOKE`:

```sql
REVOKE <privilege> ON <securable_object> FROM <principal>;
```

🔬 **Lab Reference (Lab 04 — NorthMart Retail):** In the lab, you grant schema-level permissions to a Databricks group:

```sql
-- Grant the 'data_analysts' group SELECT access on the entire silver schema
-- This gives them read access to all tables in the schema at once
GRANT SELECT ON SCHEMA main.silver TO data_analysts;

-- Grant the 'data_engineers' group more powerful privileges
-- MODIFY allows INSERT/UPDATE/DELETE, USAGE is needed to navigate the schema
GRANT SELECT, MODIFY ON TABLE main.silver.sales TO data_engineers;
```

**What each line does:**

- `GRANT SELECT ON SCHEMA main.silver TO data_analysts;` — Every table and view inside `main.silver` becomes readable by the `data_analysts` group. New tables added later are automatically included.
- `GRANT SELECT, MODIFY ON TABLE main.silver.sales TO data_engineers;` — Only the `sales` table is writable by `data_engineers`. This is a more restrictive, table-level grant.

> **Real-world pattern:** Grant at the **highest level that is safe**, then restrict at lower levels. Schema-level `SELECT` for analysts is a good default; table-level `MODIFY` for specific teams avoids accidental cross-team data corruption.

#### Revoking Privileges

```sql
-- Remove SELECT access from data_analysts on the schema
REVOKE SELECT ON SCHEMA main.silver FROM data_analysts;

-- Remove MODIFY from data_engineers on the sales table
REVOKE MODIFY ON TABLE main.silver.sales FROM data_engineers;
```

💡 **Exam Tip:** `REVOKE` only removes privileges explicitly granted — it does NOT block inheritance. If `data_engineers` have `SELECT` from a catalog-level grant, revoking `SELECT` on a specific table does NOT undo the catalog-level grant. Use `DENY` (Azure Databricks) or restructure your privilege grants to avoid this ambiguity.

---

### 2.1.2 Row Filters

#### What Is a Row Filter?

A **row filter** is a SQL user-defined function (UDF) that returns a `BOOLEAN`. When attached to a table via `SET ROW FILTER`, Unity Catalog evaluates this function for every row during query execution. Only rows where the function returns `TRUE` are visible to the querying user.

Think of it as a **dynamic, per-user WHERE clause** baked into the table definition — the user never sees the filter, never knows it exists, and cannot bypass it.

#### Creating a Row Filter Function

```sql
-- Step 1: Create the filter function
-- This function takes a region column value and returns TRUE
-- only if the region matches the current user's assigned region
CREATE OR REPLACE FUNCTION my_catalog.region_filter(region STRING)
RETURNS BOOLEAN
RETURN region = current_user_region();
```

**What each part does:**

- `CREATE OR REPLACE FUNCTION` — Defines or replaces a SQL UDF in Unity Catalog.
- `my_catalog.region_filter` — The fully qualified function name (catalog.schema.function). Must live in the same catalog as the table.
- `(region STRING)` — The input parameter. This must match the column you're filtering on.
- `RETURNS BOOLEAN` — Row filters MUST return boolean. If the return is `TRUE`, the row is visible.
- `RETURN region = current_user_region()` — The core logic. `current_user_region()` is a custom function or a lookup that maps the current user to their allowed region.

#### Applying a Row Filter to a Table

```sql
-- Step 2: Apply the filter to the table
ALTER TABLE my_catalog.silver.sales
SET ROW FILTER my_catalog.region_filter;
```

**What this does:** Every `SELECT` query against `my_catalog.silver.sales` now silently evaluates `region_filter(region)` for every row. Users see only rows where their region matches the row's region value.

#### Row Filter Under the Hood

When a user runs:

```sql
SELECT * FROM my_catalog.silver.sales;
```

Unity Catalog transparently rewrites the query to something like:

```sql
SELECT * FROM my_catalog.silver.sales
WHERE my_catalog.region_filter(region);  -- auto-injected
```

The user sees results scoped to their region. They cannot see, modify, or disable the filter.

#### Replacing or Removing a Row Filter

```sql
-- Replace with a different filter
ALTER TABLE my_catalog.silver.sales
SET ROW FILTER my_catalog.new_region_filter;

-- Remove the filter entirely
ALTER TABLE my_catalog.silver.sales
DROP ROW FILTER;
```

🔬 **Lab Reference (Lab 04 — NorthMart Retail):** The lab uses this exact pattern with NorthMart's retail data. A row filter restricts users to seeing only sales data from their assigned geographic region (e.g., a US analyst sees only US sales, an EU analyst sees only EU sales). This is a textbook **multi-tenant data isolation** pattern.

💡 **Exam Tip:** Row filters are **not** the same as views. Views are static — the filtering logic is visible in the view definition. Row filters are transparent to the user, cannot be bypassed, and apply even when the user queries the table directly, through a view, or through a dashboard. They are the preferred approach for mandatory access control (MAC) scenarios.

---

### 2.1.3 Column Masks

#### What Is a Column Mask?

A **column mask** is a SQL UDF that transforms the value of a column at query time based on the identity or group membership of the querying user. Unlike row filters which hide entire rows, column masks selectively **obfuscate cell values**.

#### Creating a Column Mask Function

```sql
-- Step 1: Create the masking function
-- This function takes an email address and returns:
-- - The original email if the user is in the 'analysts' group
-- - A masked version (e.g., "j***@***") for everyone else
CREATE OR REPLACE FUNCTION my_catalog.email_mask(email STRING)
RETURN CASE
    WHEN is_member('analysts') THEN email
    ELSE regexp_replace(email, '(.)@.*', '***@***')
END;
```

**What each part does:**

- `is_member('analysts')` — A built-in Unity Catalog function that checks whether the current user is a member of the Databricks group `analysts`. This is evaluated per-user at query time.
- `regexp_replace(email, '(.)@.*', '***@***')` — Uses regex to replace everything after the first character of the local-part and the entire domain with `***@***`. So `alice@example.com` becomes `***@***`. A simpler alternative: `LEFT(email, 1) || '***@***'` which gives `a***@***`.

#### Applying a Column Mask

```sql
-- Step 2: Apply the mask to a specific column
ALTER TABLE my_catalog.silver.users
ALTER COLUMN email SET MASK my_catalog.email_mask;
```

**What this does:** Any query that includes the `email` column from `my_catalog.silver.users` will have the `email_mask` function transparently applied. Users not in the `analysts` group see masked values.

#### Real-World Masking Patterns

```sql
-- Pattern 1: Full mask for non-privileged users
CREATE OR REPLACE FUNCTION my_catalog.ssn_mask(ssn STRING)
RETURN CASE
    WHEN is_member('hr_admins') THEN ssn
    ELSE 'XXX-XX-' || RIGHT(ssn, 4)
END;

-- Pattern 2: Partial reveal based on role
CREATE OR REPLACE FUNCTION my_catalog.phone_mask(phone STRING)
RETURN CASE
    WHEN is_member('support_agents') THEN phone
    WHEN is_member('auditors') THEN CONCAT(LEFT(phone, 4), '****')
    ELSE '***-***-****'
END;

-- Pattern 3: Hash for analytics use (irreversible but consistent)
CREATE OR REPLACE FUNCTION my_catalog.email_hash(email STRING)
RETURN CASE
    WHEN is_member('data_scientists') THEN sha256(email)
    WHEN is_member('auditors') THEN email
    ELSE 'REDACTED'
END;
```

⚠️ **Warning:** Column masks apply to **all queries**, including queries through views, joins, and subqueries. They also apply to `INSERT ... SELECT` and `CREATE TABLE AS SELECT` — so masked data can propagate if you're not careful.

#### Removing a Column Mask

```sql
ALTER TABLE my_catalog.silver.users
ALTER COLUMN email DROP MASK;
```

🔬 **Lab Reference (Lab 04 — NorthMart Retail):** The lab applies a column mask to the `email` column in the `users` table. Regular analysts see only `***@***`, while privileged users (in the `analysts` group) see the full email. This is a common PII protection pattern.

💡 **Exam Tip:** The exam will test whether you know when to use a **row filter** vs. a **column mask**. Row filters: hide entire rows based on a column value (e.g., region-based access). Column masks: obfuscate specific column values for unauthorized users (e.g., PII protection). They can be combined on the same table.

---

### 2.1.4 Securable Objects — Complete Reference

You need to know every securable object type and what privileges apply.

| Object Type | Available Privileges | Notes |
|---|---|---|
| **Metastore** | `CREATE CATALOG`, `CREATE CONNECTION`, `CREATE EXTERNAL LOCATION`, `CREATE SHARE`, `CREATE STORAGE CREDENTIAL`, `SET SHARE PERMISSION`, `USE PROVIDER`, `USE RECIPIENT`, `USE SHARE`, `USE SYSTEM CATALOG` | Typically managed by admins only. Grants here affect everything in the metastore. |
| **Catalog** | `USAGE`, `CREATE`, `CREATE_MANAGED_STORAGE`, `ALL PRIVILEGES` | `USAGE` is the minimum to see the catalog exists. `CREATE` lets users create schemas/tables inside. |
| **Schema** | `USAGE`, `CREATE`, `ALL PRIVILEGES` | `USAGE` needed to see objects in the schema. `CREATE` lets users create tables, views, volumes, etc. |
| **Table** | `SELECT`, `MODIFY`, `ALL PRIVILEGES` | `MODIFY` includes INSERT/UPDATE/DELETE/MERGE. |
| **View** | `SELECT`, `ALL PRIVILEGES` | Views are read-only in Unity Catalog. |
| **Materialized View** | `SELECT`, `READ_MATERIALIZED_VIEW`, `REFRESH`, `ALL PRIVILEGES` | `READ_MATERIALIZED_VIEW` is a specific privilege that controls read access to the cached result. `REFRESH` lets a user trigger recomputation. |
| **Volume** | `READ`, `WRITE`, `ALL PRIVILEGES` | Volumes are non-tabular file storage. `READ` to list/read files, `WRITE` to upload/modify. |
| **Function** | `EXECUTE`, `ALL PRIVILEGES` | Controls who can call a UDF or stored procedure. |
| **Model** | `EXECUTE`, `ALL PRIVILEGES` | For AI/ML model inference endpoints registered in Unity Catalog. |
| **External Location** | `CREATE MANAGED TABLE`, `CREATE EXTERNAL TABLE`, `CREATE FOREIGN TABLE`, `ALL PRIVILEGES` | Controls which storage paths can be used for external tables. Does NOT inherit from catalogs/schemas. |
| **Storage Credential** | `USAGE`, `ALL PRIVILEGES` | A storage credential authenticates to cloud storage. `USAGE` allows referencing it in external location definitions. |
| **Connection** | `USE`, `ALL PRIVILEGES` | For Lakehouse Federation — allows querying external systems. |

💡 **Exam Tip:** For the exam, memorize the **unique** privilege-object pairings:
- `READ_MATERIALIZED_VIEW` and `REFRESH` are unique to Materialized Views
- `READ` and `WRITE` are unique to Volumes
- `EXECUTE` is unique to Functions and Models
- `CREATE_MANAGED_STORAGE` is unique to Catalogs
- External Location and Storage Credential privileges are **independent** of the catalog/schema hierarchy

---

#### Documenting Data Assets with COMMENT ON

COMMENT ON adds human-readable descriptions to Unity Catalog objects. These descriptions appear in Catalog Explorer, the DESCRIBE output, and are used by AI/BI Genie for query generation.

```sql
-- Document a table
COMMENT ON TABLE main.silver.customers IS
  'Customer master data. One row per customer. Source: CRM system, updated nightly at 2 AM.';

-- Document a column  
COMMENT ON COLUMN main.silver.customers.email IS
  'Verified primary email address. May be NULL for customers who registered via social login.';

-- Document a view
COMMENT ON VIEW main.gold.monthly_revenue IS
  'Monthly revenue by product category and region. Revenue = gross_sales - returns - discounts.';
```

**Why COMMENT ON matters for the exam:**
- **Data discovery**: Users can find the right tables using Catalog Explorer's search
- **Genie accuracy**: Genie uses descriptions to match user questions to the correct tables/columns
- **Compliance**: Documenting PII columns (e.g., "Contains PII - restrict access") helps with audits
- **Self-service**: New team members can understand the data without asking the data team

**Viewing comments:**
```sql
-- View table comment
DESCRIBE EXTENDED main.silver.customers;

-- View column comments
DESCRIBE main.silver.customers;
```

**Removing a comment:**
```sql
-- Set to empty string to remove
COMMENT ON TABLE main.silver.customers IS '';
```

💡 **Exam Tip:** COMMENT ON is metadata-only — it does NOT affect access control, query behavior, or storage. It's purely for human understanding and Genie AI. The exam may test when to use COMMENT ON vs. TAG (tags are structured key-value for automated processing; COMMENT ON is free-text for humans + Genie).

---

### 2.1.5 Azure Key Vault Integration (Secret Scopes)

#### What Are Secret Scopes?

A **secret scope** is a collection of secrets stored securely in **Azure Key Vault** and made accessible to Databricks notebooks and jobs without exposing the actual secret values. Instead of hardcoding passwords, API keys, or connection strings, you reference them by name through the secret scope.

#### Architecture

```
Databricks Notebook
       │
       │  dbutils.secrets.get(scope="my-scope", key="db-password")
       │
       ▼
Azure Databricks  ──►  Azure Key Vault  ──►  Returns "s3cur3P@ss!"
(Secret Scope)           (Secret Store)
```

#### Prerequisites

1. An **Azure Key Vault** instance with secrets created
2. The Databricks workspace must have **managed identity** or **service principal** access to read from the Key Vault
3. The Key Vault's access policy must grant **Get** and **List** secret permissions to the Databricks identity

#### Creating a Secret Scope (via Databricks CLI)

```bash
# Create an Azure Key Vault-backed secret scope
databricks secrets create-scope \
  --scope northmart-secrets \
  --scope-backend-type AZURE_KEYVAULT \
  --backend-azure-keyvault-resource-id "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/northmart-kv" \
  --backend-azure-keyvault-dns-name "https://northmart-kv.vault.azure.net/"
```

**What each parameter does:**

- `--scope northmart-secrets` — The name you'll use in notebooks to reference this scope
- `--scope-backend-type AZURE_KEYVAULT` — Tells Databricks to use Azure Key Vault as the backend
- `--backend-azure-keyvault-resource-id` — The Azure Resource Manager ID of the Key Vault
- `--backend-azure-keyvault-dns-name` — The DNS endpoint of the Key Vault

#### Using Secrets in Notebooks

```python
# Retrieve a secret securely — the secret value is never logged or displayed
adls_key = dbutils.secrets.get(
    scope="northmart-secrets",
    key="adls-gen2-storage-key"
)

# Use the secret to configure Spark
spark.conf.set(
    "fs.azure.account.key.northmartadls.dfs.core.windows.net",
    adls_key
)
```

**Security guarantees:**
- The secret value is never written to notebooks or job output logs
- You cannot print the secret with `print()` or display it — Databricks intercepts it
- Secret values are encrypted in transit and at rest

#### Key Vault Access Policies

For Databricks to read secrets, the Key Vault must grant **Get** and **List** permissions to the Databricks **managed identity** (system-assigned or user-assigned) or the **service principal** that Databricks uses.

```
Azure Key Vault → Access Policies → Principal: databricks-managed-identity
                                     Permissions: Get, List (Secret Management)
```

🔬 **Lab Reference (Lab 04 — NorthMart Retail):** The lab creates an Azure Key Vault-backed secret scope called `northmart-secrets` and retrieves an ADLS Gen2 storage account key to configure the Spark `fs.azure.account.key` Hadoop configuration. This enables the notebook to read/write data in the data lake without exposing credentials in the notebook source.

💡 **Exam Tip:** The exam may ask you about the **order of precedence** for authentication. Azure Databricks authentication priority: 1) Service principal, 2) Managed identity, 3) Azure CLI credentials. Key Vault secret scopes use whichever identity the workspace is configured with.

---

### 2.1.6 Service Principals for Automated Workloads

#### Why Service Principals?

**Service principals** are non-human identities used by automated jobs, CI/CD pipelines, and scheduled notebooks. Unlike user accounts (which may be disabled when someone leaves), service principals provide **stable, long-lived credentials** for automated access.

#### Creating and Configuring a Service Principal

**Step 1: Create in Azure AD / Microsoft Entra ID**

```bash
# Azure CLI
az ad sp create-for-rbac \
  --name "databricks-etl-sp" \
  --role "Contributor" \
  --scopes "/subscriptions/<sub-id>/resourceGroups/<rg>"
```

**Step 2: Add to Databricks Workspace**

In the Databricks Admin Console → Users → Add User → Enter the service principal's application ID → Add as Service Principal.

**Step 3: Grant Unity Catalog Permissions**

```sql
-- Grant the service principal access to the catalog and schema
GRANT USAGE ON CATALOG main TO "db-12345678-9012-3456-7890-123456789012";
GRANT USAGE ON SCHEMA main.silver TO "db-12345678-9012-3456-7890-123456789012";
GRANT SELECT, MODIFY ON SCHEMA main.silver TO "db-12345678-9012-3456-7890-123456789012";
```

**Step 4: Configure Job to Use Service Principal**

In the Databricks Jobs UI, under **Access Control**, select **Run as** and choose the service principal. Jobs running as a service principal use its permissions, not the creator's.

#### Managed Identities

**Managed identities** are Azure-managed identities tied to a specific Azure resource (e.g., a Databricks workspace). They cannot be managed outside Azure but require no credential rotation — Azure handles it automatically.

```sql
-- Grant permissions to the workspace's managed identity
GRANT USAGE ON CATALOG main TO `databricks-managed-identity`;
```

**When to use which:**

| Scenario | Best Choice |
|---|---|
| Long-running production ETL jobs | Service principal (stable, revocable) |
| CI/CD pipelines | Service principal (scoped permissions per pipeline) |
| Workspace-level infrastructure | Managed identity (no credential management) |
| Ad-hoc admin scripts | User account (MFA-protected, shorter-lived) |

💡 **Exam Tip:** Know the difference: **service principals** are Azure AD identities you fully control (create, rotate credentials, revoke). **Managed identities** are Azure-managed — you cannot see their credentials, cannot rotate them, and they live/die with the Azure resource. Both can be granted Unity Catalog privileges.

---

## Module 2.2: Govern Unity Catalog Objects

### Learning Objectives

By the end of this module you will be able to:

- Apply **tags** (key-value pairs) to catalog objects for classification and discovery
- Implement **ABAC (Attribute-Based Access Control)** using tags
- Configure **data retention policies** and run **VACUUM** to manage storage
- Enable and use **Predictive Optimization** for automated table maintenance
- Query **system tables** for data lineage, audit logs, and billing
- Understand **Delta Sharing** (Databricks-to-Databricks and open sharing)
- Configure **Lakehouse Monitoring** for data quality and drift detection
- Build **dynamic views** using `current_user()`, `is_member()`, and `current_groups()`

---

### 2.2.1 Tags (Key-Value Classification)

#### What Are Tags?

**Tags** are metadata key-value pairs that you attach to Unity Catalog securable objects (catalogs, schemas, tables, views, volumes, etc.). Tags serve two purposes:

1. **Discovery & Organization** — Find assets by tag in Catalog Explorer
2. **ABAC (Attribute-Based Access Control)** — Use tags as attributes in access control policies

#### Adding Tags

```sql
-- Add a tag to a table for PII classification
ALTER TABLE main.silver.users
SET TAGS ('pii' = 'true', 'data_sensitivity' = 'high', 'retention' = '7-years');

-- Add a tag to a catalog
ALTER CATALOG main
SET TAGS ('environment' = 'production', 'owner' = 'data-platform-team');
```

#### Querying Tags

```sql
-- View all tags on a table
DESCRIBE EXTENDED main.silver.users;

-- Search for tables with a specific tag (via system table)
SELECT * FROM system.information_schema.table_tags
WHERE tag_name = 'pii' AND tag_value = 'true';
```

**Updating metadata with ALTER and COMMENT ON:**
Tags and comments serve different purposes and can coexist on the same object:
- `ALTER ... SET TAGS ('key' = 'value')` — Structured metadata for automated processing, policy evaluation, and ABAC
- `COMMENT ON TABLE/COLUMN/VIEW ... IS 'description'` — Free-text descriptions for human understanding and AI/BI Genie

Both can be updated independently and appear in Catalog Explorer's object details.

#### ABAC with Tags (Attribute-Based Access Control)

ABAC uses tags (key-value attributes) to define access policies, making governance scalable for large deployments.

**How ABAC works in Unity Catalog:**
Instead of granting privileges on individual objects (hundreds of GRANT statements), you:
1. Tag objects with attributes (e.g., `pii = 'true'`, `compliance = 'gdpr'`, `data_class = 'restricted'`)
2. Define policies that grant access based on tag values
3. New objects with the right tags automatically get the right access

**Example ABAC policy pattern:**
```sql
-- Conceptual: Grant SELECT on all tables tagged as 'analytics'
-- (Unity Catalog ABAC is configured via the Admin Console, not SQL)
-- 1. Admin tags all analytics tables
ALTER TABLE main.analytics.sales SET TAGS ('purpose' = 'analytics');
ALTER TABLE main.analytics.customers SET TAGS ('purpose' = 'analytics');

-- 2. Admin creates an ABAC policy (via Admin Console):
--    "Grant SELECT ON any table WHERE TAG('purpose') = 'analytics' TO data_analysts"
```

**ABAC vs RBAC (Role-Based Access Control):**

| Aspect | RBAC (Traditional) | ABAC (Tag-Based) |
|--------|-------------------|------------------|
| Granularity | Object-level (GRANT per table) | Attribute-level (TAG-based) |
| Scalability | N grants × M users | Few policies × tag values |
| New objects | Must explicitly grant | Auto-inherits via tags |
| Dynamic changes | Regrant needed | Just update the tag |
| Exam focus | Know the GRANT SQL | Know the tag + policy concept |

**Combining ABAC with dynamic views:**
```sql
-- A dynamic view that implements tag-like ABAC
CREATE VIEW main.silver.sales_v AS
SELECT * FROM main.bronze.sales_raw
WHERE
  CASE
    WHEN is_member('data_analysts') THEN TRUE  -- analysts see all
    WHEN is_member('regional_mgr') AND region = current_user() THEN TRUE
    ELSE FALSE
  END;
```

💡 **Exam Tip:** ABAC is tested conceptually — understand the difference between granting on individual objects (RBAC) vs granting based on attributes (ABAC). The exam may ask: "Your team adds 50 new tables per week. Which approach minimizes admin overhead?" → ABAC with tags. Tags are also **not** the same as **comments** or **descriptions** — tags are structured key-value pairs for automated processing and policy evaluation, while comments are free-text for human consumption.

🔬 **Lab Reference (Lab 05 — AutoSphere AG):** The AutoSphere AG lab applies PII classification tags to tables. The data governance team tags columns containing personally identifiable information (PII) with `pii = 'true'`. This enables automated discovery of sensitive data across the entire catalog.

---

### 2.2.2 Data Retention Policies and VACUUM

#### The Delta Lake Retention Problem

Delta Lake tables store data as **Parquet files** plus a **transaction log** (`_delta_log/`). Over time:
- `UPDATE`, `DELETE`, and `MERGE` operations create new files without deleting old ones (for time travel and ACID)
- The transaction log grows with every operation
- Orphaned files accumulate from failed writes or operations

Without management, storage costs grow unboundedly and query performance degrades.

#### Retention Policy: `VACUUM`

`VACUUM` removes files that are no longer referenced in the Delta transaction log and are older than a specified retention period.

```sql
-- Remove files older than 7 days (168 hours) from the table
VACUUM my_catalog.silver.sales RETAIN 168 HOURS;
```

**What this does:**
1. Scans the Delta transaction log to identify all live files
2. Lists all files in the table directory
3. Deletes files that are **not** in the live set AND older than 168 hours

⚠️ **Critical detail:** The default retention is **7 days (168 hours)**. You cannot set retention lower than 168 hours without changing the Spark configuration `spark.databricks.delta.retentionDurationCheck.enabled` to `false` — but doing this risks breaking concurrent operations and time travel queries.

```python
# In Python
spark.sql("VACUUM my_catalog.silver.sales RETAIN 168 HOURS")
```

#### The Danger of Aggressive VACUUM

```sql
-- WARNING: This deletes files newer than 7 days — can break time travel
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM my_catalog.silver.sales RETAIN 0 HOURS;  -- Deletes ALL unreferenced files
```

**Why 7 days minimum?** Delta Lake's **concurrent write** protocol relies on being able to read the transaction log history. If you `VACUUM` too aggressively, long-running queries, streaming pipelines, or concurrent transactions may fail because needed files are gone.

#### Checking Retention Policy

```sql
-- View the current retention configuration
SHOW TBLPROPERTIES my_catalog.silver.sales;
-- Look for: delta.logRetentionDuration, delta.deletedFileRetentionDuration
```

#### Delta Log Retention

The Delta transaction log itself also has a retention period:

```sql
-- Configure how long Delta keeps the transaction log history
ALTER TABLE my_catalog.silver.sales
SET TBLPROPERTIES (
    delta.logRetentionDuration = '30 days',
    delta.deletedFileRetentionDuration = '7 days'
);
```

- `delta.logRetentionDuration` — How long to keep the transaction log for time travel. Default: 30 days.
- `delta.deletedFileRetentionDuration` — How long to keep deleted files before VACUUM can remove them. Must be >= 7 days unless the check is disabled.

🔬 **Lab Reference (Lab 05 — AutoSphere AG):** The AutoSphere AG lab configures Delta retention policies and runs VACUUM to clean up old files from a silver-layer table, reducing storage costs while maintaining the ability to time-travel to the last 7 days of data.

💡 **Exam Tip:** The exam may ask: "How long must you retain files for VACUUM by default?" The answer is **168 hours** (7 days). You might also get: "What happens if you VACUUM with RETAIN 0 HOURS?" The answer is "All unreferenced files are deleted permanently, breaking time travel to any version older than the current one."

---

### 2.2.3 Predictive Optimization

#### What Is Predictive Optimization?

**Predictive Optimization** is a Databricks-managed service that automatically performs maintenance operations on Delta tables based on **statistical analysis of table metadata**. It optimizes tables without manual intervention, reducing the administrative burden of data maintenance.

#### How It Works

Predictive Optimization monitors table-level statistics and triggers maintenance when thresholds are met:

| Optimization | What It Does | When It Triggers |
|---|---|---|
| **OPTIMIZE** (bin-compaction) | Compacts small files into larger ones | When the ratio of small files to total files exceeds a threshold |
| **VACUUM** | Removes old, unreferenced files | Based on table retention policy (default: 7 days) |
| **ANALYZE** | Updates table statistics for the query optimizer | When statistics are stale (data change rate exceeds threshold) |
| **AUTO COMPACTION** | Merges small files during writes | When a write operation produces many small files |

#### Enabling Predictive Optimization

```sql
-- Enable on a specific table
ALTER TABLE my_catalog.silver.sales
SET TBLPROPERTIES ('delta.autoOptimize' = 'true');

-- Enable predictive optimization at the catalog level (all tables in catalog)
ALTER CATALOG main
SET TBLPROPERTIES ('predictive.optimization' = 'true');

-- Enable at the schema level
ALTER SCHEMA main.silver
SET TBLPROPERTIES ('predictive.optimization' = 'true');
```

#### Checking Predictive Optimization Status

```sql
-- Check if predictive optimization is enabled for a table
SHOW TBLPROPERTIES my_catalog.silver.sales;
-- Look for: delta.autoOptimize, predictive.optimization
```

#### When to Use Predictive Optimization vs. Manual

| Scenario | Recommendation |
|---|---|
| Bronze/raw ingestion tables (high frequency writes) | Predictive Optimization |
| Silver/cleansed tables (moderate writes, many queries) | Predictive Optimization or scheduled OPTIMIZE |
| Gold/aggregated tables (infrequent writes, critical queries) | Scheduled manual OPTIMIZE + ANALYZE for predictable timing |
| Tables > 1 TB | Predictive Optimization with VACUUM monitoring |
| Tables with streaming writes | Predictive Optimization (auto compaction is critical) |

🔬 **Lab Reference (Lab 05 — AutoSphere AG):** The AutoSphere AG lab enables predictive optimization on a Delta table and observes automatic compaction of small files after data ingestion. This reduces query latency by 30-50% without manual scheduling.

💡 **Exam Tip:** Predictive Optimization is **not** a replacement for manual OPTIMIZE in all cases — it's optimized for typical workloads. For tables with very tight performance SLAs or unusual write patterns, you may still need manual tuning. The exam tests whether you know **what** it optimizes (files, stats, vacuum) and **how** to enable it (table/schema/catalog property).

---

### 2.2.4 System Tables

#### What Are System Tables?

**System tables** are read-only tables in the built-in `system` catalog that provide observability into your Databricks workspace — audit logs, lineage, billing, and more. They are stored in Unity Catalog and queryable with standard SQL.

#### System Table Schemas

The system catalog is organized into schemas by domain:

```
system
  ├── access (audit logs)
  ├── lineage (data lineage)
  ├── billing (usage and cost)
  ├── information_schema (metadata about objects)
  └── compute (cluster and warehouse usage)
```

#### `system.access.audit` — Audit Logs

Contains detailed audit records of every action in the workspace.

```sql
-- Find all GRANT operations in the last 24 hours
SELECT
    timestamp,
    user_identity,
    action_name,
    request_params
FROM system.access.audit
WHERE
    action_name IN ('grantPrivileges', 'revokePrivileges')
    AND timestamp > current_timestamp() - INTERVAL 1 DAY
ORDER BY timestamp DESC;
```

**Key columns:**

| Column | Description |
|---|---|
| `timestamp` | When the action occurred |
| `user_identity` | Who performed the action (email, service principal) |
| `action_name` | The API action (e.g., `createTable`, `queryTable`, `grantPrivileges`) |
| `request_params` | JSON blob with action-specific details |
| `response.status_code` | HTTP status (200 = success, 403 = denied) |
| `source_ip_address` | Originating IP |

**Use case:** Compliance audits — "Who granted what to whom, and when?"

#### `system.lineage.tables` — Data Lineage

Tracks which tables feed into other tables, views, or notebooks.

```sql
-- Find all tables that feed into the gold.sales_summary table
SELECT
    source_table_catalog,
    source_table_schema,
    source_table_name,
    target_table_catalog,
    target_table_schema,
    target_table_name,
    operation
FROM system.lineage.tables
WHERE
    target_table_name = 'sales_summary'
    AND timestamp > current_timestamp() - INTERVAL 7 DAY;
```

**What this reveals:** Which bronze/silver tables are upstream of a gold table. Essential for impact analysis — "If I modify this bronze table, which downstream reports break?"

#### Viewing Lineage in Catalog Explorer

The **Catalog Explorer** provides a visual lineage view:

1. Navigate to a table in Catalog Explorer
2. Click the **Lineage** tab
3. You see a directed graph showing:
   - **Upstream dependencies**: Tables, views, files, and notebooks that feed into this table
   - **Downstream dependencies**: Tables, views, dashboards, and notebooks that consume this table
   - **Notebook lineage**: Which notebooks read/write this table
   - **Pipeline lineage**: Which Lakeflow Pipelines write to this table

**What lineage includes:**
- Tables and views (both Delta and foreign)
- Notebooks that read/write the table
- Jobs that use the table
- Dashboard queries (Lakeview)
- Lakeflow Pipeline stages

**Impact analysis with lineage:**
- "If I drop column X, which downstream dashboards break?" → Use Catalog Explorer lineage
- "Which source systems feed this gold table?" → Check upstream lineage
- "Who is using this bronze table?" → Check downstream lineage

💡 **Exam Tip:** The exam may ask: "Which tool shows the complete data flow from source to dashboard?" → Catalog Explorer → Lineage tab. The system table `system.lineage.tables` is the SQL queryable backend; Catalog Explorer is the visual interface.

#### `system.billing.usage` — Billing and Usage

```sql
-- Find the top 10 most expensive queries this month
SELECT
    workspace_id,
    sku,
    usage_type,
    usage_quantity,
    usage_unit,
    usage_start_time,
    usage_end_time,
    cloud
FROM system.billing.usage
WHERE
    usage_start_time >= DATE_TRUNC('month', current_date())
ORDER BY usage_quantity DESC
LIMIT 10;
```

#### `system.information_schema` — Metadata

Standard SQL information schema for Unity Catalog objects:

```sql
-- Find all tables tagged as PII
SELECT
    table_catalog,
    table_schema,
    table_name,
    tag_name,
    tag_value
FROM system.information_schema.table_tags
WHERE tag_name = 'pii' AND tag_value = 'true';

-- Find all tables with row filters or column masks
SELECT
    table_catalog,
    table_schema,
    table_name,
    row_filter_name,
    masking_policy_name
FROM system.information_schema.tables
WHERE row_filter_name IS NOT NULL
   OR masking_policy_name IS NOT NULL;
```

#### Enabling System Tables

System tables must be enabled by a workspace admin:

```sql
-- An admin runs this once to enable the system catalog
-- This is done through the Databricks UI under Admin Console → System Tables
```

Or via the Databricks CLI:

```bash
databricks system-schema enable --schema access
databricks system-schema enable --schema lineage
databricks system-schema enable --schema billing
```

🔬 **Lab Reference (Lab 05 — AutoSphere AG):** The AutoSphere lab queries `system.access.audit` to verify compliance — confirming that only authorized users accessed sensitive tables. It also queries `system.lineage.tables` to trace the data pipeline from bronze ingestion through silver cleansing to gold aggregation.

💡 **Exam Tip:** Memorize which system table to use for each scenario:
- **Who granted what?** → `system.access.audit`
- **What tables feed this report?** → `system.lineage.tables`
- **How much did this workspace cost?** → `system.billing.usage`
- **Which tables have PII tags?** → `system.information_schema.table_tags`

---

### 2.2.5 Audit Logging

#### Why Audit Logging Matters

**Audit logging** is the cornerstone of data governance compliance (GDPR, HIPAA, SOC 2, SOX). Every data access, privilege change, and configuration modification should be recorded in an immutable log.

#### Unity Catalog Audit Log Sources

| Source | What It Logs | Retention |
|---|---|---|
| `system.access.audit` | API-level actions (queries, grants, table operations) | Configurable (default: 90 days) |
| Delta transaction log | File-level changes (every write, delete, update) | Configurable (default: 30 days) |
| Cluster event logs | Cluster creation, termination, auto-scaling | 30 days |
| Notebook revision history | Notebook edits (versioned) | Indefinite |

#### Querying for Compliance

```sql
-- Compliance check: Who accessed the PII table in the last 30 days?
SELECT DISTINCT
    user_identity,
    date_trunc('day', timestamp) AS access_day,
    COUNT(*) AS query_count
FROM system.access.audit
WHERE
    action_name = 'queryTable'
    AND request_params:table_full_name = 'main.silver.users'
    AND timestamp > current_timestamp() - INTERVAL 30 DAY
GROUP BY user_identity, date_trunc('day', timestamp)
ORDER BY access_day DESC, query_count DESC;
```

```sql
-- Compliance check: Any failed access attempts (403 errors)?
SELECT
    timestamp,
    user_identity,
    action_name,
    request_params,
    source_ip_address
FROM system.access.audit
WHERE
    response.status_code = 403
    AND timestamp > current_timestamp() - INTERVAL 7 DAY
ORDER BY timestamp DESC;
```

#### Exporting Audit Logs

For long-term retention or SIEM integration (e.g., Azure Sentinel, Splunk), export audit logs to Azure Storage:

```bash
# Configure diagnostic settings in Azure Portal for the Databricks workspace
# Export to: Log Analytics workspace, Storage account, or Event Hub
```

💡 **Exam Tip:** The exam may ask: "How long are audit logs retained in system.access.audit by default?" The answer is **90 days**. For longer retention, you must export logs to Azure Storage or Log Analytics.

---

### 2.2.6 Delta Sharing

#### What Is Delta Sharing?

**Delta Sharing** is an open protocol for secure data sharing across platforms, clouds, and organizations. It is built into Delta Lake and Unity Catalog, allowing you to share data without copying or moving it.

**Key principle:** The data provider controls access entirely through Unity Catalog. The recipient never has direct access to the provider's storage.

#### Sharing Models

##### 1. Databricks-to-Databricks Sharing

Both the provider and recipient use Databricks workspaces with Unity Catalog.

```sql
-- Provider side: Create a share
CREATE SHARE northmart_monthly_sales;

-- Add tables to the share
ALTER SHARE northmart_monthly_sales
ADD TABLE main.silver.sales;

-- Grant access to a recipient
GRANT SELECT ON SHARE northmart_monthly_sales TO `recipient-tenant-id`;
```

The recipient sees the shared tables in their Databricks workspace as read-only Unity Catalog tables.

##### 2. Open Sharing (non-Databricks Recipient)

The recipient receives a **share credential file** (a JSON file with a URL and bearer token) that enables any Delta Sharing-compatible client (Spark, Pandas, Rust, etc.) to read the data.

```sql
-- Provider side: Create an open share (recipient has no Databricks account)
CREATE SHARE northmart_public_sales;
ALTER SHARE northmart_public_sales
ADD TABLE main.silver.sales_agg;

-- Generate the activation link (via UI or API)
-- Recipient gets: {"shareCredentialsVersion":1,"bearerToken":"abc...","endpoint":"https://..."
```

The recipient then reads with any Delta Sharing client:

```python
# Recipient side (Python, any platform)
import delta_sharing

client = delta_sharing.SharingClient("path/to/credential.json")
tables = client.list_tables()
df = delta_sharing.load_as_pandas(client, tables[0])
```

#### Managed vs. Unmanaged Shares

| Type | Storage Location | Best For |
|---|---|---|
| **Managed Share** | Provider's Databricks-managed storage | Simple sharing within your organization |
| **Unmanaged Share** | Provider's own cloud storage (ADLS, S3) | Regulatory compliance where data must stay in a specific region or account |

#### Sharing Best Practices

- **Share only** aggregate or anonymized data when possible
- Use **column masks and row filters** on shared tables before adding them to a share
- **Monitor share usage** via `system.access.audit`
- **Rotate share tokens** regularly (open sharing)
- Create separate shares for different recipients to enable individual revocation

💡 **Exam Tip:** Delta Sharing is **read-only** for recipients. There is no write-sharing. Also remember: **Databricks-to-Databricks** sharing uses the recipient's Unity Catalog namespace; **open sharing** uses credential files and works with any Delta-compatible client.

---

### 2.2.7 Lakehouse Monitoring

#### What Is Lakehouse Monitoring?

**Lakehouse Monitoring** is a Databricks feature that continuously monitors tables for:
- **Data drift** — Changes in distribution of values over time
- **Data quality** — Against user-defined constraints
- **Model performance** — For ML model inference tables

#### Setting Up Monitoring

```py
# Python: Create a monitor on a table
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

w.lakehouse_monitors.create(
    table_name="main.silver.sales",
    assets_dir="/Users/admin/monitoring/sales",
    output_schema_name="main.monitoring",
    data_freshness="1 hour",
    metrics=[
        "count_null",
        "count_distinct",
        "min", "max", "avg", "stddev"
    ],
    timeframe="7 days"
)
```

#### What Monitoring Produces

Lakehouse Monitoring creates a dashboard with:
1. **Profile metrics** — Min, max, mean, null count, distinct count per column
2. **Drift metrics** — Population stability index (PSI) comparing recent data to baseline
3. **Anomaly alerts** — Configurable notifications when metrics exceed thresholds
4. **Data quality checks** — Results of user-defined constraints

#### Monitoring System Tables

Monitoring data is stored in the `system` catalog:

```sql
-- Check monitoring metrics for a table
SELECT * FROM system.lakehouse.monitoring_metrics
WHERE table_name = 'main.silver.sales'
ORDER BY timestamp DESC
LIMIT 10;
```

💡 **Exam Tip:** Lakehouse Monitoring is distinct from querying `system.access.audit`. Monitoring is about **data quality and drift**. Audit logging is about **access and compliance**. Know which tool addresses which governance concern.

---

### 2.2.8 Dynamic Views with `current_user()` and `is_member()`

#### What Are Dynamic Views?

A **dynamic view** is a regular SQL view whose definition includes functions that evaluate at query time based on the **current user's identity**. Unlike row filters (which are invisible), dynamic views are explicit and can incorporate complex logic with joins, subqueries, and multiple conditions.

#### Key Built-in Functions

| Function | Returns | Description |
|---|---|---|
| `current_user()` | STRING | The email or principal ID of the current user |
| `is_member(group_name)` | BOOLEAN | Whether the current user belongs to a Databricks group |
| `current_groups()` | ARRAY<STRING> | List of all groups the current user belongs to |

#### Dynamic View Pattern: Row-Level Security via Views

```sql
-- Create a view that returns different data based on the user's group
CREATE OR REPLACE VIEW main.silver.sales_by_region AS
SELECT * FROM main.bronze.sales_raw
WHERE
    -- Only show rows where the region matches the user's group
    CASE
        WHEN is_member('global_sales') THEN TRUE
        WHEN is_member('us_sales') AND region = 'US' THEN TRUE
        WHEN is_member('eu_sales') AND region = 'EU' THEN TRUE
        ELSE FALSE
    END;
```

**What this does:**
- Global sales team sees all regions
- US sales team sees only US rows
- EU sales team sees only EU rows
- Anyone not in one of these groups sees nothing

#### Dynamic View Pattern: Column-Level Security

```sql
-- Create a view that masks data based on the user's role
CREATE OR REPLACE VIEW main.silver.users_v AS
SELECT
    user_id,
    username,
    -- Non-HR users see masked email
    CASE
        WHEN is_member('hr_team') THEN email
        ELSE regexp_replace(email, '(.)@.*', '***@***')
    END AS email,
    -- Only HR admins see full SSN
    CASE
        WHEN is_member('hr_admins') THEN ssn
        ELSE 'MASKED'
    END AS ssn
FROM main.bronze.users_raw;
```

#### Dynamic View Pattern: Per-User Data Isolation

```sql
-- Each user sees only their own data
CREATE OR REPLACE VIEW main.silver.my_data AS
SELECT * FROM main.bronze.all_data
WHERE owner_email = current_user();
```

#### When to Use Dynamic Views vs. Row Filters vs. Column Masks

| Feature | Row Filter | Column Mask | Dynamic View |
|---|---|---|---|
| Hides rows | ✅ | ❌ | ✅ |
| Masks columns | ❌ | ✅ | ✅ |
| Can join multiple tables | ❌ | ❌ | ✅ |
| Transparent to user | ✅ | ✅ | ❌ (visible in view definition) |
| Can be bypassed | ❌ (always active) | ❌ (always active) | ✅ (table still exists directly) |
| Complex logic (subqueries, CASE) | ❌ (single function) | ❌ (single function) | ✅ |

**Guideline:**
- Use **row filters** for mandatory, transparent row-level restrictions
- Use **column masks** for mandatory, transparent column-level protection
- Use **dynamic views** when you need complex multi-table logic, or when users should query the view explicitly (not the underlying table)

🔬 **Lab Reference (Lab 04 — NorthMart Retail, Lab 05 — AutoSphere AG):** Across both labs, dynamic views are used to implement data isolation for different business units. The NorthMart lab applies row filters and column masks for mandatory controls, while the AutoSphere AG lab uses dynamic views for department-level data partitioning.

💡 **Exam Tip:** The exam may give you a scenario and ask: "Should you use a row filter, a column mask, or a dynamic view?" Decision framework:
1. **Must be invisible and cannot be bypassed?** → Row filter or column mask
2. **Need to hide entire rows?** → Row filter
3. **Need to protect a specific column (PII)?** → Column mask
4. **Need complex multi-table logic or need to expose different columns to different users?** → Dynamic view
5. **Need both row and column control?** → Combine row filter + column mask

---

## Key Exam Tips — What to Memorize

### Privilege Model

| Fact | Memorize |
|---|---|
| Minimum privilege needed to see/use any object | `USAGE` on catalog and schema |
| Prerequisite for all other privileges | `USAGE` (without it, nothing else works) |
| How privileges flow | **Downward** from catalog → schema → table (inheritance) |
| What does NOT inherit | External Location and Storage Credential privileges |
| Unique materialized view privileges | `READ_MATERIALIZED_VIEW` and `REFRESH` |
| Unique volume privileges | `READ` and `WRITE` |

### Row Filters vs. Column Masks vs. Dynamic Views

| Concept | Key Exam Point |
|---|---|
| Row filter must return | `BOOLEAN` (`TRUE` = row visible) |
| Column mask must return | Same type as the column being masked |
| User group check function | `is_member('group_name')` |
| Current user email | `current_user()` |
| Row filter bypass risk | **None** — always active, transparent to user |
| Column mask on INSERT/CTAS | **Applies** — masked values can propagate |

### System Tables

| System Table | What to Query For |
|---|---|
| `system.access.audit` | Audit logs, privilege changes, query history, 403 errors |
| `system.lineage.tables` | Data lineage — upstream/downstream dependencies |
| `system.billing.usage` | Cost and usage by workspace, SKU, and time |
| `system.information_schema.table_tags` | Find tables by tag (e.g., PII classification) |

### VACUUM and Retention

| Fact | Memorize |
|---|---|
| Default VACUUM retention | **168 hours (7 days)** |
| Minimum safe VACUUM retention | 7 days (unless config override) |
| What VACUUM deletes | Files **not referenced** in Delta log AND older than retention |
| What VACUUM does NOT affect | The Delta transaction log (separate retention) |
| Delta log retention default | **30 days** (for time travel) |

### Delta Sharing

| Fact | Memorize |
|---|---|
| Recipient access | **Read-only** — no write sharing |
| Databricks-to-Databricks | Recipient sees shared tables in their own Unity Catalog |
| Open Sharing | Uses credential file + bearer token |
| Who controls permissions | **Provider** always controls access |

### Tags and ABAC

| Fact | Memorize |
|---|---|
| Tag structure | Key-value pair (`'key' = 'value'`) |
| Tag purposes | Classification, discovery, ABAC |
| ABAC stands for | Attribute-Based Access Control |

### Common Exam Traps

1. **The USAGE prerequisite trap:** A user has `SELECT` on a table but no `USAGE` on the schema or catalog → query fails. Always ensure `USAGE` is granted.

2. **The inheritance trap:** Granting a privilege at the catalog level grants it on ALL current and future schemas/tables. This is broader than most people expect.

3. **The VACUUM retention trap:** Default is 168 hours (7 days). Setting it lower requires disabling a safety check and risks breaking concurrent operations.

4. **The column-mask-propagation trap:** Column masks apply during `INSERT ... SELECT` and `CREATE TABLE AS SELECT`. Masked values can end up in new tables without you realizing it.

5. **The dynamic view bypass trap:** Dynamic views are NOT mandatory — if a user has direct access to the underlying table, they can bypass the view. Use row filters/column masks for mandatory controls.

---

## Quick Reference: Essential SQL Commands

```sql
-- Granting privileges
GRANT USAGE ON CATALOG catalog_name TO principal;
GRANT USAGE ON SCHEMA catalog.schema_name TO principal;
GRANT SELECT ON TABLE catalog.schema.table_name TO principal;
GRANT SELECT, MODIFY ON SCHEMA catalog.schema_name TO principal;

-- Revoking privileges
REVOKE SELECT ON TABLE catalog.schema.table_name FROM principal;

-- Row filter
CREATE OR REPLACE FUNCTION catalog.schema.filter_func(col STRING) RETURNS BOOLEAN
  RETURN col = current_user();
ALTER TABLE catalog.schema.table SET ROW FILTER catalog.schema.filter_func;

-- Column mask
CREATE OR REPLACE FUNCTION catalog.schema.mask_func(col STRING)
  RETURN CASE WHEN is_member('group') THEN col ELSE 'REDACTED' END;
ALTER TABLE catalog.schema.table ALTER COLUMN col SET MASK catalog.schema.mask_func;

-- Tags
ALTER TABLE catalog.schema.table SET TAGS ('key' = 'value');

-- VACUUM
VACUUM catalog.schema.table RETAIN 168 HOURS;

-- System tables
SELECT * FROM system.access.audit WHERE action_name = 'queryTable';
SELECT * FROM system.lineage.tables WHERE target_table_name = 'gold_table';

-- Delta Sharing
CREATE SHARE share_name;
ALTER SHARE share_name ADD TABLE catalog.schema.table;
GRANT SELECT ON SHARE share_name TO recipient;

-- Dynamic view
CREATE OR REPLACE VIEW catalog.schema.dynamic_v AS
  SELECT * FROM catalog.schema.base_table
  WHERE region = CASE
    WHEN is_member('global') THEN region
    ELSE current_user()
  END;
```

---

## Lab Case Study Summaries

### Lab 04 — NorthMart Retail (Security)
**Duration:** 35-40 minutes | **Focus:** Securing Unity Catalog Objects

**Scenario:** NorthMart is a global retailer with regional sales teams. Each team should see only their region's data. PII (email addresses) must be masked for most users.

**Steps performed:**
1. Granted schema-level permissions (`SELECT` on `main.silver`) to `data_analysts` group
2. Created a row filter function based on region and applied it to the `sales` table — regional teams see only their rows
3. Created a column mask function for the `email` column and applied it to the `users` table — non-privileged users see `***@***`
4. Created an Azure Key Vault-backed secret scope with Databricks CLI
5. Retrieved storage account keys from the secret scope using `dbutils.secrets.get()`
6. Configured Spark to authenticate to ADLS Gen2 using the retrieved secret

**Key takeaway:** Unity Catalog enables fine-grained, transparent access control without modifying application queries.

### Lab 05 — AutoSphere AG (Governance)
**Duration:** 30 minutes | **Focus:** Governing Unity Catalog Objects

**Scenario:** AutoSphere AG is an automotive parts supplier that needs to demonstrate compliance with data governance regulations. They must classify sensitive data, manage retention, and provide audit trails.

**Steps performed:**
1. Applied tags to tables for PII classification (`pii = 'true'`)
2. Set Delta retention properties and ran `VACUUM RETAIN 168 HOURS`
3. Enabled predictive optimization via table properties
4. Queried `system.lineage.tables` to map the data pipeline from bronze → silver → gold
5. Analyzed `system.access.audit` to verify compliance — confirmed no unauthorized access to PII tables
6. Created dynamic views with `is_member()` for department-level data isolation

**Key takeaway:** A combination of tags, retention policies, system tables, and dynamic views creates a complete data governance framework.

---

> **Next Step:** Practice these concepts by running through Labs 04 and 05 in the Databricks environment. Focus especially on the privilege model (USAGE prerequisite!) and the differences between row filters, column masks, and dynamic views. These distinctions are the highest-leverage study areas for the exam.
