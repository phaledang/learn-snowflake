# Lab 06 Syntax Reference – Data Classification & Secure Loading

Covers the Snowflake constructs demonstrated in:
- `lab-06-load-customers.sql`
- `lab-06-data-classification.sql`

---
## 1. File Format Definition
```sql
CREATE OR REPLACE FILE FORMAT ff_csv_customers
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('\\N','NULL','');
```
**Options Explained:**
| Option | Purpose |
|--------|---------|
| `TYPE = CSV` | Declares CSV parsing semantics |
| `FIELD_OPTIONALLY_ENCLOSED_BY` | Handles quoted fields containing delimiters |
| `SKIP_HEADER = 1` | Ignores first line (column headers) |
| `NULL_IF` | Treats listed tokens as NULL |

**Tip:** Use named file formats to keep COPY commands clean and reusable.

---
## 2. Staging Data
```sql
CREATE OR REPLACE STAGE stg_customers FILE_FORMAT = ff_csv_customers;
```
Creates an **internal named stage** that stores uploaded (PUT) files + inherits the declared format.

### Listing Contents
```sql
LIST @stg_customers;                 -- Show all files
LIST @stg_customers/prefix;          -- Optional path filter
```

### Uploading from Client (CLI – Not SQL)
```bash
snowsql -q "PUT file://lab-06-customers.csv @stg_customers AUTO_COMPRESS=TRUE"
```
**Key Flags:**
- `AUTO_COMPRESS=TRUE` – Gzips file during upload (Snowflake decompresses automatically on load).

---
## 3. COPY INTO (Validation and Load)
### Preview Validation (Dry Run)
```sql
COPY INTO customers
FROM @stg_customers
VALIDATION_MODE = 'RETURN_2_ROWS';
```
Returns sample rows **without** loading table data—great for schema alignment checks.

### Strict Load
```sql
COPY INTO customers
FROM @stg_customers/lab-06-customers.csv.gz
FILE_FORMAT = (FORMAT_NAME = ff_csv_customers)
ON_ERROR = 'ABORT_STATEMENT';
```
**Important Options:**
| Option | Behavior |
|--------|----------|
| `FILE_FORMAT=(FORMAT_NAME=...)` | Reuses registered format |
| `ON_ERROR='ABORT_STATEMENT'` | Stops at first bad row (strict mode) |
| (Alternatives) | `CONTINUE`, `SKIP_FILE`, `SKIP_FILE_<n>` |

### Row Verification
```sql
SELECT COUNT(*) AS loaded_rows FROM customers;
SELECT * FROM customers ORDER BY customer_id;
```

---
## 4. Tag Creation (Data Classification)
```sql
CREATE OR REPLACE TAG pii_classification
  COMMENT = 'Marks columns that contain Personally Identifiable Information (PII)';

CREATE OR REPLACE TAG sensitivity_level
  COMMENT = 'Defines sensitivity classification levels';
```
**Concept:** Tags are reusable metadata labels attachable to objects & columns. They are queryable for governance.

---
## 5. Table Creation for Tagging
```sql
CREATE OR REPLACE TABLE customers (
  customer_id NUMBER,
  full_name   STRING,
  email       STRING,
  phone       STRING,
  balance     NUMBER
);
```

---
## 6. Applying Tags to Columns
```sql
ALTER TABLE customers ALTER COLUMN full_name SET TAG pii_classification = 'true';
ALTER TABLE customers ALTER COLUMN email     SET TAG pii_classification = 'true';
ALTER TABLE customers ALTER COLUMN phone     SET TAG sensitivity_level = 'Confidential';
```
**Notes:**
- Tag values are arbitrary strings (e.g. `'true'`, `'Confidential'`, `'LEVEL3'`).
- Reapplying with `SET TAG` overwrites prior value.
- Remove with: `ALTER TABLE ... ALTER COLUMN col UNSET TAG tag_name;`

---
## 7. Auditing Tag Usage
### Account-wide (Aggregated)
```sql
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
WHERE TAG_NAME IN ('PII_CLASSIFICATION','SENSITIVITY_LEVEL');
```
**Latency:** ACCOUNT_USAGE views may be delayed by up to ~90 minutes.

### Immediate Object-Level Inspection
```sql
SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'CUSTOMERS',
    'TABLE'
  )
);
```
Returns column-level tag metadata for a single table instantly.

---
## 8. Masking Policy + Tag Binding
### Define a Masking Policy
```sql
CREATE MASKING POLICY mask_pii AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ANALYST_ROLE') THEN '***MASKED***'
    ELSE val
  END;
```
**Behavior:** Redacts column values dynamically at query-time based on executing role.

### Attach Policy to a Tag
```sql
ALTER TAG pii_classification SET MASKING POLICY mask_pii;
```
Any column with this tag inherits the masking logic—centralized governance.

**Remove:**
```sql
ALTER TAG pii_classification UNSET MASKING POLICY;
```

---
## 9. Execution Flow Summary
1. Create file format → stage → upload file.
2. Validate with `VALIDATION_MODE`.
3. Load with `COPY INTO` (strict mode).
4. Create classification tags.
5. Create target table & load data (or tag existing one).
6. Apply tags to sensitive columns.
7. (Optional) Bind masking policy to tag.
8. Audit via ACCOUNT_USAGE / INFORMATION_SCHEMA.

---
## 10. Common Variations & Options
| Task | Variation | Use Case |
|------|-----------|----------|
| File format | Add `FIELD_DELIMITER=','` | Explicit delimiter override |
| COPY error handling | `ON_ERROR='CONTINUE'` | Load good rows; log bad ones |
| COPY validation | `VALIDATION_MODE='RETURN_ERRORS'` | See parsing failures |
| Tag values | `'YES' / 'NO'` | Boolean semantics |
| Masking condition | `CURRENT_ROLE() NOT IN (...)` | Inverted access rule |
| Stage type | `CREATE STAGE ... URL='s3://...'` | External (S3/Azure/GCS) integration |

---
## 11. Best Practices
| Area | Recommendation |
|------|----------------|
| Staging | Keep raw files compressed (gzip saves storage & transfer) |
| Governance | Standardize tag names + value taxonomies (e.g. PII: low/med/high) |
| Security | Use role hierarchy: masked → unmasked via controlled role grants |
| Auditing | Schedule periodic export of `TAG_REFERENCES` snapshot |
| Policies | Keep generic (role-based) rather than user-specific |
| Idempotency | Use `CREATE OR REPLACE` only in dev; avoid in prod unless intentional |

---
**Next:** Continue to Lab 07 for API + AI-driven interaction layers using classified data securely.
