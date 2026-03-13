slidenumbers: true
autoscale: true
build-lists: true
footer: madsql overview

# [fit] madsql 🤬
## Deterministic SQL dialect transcoding from the command line

### Matt DeMarco
### matthew.demarco@oracle.com

---

# Why madsql exists

- SQL migrations are repetitive, brittle, and easy to do inconsistently.
- Batch conversion gets harder when scripts contain mixed formatting and multiple statements.
- Schema context is often missing when all you have is a query workload.
- `madsql` wraps `SQLGlot` and `sqlparse` into one deterministic CLI workflow.

---

# What ships today

- `madsql dialects`
  List supported SQL dialect names.
- `madsql convert`
  Convert SQL between dialects.
- `madsql split-statements`
  Split multi-statement SQL into one file per statement.
- `madsql infer-schema`
  Infer creation DDL or JSON from SQL workloads.

---

# Supported dialects

Run this to see the current list:

```bash
madsql dialects
```

Examples from the current repo runtime (32 in total):

`athena`, `bigquery`, `clickhouse`, `duckdb`, `mysql`, `oracle`, `postgres`, `singlestore`, `snowflake`, `spark`, `sqlite`, `trino`, `tsql`

---

# Install and verify

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
madsql --version
```

Expected runtime shape:

```text
madsql 0.11.0
python 3.13.x
sqlglot 29.0.1
sqlparse 0.5.3
```

---

# Demo 1
## Convert one statement from stdin

```bash
echo 'SELECT TOP 3 [name] FROM dbo.users;' | madsql convert --source tsql --target oracle
```

Output:

```sql
SELECT "name" FROM dbo.users FETCH FIRST 3 ROWS ONLY
```

---

# Demo 2
## Convert a file or directory tree

[.column]
Single file to stdout:

```bash
madsql convert --source postgres --target mysql ./input.sql
```

Directory to an output tree:

```bash
madsql convert --source postgres --target mysql --in ./sql --out ./converted
```


---

# Demo 3
## Split multi-statement SQL

```bash
madsql split-statements \
  --in ./examples/input/mssql/example_queries.sql \
  --out ./split
```

Resulting layout:

```text
./split/examples/input/mssql/example_queries/0001_stmt.sql
./split/examples/input/mssql/example_queries/0002_stmt.sql
./split/examples/input/mssql/example_queries/0003_stmt.sql
...
```

This is useful when a single script needs review, reordering, or selective replay.

---

# Demo 4
## Infer schema from a query workload

```bash
madsql infer-schema \
  --source singlestore \
  ./examples/input/singlestore/nyc_taxi_queries.sql
```

Sample output:

```sql
CREATE TABLE nyc_taxi.neighborhoods (
  id BIGINT,
  name TEXT,
  polygon GEOGRAPHY
);

CREATE TABLE nyc_taxi.trips (
  accept_time DOUBLE,
  dropoff_location GEOGRAPHY,
  ...
);
```

---
# Demo 4 (cont)
## Infer schema from a query workload for specific target

```bash
madsql infer-schema \
  --source singlestore \
  --input ./examples/input/singlestore/nyc_taxi_queries.sql \
  --target oracle \
  --create-user \
  --create-user-password 'matt' \
  --pretty
```
---
Sample output:

```sql
CREATE USER nyc_taxi IDENTIFIED BY "matt";
GRANT CREATE SESSION, CREATE TABLE TO nyc_taxi;

ALTER SESSION SET CURRENT_SCHEMA = nyc_taxi;
```
---
Sample output: 

```sql
CREATE TABLE nyc_taxi.neighborhoods (
  id INT,
  name CLOB,
  polygon SDO_GEOMETRY
);

CREATE TABLE nyc_taxi.trips (
  accept_time NUMBER,
  dropoff_location SDO_GEOMETRY,
  dropoff_time NUMBER,
  num_riders NUMBER,
  pickup_location SDO_GEOMETRY,
  pickup_time NUMBER,
  price NUMBER,
  request_time NUMBER,
  status CLOB
);
```

---
# Demo 4 (cont)
## Infer schema from stdin

```bash
echo "select x.xxx, y.yyy from foo x, bar y where x.xxx=y.yyy limit 10" | \
madsql infer-schema --target oracle --default-type 'varchar2(100)' --pretty

```
--- 

# Sample output: 

```sql
CREATE TABLE bar (
  yyy VARCHAR2(100)
);

CREATE TABLE foo (
  xxx VARCHAR2(100)
);

```
---
# Demo 4 (cont)
## Infer schema from stdin & derive types

```bash
echo "select x.xxx + y.yyy from foo x, bar y where x.xxx=y.yyy limit 10" | \
madsql infer-schema --target oracle --default-type 'varchar2(100)' --pretty

```

---
# Sample output:

```sql
CREATE TABLE bar (
  yyy NUMBER
);

CREATE TABLE foo (
  xxx NUMBER
);
```

---

# Inference details

- `infer-schema` merges evidence from multiple statements.
- `CREATE TABLE` contributes explicit types.
- `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE VIEW`, and `CREATE INDEX` add table and column references.
- Output can be DDL or JSON.
- Unqualified columns can be skipped or assigned with low confidence.

---

# Observability and error handling

- Default behavior is continue-on-error with a non-zero exit code if failures were recorded.
- `--fail-fast` stops on the first failure.
- `--ignore-errors` returns exit code `0` while still recording diagnostics.
- `--errors errors.json` writes a structured JSON report.
- `--log 0` or `--log 1` writes run logs.
- `--report` writes a timestamped Markdown summary.

---

# Why it works well in CI

- Deterministic outputs for identical inputs and `SQLGlot` version.
- Stable statement ordering and UTF-8 output.
- Predictable artifact names such as `input.postgres.sql` and `inferred_schema-postgres.sql`.
- Clear exit codes:
  - `0` for success
  - `1` for completed runs with recorded parse or conversion errors
  - `2` for CLI misuse

---

# Good fits

- Cross-dialect migration prep
- Large SQL cleanup efforts
- Batch conversion for repositories
- Splitting vendor scripts into reviewable units
- Inferring starter DDL from analytic workloads
- CI checks around SQL normalization

---

# Boundaries

- This is a CLI, not a GUI.
- It intentionally builds on `SQLGlot` and `sqlparse`; it does not hide that dependency.
- Output quality depends on parser support for the source dialect and syntax used.
- It is strongest when you want reproducible automation, not hand-tuned one-off rewrites.

---

# [fit] Recommended first commands

```bash
madsql dialects
madsql --help
madsql convert --help
madsql split-statements --help
madsql infer-schema --help
```

If the console script is not on your `PATH`, use:

```bash
python3 -m madsql <command>
```

---

# [fit] Get Started!

Repository quick start:

```bash
git clone https://github.com/oramatt/madsql.git
pip install -e .
madsql --version
```

Core idea:

> deterministic SQL conversion, splitting, and schema inference in one CLI

---
# [fit] Questions?




---

# [fit] Thank you