from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

import sqlparse
from sqlglot import __version__ as SQLGLOT_VERSION
from sqlglot.errors import TokenError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from madsql import cli as cli_module
from madsql import infer_schema as infer_schema_module
from madsql import __version__ as MADSQL_VERSION
from madsql.convert import ConversionResult, RenderedStatement
from madsql.errors import ConversionError

FIXTURES = ROOT / "tests" / "fixtures"
ENV = {
    **os.environ,
    "PYTHONPATH": str(ROOT / "src"),
}


def run_cli(*args: str, input_text: str | None = None, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "madsql", *args],
        input=input_text,
        text=True,
        capture_output=True,
        cwd=cwd or ROOT,
        env=ENV,
        check=False,
    )


def read_error_report(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


class CliTests(unittest.TestCase):
    def test_conversion_fixtures_across_dialects(self) -> None:
        cases = json.loads((FIXTURES / "conversion_cases.json").read_text(encoding="utf-8"))
        for case in cases:
            with self.subTest(case=case["name"]):
                result = run_cli(
                    "convert",
                    "--source",
                    case["source"],
                    "--target",
                    case["target"],
                    input_text=case["input"],
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, case["expected"])

    def test_parse_failure_fixtures_across_dialects(self) -> None:
        cases = json.loads((FIXTURES / "parse_failure_cases.json").read_text(encoding="utf-8"))
        for case in cases:
            with self.subTest(case=case["name"]):
                result = run_cli(
                    "convert",
                    "--source",
                    case["source"],
                    "--target",
                    case["target"],
                    "--errors",
                    "errors.json",
                    input_text=case["input"],
                )
                self.assertEqual(result.returncode, 1)
                self.assertIn(case["expected_message_contains"], result.stderr)
                report_payload = read_error_report(ROOT / "errors.json")
                payload = report_payload["errors"]
                self.assertEqual(report_payload["version_info"]["madsql"], MADSQL_VERSION)
                self.assertEqual(payload[0]["error_type"], case["expected_error_type"])
                self.assertEqual(payload[0]["statement_index"], 1)
                self.assertIsNone(payload[0]["path"])
                (ROOT / "errors.json").unlink()

    def test_translation_edge_fixtures(self) -> None:
        cases = json.loads((FIXTURES / "translation_edge_cases.json").read_text(encoding="utf-8"))
        for case in cases:
            with self.subTest(case=case["name"]):
                result = run_cli(
                    "convert",
                    "--source",
                    case["source"],
                    "--target",
                    case["target"],
                    input_text=case["input"],
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, case["expected"])

    def test_dialects_lists_known_dialect(self) -> None:
        result = run_cli("dialects")
        self.assertEqual(result.returncode, 0)
        self.assertIn("postgres", result.stdout.splitlines())

    def test_version_displays_runtime_information(self) -> None:
        result = run_cli("--version")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"madsql {MADSQL_VERSION}", result.stdout)
        self.assertIn("python ", result.stdout)
        self.assertIn(f"sqlglot {SQLGLOT_VERSION}", result.stdout)
        self.assertIn(f"sqlparse {sqlparse.__version__}", result.stdout)
        self.assertIn("platform ", result.stdout)
        self.assertEqual(result.stderr, "")

    def test_help_includes_examples(self) -> None:
        result = run_cli("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("Start here:", result.stdout)
        self.assertIn("Quick examples:", result.stdout)
        self.assertIn("madsql convert --source postgres --target mysql --in ./sql --out ./converted", result.stdout)
        self.assertIn("madsql split-statements --in ./sql --out ./split", result.stdout)
        self.assertIn("madsql infer-schema --source singlestore ./queries.sql", result.stdout)

    def test_convert_help_includes_feature_examples(self) -> None:
        result = run_cli("convert", "--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("Input rules:", result.stdout)
        self.assertIn("Output rules:", result.stdout)
        self.assertIn("Observability:", result.stdout)
        self.assertIn("Examples:", result.stdout)
        self.assertIn("Split multi-statement SQL into one file per statement:", result.stdout)
        self.assertIn("--continue --log 1 --report", result.stdout)
        self.assertIn("--infer-schema", result.stdout)
        self.assertIn("--compact", result.stdout)
        self.assertIn("--infer-schema-create-schema", result.stdout)
        self.assertIn("--infer-schema-create-user", result.stdout)
        self.assertIn("Fatal CLI misuse exits with code 2", result.stdout)

    def test_split_help_includes_feature_examples(self) -> None:
        result = run_cli("split-statements", "--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("Split SQL into one output file per detected statement.", result.stdout)
        self.assertIn("--source is optional.", result.stdout)
        self.assertIn("madsql split-statements --in ./sql --out ./split", result.stdout)
        self.assertIn("--pretty", result.stdout)
        self.assertIn("--compact", result.stdout)
        self.assertIn("--continue --log 1 --report", result.stdout)
        self.assertIn("--infer-schema", result.stdout)
        self.assertIn("--infer-schema-create-schema", result.stdout)
        self.assertIn("--infer-schema-create-user", result.stdout)

    def test_infer_schema_help_includes_feature_examples(self) -> None:
        result = run_cli("infer-schema", "--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("Infer table schemas from SQL and emit creation DDL or JSON.", result.stdout)
        self.assertIn("madsql infer-schema --source mysql --in ./sql --out ./artifacts/schema.sql", result.stdout)
        self.assertIn("--format json", result.stdout)
        self.assertIn("--log 1 --report", result.stdout)
        self.assertIn("--unqualified-columns", result.stdout)
        self.assertIn("--compact", result.stdout)
        self.assertIn("--create-schema", result.stdout)
        self.assertIn("--create-user", result.stdout)
        self.assertIn("Common supported values: TEXT, VARCHAR(255), VARCHAR2(100)", result.stdout)

    def test_infer_schema_empty_invocation_prints_help_and_exits_2(self) -> None:
        result = run_cli("infer-schema")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("Infer table schemas from SQL and emit creation DDL or JSON.", result.stderr)
        self.assertIn("madsql infer-schema --source mysql --in ./sql --out ./artifacts/schema.sql", result.stderr)

    def test_convert_single_file_to_stdout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT TOP 1 * FROM foo;", encoding="utf-8")
            result = run_cli("convert", "--source", "tsql", "--target", "postgres", str(path), cwd=root)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("LIMIT 1", result.stdout)

    def test_convert_supports_pretty_and_compact_output(self) -> None:
        sql = "SELECT a, b FROM t WHERE c = 1;"
        compact = run_cli(
            "convert",
            "--source",
            "postgres",
            "--target",
            "postgres",
            "--compact",
            input_text=sql,
        )
        self.assertEqual(compact.returncode, 0, compact.stderr)
        self.assertNotIn("\nFROM t\n", compact.stdout)

        pretty = run_cli(
            "convert",
            "--source",
            "postgres",
            "--target",
            "postgres",
            "--pretty",
            input_text=sql,
        )
        self.assertEqual(pretty.returncode, 0, pretty.stderr)
        self.assertIn("\nFROM t\n", pretty.stdout)

    def test_convert_single_file_with_in_argument(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT TOP 1 * FROM foo;", encoding="utf-8")
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "tsql",
                "--target",
                "postgres",
                "--in",
                str(path),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((out_dir / "input.postgres.sql").exists())

    def test_convert_single_file_with_input_output_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT TOP 1 * FROM foo;", encoding="utf-8")
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "tsql",
                "--target",
                "postgres",
                "--input",
                str(path),
                "--output",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((out_dir / "input.postgres.sql").exists())

    def test_convert_directory_with_in_argument(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sql_dir = root / "sql"
            nested = sql_dir / "nested"
            nested.mkdir(parents=True)
            (sql_dir / "a.sql").write_text("SELECT 1;", encoding="utf-8")
            (nested / "b.sql").write_text("SELECT 2;", encoding="utf-8")
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                "--in",
                str(sql_dir),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((out_dir / "sql" / "a.mysql.sql").exists())
            self.assertTrue((out_dir / "sql" / "nested" / "b.mysql.sql").exists())

    def test_convert_from_stdin_to_stdout(self) -> None:
        result = run_cli(
            "convert",
            "--source",
            "mysql",
            "--target",
            "postgres",
            input_text="SELECT `name` FROM users;",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('"name"', result.stdout)

    def test_convert_ignores_empty_statements_created_by_extra_delimiters(self) -> None:
        result = run_cli(
            "convert",
            "--source",
            "oracle",
            "--target",
            "postgres",
            input_text=";\nSELECT 1 FROM dual;",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "SELECT 1 FROM dual\n")

    def test_multiple_inputs_require_out(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a.sql").write_text("SELECT 1;", encoding="utf-8")
            (root / "b.sql").write_text("SELECT 2;", encoding="utf-8")
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                "a.sql",
                "b.sql",
                cwd=root,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("--out is required", result.stderr)

    def test_multiple_in_arguments_require_out(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a.sql").write_text("SELECT 1;", encoding="utf-8")
            (root / "b.sql").write_text("SELECT 2;", encoding="utf-8")
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                "--in",
                "a.sql",
                "--in",
                "b.sql",
                cwd=root,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("--out is required", result.stderr)

    def test_multiple_input_arguments_require_out(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a.sql").write_text("SELECT 1;", encoding="utf-8")
            (root / "b.sql").write_text("SELECT 2;", encoding="utf-8")
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                "--input",
                "a.sql",
                "--input",
                "b.sql",
                cwd=root,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("--out is required", result.stderr)

    def test_continue_and_fail_fast_conflict_for_convert(self) -> None:
        result = run_cli(
            "convert",
            "--source",
            "postgres",
            "--target",
            "mysql",
            "--continue",
            "--fail-fast",
            input_text="SELECT 1;",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("Use either --continue or --fail-fast", result.stderr)

    def test_continue_and_fail_fast_conflict_for_split(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT 1;", encoding="utf-8")
            result = run_cli(
                "split-statements",
                "--in",
                str(path),
                "--out",
                str(root / "split"),
                "--continue",
                "--fail-fast",
                cwd=root,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("Use either --continue or --fail-fast", result.stderr)

    def test_continue_and_fail_fast_conflict_for_infer_schema(self) -> None:
        result = run_cli(
            "infer-schema",
            "--continue",
            "--fail-fast",
            input_text="SELECT 1;",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("Use either --continue or --fail-fast", result.stderr)

    def test_infer_schema_report_requires_out(self) -> None:
        result = run_cli(
            "infer-schema",
            "--source",
            "postgres",
            "--report",
            input_text="SELECT id FROM users;",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--report requires --out", result.stderr)

    def test_infer_schema_log_requires_out(self) -> None:
        result = run_cli(
            "infer-schema",
            "--source",
            "postgres",
            "--log",
            "1",
            input_text="SELECT id FROM users;",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--log requires --out", result.stderr)

    def test_convert_infer_schema_requires_out(self) -> None:
        result = run_cli(
            "convert",
            "--source",
            "postgres",
            "--target",
            "mysql",
            "--infer-schema",
            input_text="SELECT id FROM users;",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--infer-schema requires --out", result.stderr)

    def test_infer_schema_create_user_requires_password(self) -> None:
        result = run_cli(
            "infer-schema",
            "--source",
            "singlestore",
            "--target",
            "oracle",
            "--create-user",
            input_text="USE nyc_taxi; SELECT id FROM neighborhoods;",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--create-user-password is required with --create-user", result.stderr)

    def test_infer_schema_create_schema_is_not_supported_for_oracle(self) -> None:
        result = run_cli(
            "infer-schema",
            "--source",
            "singlestore",
            "--target",
            "oracle",
            "--create-schema",
            input_text="USE nyc_taxi; SELECT id FROM neighborhoods;",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--create-schema is not supported for oracle; use --create-user instead", result.stderr)

    def test_infer_schema_from_query_workload_outputs_creation_ddl(self) -> None:
        sql = """
use nyc_taxi;

SELECT COUNT(*) num_rides, n.name
FROM trips t, neighborhoods n
WHERE
    n.id IN (
        SELECT id FROM neighborhoods
    ) AND
    GEOGRAPHY_INTERSECTS(t.pickup_location, n.polygon)
GROUP BY n.name
ORDER BY num_rides DESC;

SELECT ROUND(AVG(pickup_time - request_time) / 60,2) val
FROM trips t, neighborhoods n
WHERE
    n.id IN (
        SELECT id FROM neighborhoods
    ) AND
    GEOGRAPHY_INTERSECTS(t.pickup_location, n.polygon) AND
    pickup_time != 0 AND
    request_time != 0;

SELECT ROUND(AVG(dropoff_time - pickup_time) / 60, 2) val
FROM trips t, neighborhoods n
WHERE
    status = "completed" AND
    n.id IN (
        SELECT id FROM neighborhoods
    ) AND
    GEOGRAPHY_INTERSECTS(t.pickup_location, n.polygon);
"""
        result = run_cli("infer-schema", "--source", "singlestore", input_text=sql)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("CREATE TABLE", result.stdout)
        self.assertIn("nyc_taxi", result.stdout)
        self.assertIn("neighborhoods", result.stdout)
        self.assertIn("trips", result.stdout)
        self.assertIn("pickup_location GEOGRAPHY", result.stdout)
        self.assertIn("polygon GEOGRAPHY", result.stdout)
        self.assertIn("`status` TEXT", result.stdout)
        self.assertIn("pickup_time DOUBLE", result.stdout)
        self.assertIn("request_time DOUBLE", result.stdout)
        self.assertIn("id BIGINT", result.stdout)
        self.assertIn("-- Low-confidence columns were assigned from unqualified references:", result.stdout)

    def test_infer_schema_recovers_dashboard_template_sql(self) -> None:
        sql = """
WITH globalTraffic AS (
    SELECT startTime, globalId, SUM(egBytes) * 8 AS egBytes
    FROM GLOBALACCOUNTTRAFFIC aT
    WHERE aT.accountId IN ()
    GROUP BY startTime, globalId
)
SELECT DISTINCT globalId AS ""Parent Account Id""
FROM globalTraffic;

SELECT max(value) AS ""MAXIMUM""
FROM (
    SELECT startTime AS time_sec, SUM(egBytes) * 8 AS value, 'nse' AS source
    FROM NSE_ACCOUNT_TRAFFIC aT
    WHERE aT.globalAccountId IN ($globalAccount)
      AND aT.accountId IN ($account)
    GROUP BY startTime
) q;
"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.sql"
            path.write_text(sql, encoding="utf-8")
            result = run_cli("infer-schema", "--input", str(path))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stderr, "")
            self.assertIn("CREATE TABLE GLOBALACCOUNTTRAFFIC", result.stdout.upper())
            self.assertIn("CREATE TABLE NSE_ACCOUNT_TRAFFIC", result.stdout.upper())

    def test_infer_schema_recovers_partial_schema_after_statement_fallback(self) -> None:
        sql = """
WITH globalTraffic AS (
    SELECT startTime, globalId, SUM(egBytes) * 8 AS egBytes
    FROM GLOBALACCOUNTTRAFFIC aT
    WHERE aT.accountId IN ()
    GROUP BY startTime, globalId
)
SELECT DISTINCT globalId AS ""Parent Account Id""
FROM globalTraffic;

SELECT max(value) AS ""MAXIMUM""
FROM (
    SELECT startTime AS time_sec, SUM(egBytes) * 8 AS value, 'nse' AS source
    FROM NSE_ACCOUNT_TRAFFIC aT
    WHERE aT.globalAccountId IN ($globalAccount)
      AND aT.accountId IN ($account)
    GROUP BY startTime
) q;

WITH latest_map AS (
    SELECT accountId, cpcode
    FROM cpcodeTraffic
SELECT broken
FROM latest_map;
"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.sql"
            path.write_text(sql, encoding="utf-8")
            result = run_cli("infer-schema", "--input", str(path))
            self.assertEqual(result.returncode, 1)
            self.assertIn("CREATE TABLE GLOBALACCOUNTTRAFFIC", result.stdout.upper())
            self.assertIn("CREATE TABLE NSE_ACCOUNT_TRAFFIC", result.stdout.upper())
            self.assertIn("statement 3: parse_error", result.stderr)
            self.assertNotIn("statement 1: parse_error", result.stderr)
            self.assertNotIn("statement 2: parse_error", result.stderr)

    def test_infer_schema_canonicalizes_placeholder_identifiers(self) -> None:
        sql = """
SELECT geo.$countryCodeIdField, SUM(egBytes)
FROM $dataFromTable geo
WHERE startTime >= ${__from:date:seconds}
GROUP BY geo.$countryCodeIdField;
"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.sql"
            path.write_text(sql, encoding="utf-8")
            result = run_cli("infer-schema", "--input", str(path))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("CREATE TABLE madsql_dataFromTable", result.stdout)
            self.assertIn("madsql_countryCodeIdField TEXT", result.stdout)
            self.assertNotIn("CREATE TABLE $dataFromTable", result.stdout)
            self.assertNotIn("$countryCodeIdField", result.stdout)

    def test_infer_schema_recovers_oracle_create_table_with_storage_suffixes(self) -> None:
        sql = """
CREATE TABLE customers
    ( customer_id        NUMBER(12)
    , cust_first_name    VARCHAR2(30) CONSTRAINT cust_fname_nn NOT NULL
    , cust_last_name     VARCHAR2(30) CONSTRAINT cust_lname_nn NOT NULL
    , customer_since     DATE
    ) &compress initrans 16 STORAGE (INITIAL 8M NEXT 8M);
"""
        result = run_cli("infer-schema", "--continue", input_text=sql)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertIn("CREATE TABLE customers", result.stdout)
        self.assertIn("customer_id DECIMAL(12)", result.stdout)
        self.assertIn("cust_first_name VARCHAR(30)", result.stdout)
        self.assertIn("customer_since DATE", result.stdout)

    def test_infer_schema_recovers_oracle_partitioned_create_table(self) -> None:
        sql = """
CREATE TABLE orders
    ( order_id           NUMBER(12)
    , order_date         TIMESTAMP WITH LOCAL TIME ZONE CONSTRAINT order_date_nn NOT NULL
    , warehouse_id       NUMBER(6)
    ) &compress initrans 16
    PARTITION BY HASH(order_id)
    PARTITIONS 16;
"""
        result = run_cli("infer-schema", "--source", "oracle", input_text=sql)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertIn("CREATE TABLE orders", result.stdout)
        self.assertIn("order_id NUMBER(12)", result.stdout)
        self.assertIn("order_date TIMESTAMPLTZ", result.stdout)
        self.assertIn("warehouse_id NUMBER(6)", result.stdout)

    def test_infer_schema_supports_create_view_materialized_view_and_index(self) -> None:
        sql = """
CREATE VIEW recent_users AS SELECT id FROM users;
CREATE MATERIALIZED VIEW user_names AS SELECT name FROM users;
CREATE INDEX idx_users_email ON users (email);
"""
        result = run_cli("infer-schema", "--source", "postgres", "--format", "json", input_text=sql)
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["statement_count"], 3)
        table_names = {table["name"] for table in payload["tables"]}
        self.assertIn("users", table_names)
        users = next(table for table in payload["tables"] if table["name"] == "users")
        user_columns = {column["name"] for column in users["columns"]}
        self.assertIn("id", user_columns)
        self.assertIn("name", user_columns)
        self.assertIn("email", user_columns)

    def test_infer_schema_reports_unsupported_statements_in_artifacts_and_continues(self) -> None:
        sql = """
SELECT id FROM users;
ALTER TABLE users ADD COLUMN name TEXT;
INSERT INTO users (email) VALUES ('a@example.com');
"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text(sql, encoding="utf-8")
            out_dir = root / "artifacts"
            result = run_cli(
                "infer-schema",
                "--source",
                "postgres",
                "--input",
                str(path),
                "--out",
                str(out_dir),
                "--report",
                "--log",
                "1",
                "--errors",
                "errors.json",
                "--continue",
                cwd=root,
            )
            self.assertEqual(result.returncode, 1)
            schema_payload = (out_dir / "inferred_schema-postgres.sql").read_text(encoding="utf-8")
            self.assertIn("CREATE TABLE users", schema_payload)
            self.assertIn("id BIGINT", schema_payload)
            self.assertIn("email TEXT", schema_payload)

            report_payload = read_error_report(out_dir / "errors.json")
            errors_payload = report_payload["errors"]
            self.assertEqual(report_payload["version_info"]["madsql"], MADSQL_VERSION)
            self.assertEqual(len(errors_payload), 1)
            self.assertEqual(errors_payload[0]["error_type"], "unsupported_statement")
            self.assertEqual(errors_payload[0]["statement_index"], 2)
            self.assertEqual(errors_payload[0]["statement_type"], "ALTER TABLE")
            self.assertIn("ALTER TABLE users ADD COLUMN name TEXT", errors_payload[0]["statement_sql"])

            log_path = next(out_dir.glob("*-madsql-infer-schema.log"))
            log_payload = log_path.read_text(encoding="utf-8")
            self.assertIn("type=unsupported_statement", log_payload)
            self.assertIn("statement_type=ALTER TABLE", log_payload)
            self.assertIn("ALTER TABLE users ADD COLUMN name TEXT", log_payload)

            report_path = next(out_dir.glob("*-madsql-infer-schema-report.md"))
            report_payload = report_path.read_text(encoding="utf-8")
            self.assertIn("## Error Details", report_payload)
            self.assertIn("`unsupported_statement`", report_payload)
            self.assertIn("`ALTER TABLE`", report_payload)
            self.assertIn("ALTER TABLE users ADD COLUMN name TEXT", report_payload)

    def test_infer_schema_fail_fast_stops_after_first_unsupported_statement_in_file(self) -> None:
        sql = """
SELECT id FROM users;
ALTER TABLE users ADD COLUMN name TEXT;
INSERT INTO users (email) VALUES ('a@example.com');
"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text(sql, encoding="utf-8")
            out_dir = root / "artifacts"
            result = run_cli(
                "infer-schema",
                "--source",
                "postgres",
                "--input",
                str(path),
                "--out",
                str(out_dir),
                "--errors",
                "errors.json",
                "--fail-fast",
                cwd=root,
            )
            self.assertEqual(result.returncode, 1)
            schema_payload = (out_dir / "inferred_schema-postgres.sql").read_text(encoding="utf-8")
            self.assertIn("id BIGINT", schema_payload)
            self.assertNotIn("email", schema_payload)
            report_payload = read_error_report(out_dir / "errors.json")
            errors_payload = report_payload["errors"]
            self.assertEqual(len(errors_payload), 1)
            self.assertEqual(errors_payload[0]["statement_type"], "ALTER TABLE")

    def test_infer_schema_can_skip_unqualified_columns(self) -> None:
        sql = """
SELECT status, n.name
FROM trips t, neighborhoods n
WHERE n.id = 1;
"""
        result = run_cli(
            "infer-schema",
            "--source",
            "postgres",
            "--unqualified-columns",
            "skip",
            input_text=sql,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("CREATE TABLE neighborhoods", result.stdout)
        self.assertNotIn("status", result.stdout)
        self.assertNotIn("-- Low-confidence columns", result.stdout)

    def test_infer_schema_supports_pretty_and_compact_output(self) -> None:
        sql = "CREATE TABLE users (id INT, name TEXT);"
        compact = run_cli("infer-schema", "--source", "postgres", "--compact", input_text=sql)
        self.assertEqual(compact.returncode, 0, compact.stderr)
        self.assertIn("CREATE TABLE users (id INT, name TEXT);", compact.stdout)
        self.assertNotIn("CREATE TABLE users (\n", compact.stdout)

        pretty = run_cli("infer-schema", "--source", "postgres", "--pretty", input_text=sql)
        self.assertEqual(pretty.returncode, 0, pretty.stderr)
        self.assertIn("CREATE TABLE users (\n", pretty.stdout)
        self.assertIn("\n  id INT,\n", pretty.stdout)

    def test_infer_schema_renders_inferred_double_as_number_for_oracle(self) -> None:
        sql = "SELECT ROUND(AVG(price), 2) FROM trips;"
        result = run_cli("infer-schema", "--source", "singlestore", "--target", "oracle", input_text=sql)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("price NUMBER", result.stdout)
        self.assertNotIn("DOUBLE PRECISION", result.stdout)

    def test_infer_schema_preserves_explicit_double_for_oracle(self) -> None:
        sql = "CREATE TABLE trips (price DOUBLE);"
        result = run_cli("infer-schema", "--source", "singlestore", "--target", "oracle", input_text=sql)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("price DOUBLE PRECISION", result.stdout)

    def test_infer_schema_renders_geography_as_sdo_geometry_for_oracle(self) -> None:
        sql = "SELECT GEOGRAPHY_INTERSECTS(t.pickup_location, n.polygon) FROM trips t, neighborhoods n;"
        result = run_cli("infer-schema", "--source", "singlestore", "--target", "oracle", input_text=sql)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("pickup_location SDO_GEOMETRY", result.stdout)
        self.assertIn("polygon SDO_GEOMETRY", result.stdout)
        self.assertNotIn(" GEOGRAPHY", result.stdout)

    def test_infer_schema_renders_explicit_geography_as_sdo_geometry_for_oracle(self) -> None:
        sql = "CREATE TABLE places (shape GEOGRAPHY);"
        result = run_cli("infer-schema", "--source", "singlestore", "--target", "oracle", input_text=sql)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("shape SDO_GEOMETRY", result.stdout)

    def test_infer_schema_can_prepend_oracle_create_user_statements(self) -> None:
        sql = "USE nyc_taxi; CREATE TABLE neighborhoods (id INT, name TEXT);"
        result = run_cli(
            "infer-schema",
            "--source",
            "singlestore",
            "--target",
            "oracle",
            "--create-user",
            "--create-user-password",
            "ChangeMe123",
            input_text=sql,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('CREATE USER nyc_taxi IDENTIFIED BY "ChangeMe123";', result.stdout)
        self.assertIn("GRANT CREATE SESSION, CREATE TABLE TO nyc_taxi;", result.stdout)
        self.assertIn("ALTER SESSION SET CURRENT_SCHEMA = nyc_taxi;", result.stdout)
        self.assertIn("CREATE TABLE nyc_taxi.neighborhoods", result.stdout)

    def test_infer_schema_renders_explicit_schema_and_database_as_oracle_users(self) -> None:
        sql = "CREATE SCHEMA analytics; CREATE DATABASE reporting;"
        result = run_cli(
            "infer-schema",
            "--source",
            "postgres",
            "--target",
            "oracle",
            "--create-user-password",
            "ChangeMe123",
            input_text=sql,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertIn('CREATE USER analytics IDENTIFIED BY "ChangeMe123";', result.stdout)
        self.assertIn("GRANT CREATE SESSION, CREATE TABLE TO analytics;", result.stdout)
        self.assertIn('CREATE USER reporting IDENTIFIED BY "ChangeMe123";', result.stdout)
        self.assertIn("GRANT CREATE SESSION, CREATE TABLE TO reporting;", result.stdout)

    def test_infer_schema_explicit_oracle_schema_statements_require_password(self) -> None:
        result = run_cli(
            "infer-schema",
            "--source",
            "postgres",
            "--target",
            "oracle",
            input_text="CREATE SCHEMA analytics;",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn(
            "--create-user-password is required to render input CREATE SCHEMA/CREATE DATABASE statements for oracle",
            result.stderr,
        )

    def test_infer_schema_can_prepend_create_schema_statements_for_postgres(self) -> None:
        sql = "USE nyc_taxi; CREATE TABLE neighborhoods (id INT, name TEXT);"
        result = run_cli(
            "infer-schema",
            "--source",
            "singlestore",
            "--target",
            "postgres",
            "--create-schema",
            input_text=sql,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("CREATE SCHEMA IF NOT EXISTS nyc_taxi;", result.stdout)
        self.assertIn("CREATE TABLE nyc_taxi.neighborhoods", result.stdout)

    def test_infer_schema_renders_explicit_database_as_create_schema_for_postgres(self) -> None:
        result = run_cli(
            "infer-schema",
            "--source",
            "postgres",
            "--target",
            "postgres",
            input_text="CREATE DATABASE analytics;",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertIn("CREATE SCHEMA IF NOT EXISTS analytics;", result.stdout)

    def test_infer_schema_merges_directory_inputs_and_writes_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sql_dir = root / "sql"
            sql_dir.mkdir()
            (sql_dir / "create.sql").write_text("CREATE TABLE users (id INT, name TEXT);", encoding="utf-8")
            (sql_dir / "query.sql").write_text("SELECT order_id, amount FROM orders;", encoding="utf-8")
            out_dir = root / "artifacts"
            result = run_cli(
                "infer-schema",
                "--source",
                "postgres",
                "--format",
                "json",
                "--in",
                str(sql_dir),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            schema_path = out_dir / "inferred_schema-postgres.json"
            self.assertTrue(schema_path.exists())
            payload = json.loads(schema_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["statement_count"], 2)
            tables = {table["qualified_name"]: table for table in payload["tables"]}
            self.assertIn("users", tables)
            self.assertIn("orders", tables)
            users = {column["name"]: column for column in tables["users"]["columns"]}
            orders = {column["name"]: column for column in tables["orders"]["columns"]}
            self.assertEqual(users["id"]["type"], "INT")
            self.assertEqual(users["name"]["type"], "TEXT")
            self.assertEqual(orders["order_id"]["type"], "BIGINT")
            self.assertEqual(orders["amount"]["type"], "TEXT")

    def test_infer_schema_writes_timestamped_log_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT pickup_time - request_time AS wait_time FROM trips;", encoding="utf-8")
            out_dir = root / "artifacts"
            result = run_cli(
                "infer-schema",
                "--source",
                "postgres",
                "--out",
                str(out_dir),
                "--log",
                "1",
                "--report",
                str(path),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            schema_path = out_dir / "inferred_schema-postgres.sql"
            self.assertTrue(schema_path.exists())
            logs = sorted(out_dir.glob("*-madsql-infer-schema.log"))
            reports = sorted(out_dir.glob("*-madsql-infer-schema-report.md"))
            self.assertEqual(len(logs), 1)
            self.assertEqual(len(reports), 1)
            log_payload = logs[0].read_text(encoding="utf-8")
            report_payload = reports[0].read_text(encoding="utf-8")
            self.assertIn("command: madsql infer-schema", log_payload)
            self.assertIn("command_type: infer-schema", log_payload)
            self.assertIn(f"madsql_version: {MADSQL_VERSION}", log_payload)
            self.assertIn("tables_inferred: 1", log_payload)
            self.assertIn("# madsql Infer Schema Report", report_payload)
            self.assertIn("- Command Type: `infer-schema`", report_payload)
            self.assertIn("- Output Format: `ddl`", report_payload)
            self.assertIn("## Version Information", report_payload)
            self.assertIn(f"- madsql: `{MADSQL_VERSION}`", report_payload)

    def test_infer_schema_directory_output_uses_source_and_target_in_filename(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT id FROM users;", encoding="utf-8")
            out_dir = root / "artifacts"
            result = run_cli(
                "infer-schema",
                "--source",
                "postgres",
                "--target",
                "mysql",
                "--out",
                str(out_dir),
                str(path),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            schema_path = out_dir / "inferred_schema-postgres-to-mysql.sql"
            self.assertTrue(schema_path.exists())

    def test_infer_schema_explicit_output_file_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT id FROM users;", encoding="utf-8")
            out_file = root / "schema.sql"
            result = run_cli(
                "infer-schema",
                "--source",
                "postgres",
                "--out",
                str(out_file),
                str(path),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(out_file.exists())

    def test_infer_schema_sorts_mixed_qualified_and_unqualified_tables(self) -> None:
        sql = """
SELECT id FROM users;
SELECT id FROM analytics.orders;
"""
        result = run_cli("infer-schema", "--source", "postgres", input_text=sql)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("CREATE TABLE users", result.stdout)
        self.assertIn("CREATE TABLE analytics.orders", result.stdout)

    def test_infer_schema_converts_token_errors_into_parse_errors(self) -> None:
        with patch.object(infer_schema_module, "parse", side_effect=TokenError("bad token")):
            result = infer_schema_module.infer_schema(
                "SELECT 1",
                source="postgres",
                path=Path("input.sql"),
                default_type="TEXT",
                unqualified_columns="first-table",
            )
        self.assertEqual(result.statement_count, 0)
        self.assertEqual(len(result.errors), 1)
        self.assertEqual(result.errors[0].error_type, "parse_error")
        self.assertIn("bad token", result.errors[0].message)

    def test_convert_can_write_inferred_schema_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text(
                "SELECT status, pickup_time - request_time AS trip_wait FROM trips WHERE status = 'completed';",
                encoding="utf-8",
            )
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                "--infer-schema",
                "--infer-schema-if-not-exists",
                str(path),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            schema_path = out_dir / "inferred_schema-postgres-to-mysql.sql"
            self.assertTrue(schema_path.exists())
            payload = schema_path.read_text(encoding="utf-8")
            self.assertIn("CREATE TABLE IF NOT EXISTS trips", payload)
            self.assertIn("pickup_time DOUBLE", payload)
            self.assertIn("request_time DOUBLE", payload)
            self.assertIn("status TEXT", payload)

    def test_convert_infer_schema_artifact_respects_pretty_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("CREATE TABLE users (id INT, name TEXT);", encoding="utf-8")
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "postgres",
                "--pretty",
                "--infer-schema",
                str(path),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            schema_path = out_dir / "inferred_schema-postgres.sql"
            self.assertIn("CREATE TABLE users (\n", schema_path.read_text(encoding="utf-8"))

    def test_convert_infer_schema_artifact_can_prepend_oracle_create_user_statements(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("USE nyc_taxi; CREATE TABLE neighborhoods (id INT);", encoding="utf-8")
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "singlestore",
                "--target",
                "oracle",
                "--infer-schema",
                "--infer-schema-create-user",
                "--infer-schema-create-user-password",
                "ChangeMe123",
                str(path),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            schema_path = out_dir / "inferred_schema-singlestore-to-oracle.sql"
            payload = schema_path.read_text(encoding="utf-8")
            self.assertIn('CREATE USER nyc_taxi IDENTIFIED BY "ChangeMe123";', payload)
            self.assertIn("ALTER SESSION SET CURRENT_SCHEMA = nyc_taxi;", payload)
            self.assertIn("CREATE TABLE nyc_taxi.neighborhoods", payload)

    def test_convert_infer_schema_artifact_can_prepend_create_schema_statements(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("USE nyc_taxi; CREATE TABLE neighborhoods (id INT);", encoding="utf-8")
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "singlestore",
                "--target",
                "postgres",
                "--infer-schema",
                "--infer-schema-create-schema",
                str(path),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            schema_path = out_dir / "inferred_schema-singlestore-to-postgres.sql"
            payload = schema_path.read_text(encoding="utf-8")
            self.assertIn("CREATE SCHEMA IF NOT EXISTS nyc_taxi;", payload)
            self.assertIn("CREATE TABLE nyc_taxi.neighborhoods", payload)

    def test_convert_infer_schema_artifact_uses_parent_when_output_is_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT user_id FROM events;", encoding="utf-8")
            out_file = root / "converted.sql"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                "--infer-schema",
                str(path),
                "--out",
                str(out_file),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            schema_path = root / "inferred_schema-postgres-to-mysql.sql"
            self.assertTrue(schema_path.exists())
            self.assertIn("user_id BIGINT", schema_path.read_text(encoding="utf-8"))

    def test_split_can_write_inferred_schema_artifact_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT id, polygon FROM neighborhoods;", encoding="utf-8")
            out_dir = root / "split"
            result = run_cli(
                "split-statements",
                "--source",
                "postgres",
                "--infer-schema",
                "--infer-schema-format",
                "json",
                "--out",
                str(out_dir),
                str(path),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            schema_path = out_dir / "inferred_schema-postgres.json"
            self.assertTrue(schema_path.exists())
            payload = json.loads(schema_path.read_text(encoding="utf-8"))
            tables = {table["qualified_name"]: table for table in payload["tables"]}
            self.assertIn("neighborhoods", tables)
            columns = {column["name"]: column for column in tables["neighborhoods"]["columns"]}
            self.assertEqual(columns["id"]["type"], "BIGINT")

    def test_glob_expansion_and_relative_structure_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            nested = root / "sql" / "nested"
            nested.mkdir(parents=True)
            (root / "sql" / "b.sql").write_text("SELECT 2;", encoding="utf-8")
            (nested / "a.sql").write_text("SELECT 1;", encoding="utf-8")
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                "sql/**/*.sql",
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((out_dir / "sql" / "b.mysql.sql").exists())
            self.assertTrue((out_dir / "sql" / "nested" / "a.mysql.sql").exists())

    def test_split_statements_uses_deterministic_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT 1; SELECT 2;", encoding="utf-8")
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "oracle",
                str(path),
                "--out",
                str(out_dir),
                "--split-statements",
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            split_dir = out_dir / "input"
            self.assertEqual(
                sorted(child.name for child in split_dir.iterdir()),
                ["0001_stmt.oracle.sql", "0002_stmt.oracle.sql"],
            )

    def test_convert_payload_keeps_successful_statements_when_one_statement_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_dir = root / "converted"
            mocked_result = ConversionResult(
                statements=[
                    RenderedStatement(statement_index=1, sql="SELECT 1"),
                    RenderedStatement(statement_index=3, sql="SELECT 3"),
                ],
                errors=[
                    ConversionError(
                        path=str(root / "input.sql"),
                        statement_index=2,
                        error_type="convert_error",
                        message="statement 2 failed",
                    )
                ],
            )
            with patch.object(cli_module, "convert_sql", return_value=mocked_result):
                wrote_output, errors, statement_count = cli_module._convert_single_payload(
                    sql="ignored",
                    path=root / "input.sql",
                    relative_path=Path("input.sql"),
                    source="postgres",
                    target="mysql",
                    pretty=False,
                    out_path=out_dir,
                    overwrite=False,
                    split_statements=False,
                    suffix=".sql",
                    write_stdout=False,
                )
            self.assertTrue(wrote_output)
            self.assertEqual(statement_count, 2)
            self.assertEqual(len(errors), 1)
            self.assertEqual((out_dir / "input.mysql.sql").read_text(encoding="utf-8"), "SELECT 1;\nSELECT 3\n")

    def test_convert_split_payload_preserves_original_statement_indexes_on_partial_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_dir = root / "converted"
            mocked_result = ConversionResult(
                statements=[
                    RenderedStatement(statement_index=1, sql="SELECT 1"),
                    RenderedStatement(statement_index=3, sql="SELECT 3"),
                ],
                errors=[
                    ConversionError(
                        path=str(root / "input.sql"),
                        statement_index=2,
                        error_type="convert_error",
                        message="statement 2 failed",
                    )
                ],
            )
            with patch.object(cli_module, "convert_sql", return_value=mocked_result):
                wrote_output, errors, statement_count = cli_module._convert_single_payload(
                    sql="ignored",
                    path=root / "input.sql",
                    relative_path=Path("input.sql"),
                    source="postgres",
                    target="mysql",
                    pretty=False,
                    out_path=out_dir,
                    overwrite=False,
                    split_statements=True,
                    suffix=".sql",
                    write_stdout=False,
                )
            self.assertTrue(wrote_output)
            self.assertEqual(statement_count, 2)
            self.assertEqual(len(errors), 1)
            split_dir = out_dir / "input"
            self.assertTrue((split_dir / "0001_stmt.mysql.sql").exists())
            self.assertFalse((split_dir / "0002_stmt.mysql.sql").exists())
            self.assertTrue((split_dir / "0003_stmt.mysql.sql").exists())

    def test_split_payload_keeps_successful_statements_when_one_statement_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_dir = root / "split"
            mocked_result = ConversionResult(
                statements=[
                    RenderedStatement(statement_index=1, sql="SELECT 1"),
                    RenderedStatement(statement_index=3, sql="SELECT 3"),
                ],
                errors=[
                    ConversionError(
                        path=str(root / "input.sql"),
                        statement_index=2,
                        error_type="split_error",
                        message="statement 2 failed",
                    )
                ],
            )
            with patch.object(cli_module, "split_sql", return_value=mocked_result):
                wrote_output, errors, statement_count = cli_module._split_single_payload(
                    sql="ignored",
                    path=root / "input.sql",
                    relative_path=Path("input.sql"),
                    source="postgres",
                    pretty=False,
                    out_path=out_dir,
                    overwrite=False,
                )
            self.assertTrue(wrote_output)
            self.assertEqual(statement_count, 2)
            self.assertEqual(len(errors), 1)
            split_dir = out_dir / "input"
            self.assertTrue((split_dir / "0001_stmt.sql").exists())
            self.assertFalse((split_dir / "0002_stmt.sql").exists())
            self.assertTrue((split_dir / "0003_stmt.sql").exists())

    def test_split_statements_subcommand_splits_single_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT 1; SELECT 2;", encoding="utf-8")
            out_dir = root / "split"
            result = run_cli(
                "split-statements",
                "--in",
                str(path),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            split_dir = out_dir / "input"
            self.assertEqual(
                sorted(child.name for child in split_dir.iterdir()),
                ["0001_stmt.sql", "0002_stmt.sql"],
            )

    def test_split_statements_ignores_empty_statements_created_by_extra_delimiters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text(";\nSELECT 1;", encoding="utf-8")
            out_dir = root / "split"
            result = run_cli(
                "split-statements",
                "--in",
                str(path),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            split_dir = out_dir / "input"
            self.assertEqual(
                sorted(child.name for child in split_dir.iterdir()),
                ["0001_stmt.sql"],
            )

    def test_split_statements_subcommand_splits_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sql_dir = root / "sql"
            nested = sql_dir / "nested"
            nested.mkdir(parents=True)
            (sql_dir / "a.sql").write_text("SELECT 1; SELECT 2;", encoding="utf-8")
            (nested / "b.sql").write_text("SELECT 3;", encoding="utf-8")
            out_dir = root / "split"
            result = run_cli(
                "split-statements",
                "--in",
                str(sql_dir),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((out_dir / "sql" / "a" / "0001_stmt.sql").exists())
            self.assertTrue((out_dir / "sql" / "a" / "0002_stmt.sql").exists())
            self.assertTrue((out_dir / "sql" / "nested" / "b" / "0001_stmt.sql").exists())

    def test_split_statements_directory_in_is_rooted_to_input_directory_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_dir = root / "examples" / "input" / "mssql"
            source_dir.mkdir(parents=True)
            (source_dir / "noGOData.sql").write_text("SELECT 1; SELECT 2;", encoding="utf-8")
            out_dir = root / "examples" / "output"
            result = run_cli(
                "split-statements",
                "--in",
                str(source_dir),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((out_dir / "mssql" / "noGOData" / "0001_stmt.sql").exists())
            self.assertTrue((out_dir / "mssql" / "noGOData" / "0002_stmt.sql").exists())
            self.assertFalse((out_dir / "examples").exists())

    def test_split_statements_subcommand_requires_out(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT 1;", encoding="utf-8")
            result = run_cli(
                "split-statements",
                "--in",
                str(path),
                cwd=root,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("--out", result.stderr)

    def test_split_statements_subcommand_accepts_optional_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT TOP 1 * FROM foo; SELECT TOP 2 * FROM bar;", encoding="utf-8")
            out_dir = root / "split"
            result = run_cli(
                "split-statements",
                "--source",
                "tsql",
                "--in",
                str(path),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            split_dir = out_dir / "input"
            self.assertEqual((split_dir / "0001_stmt.sql").read_text(encoding="utf-8"), "SELECT TOP 1 * FROM foo\n")

    def test_split_statements_subcommand_supports_pretty_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT a, b FROM t WHERE c = 1;", encoding="utf-8")
            out_dir = root / "split"
            result = run_cli(
                "split-statements",
                "--source",
                "postgres",
                "--pretty",
                "--in",
                str(path),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            payload = (out_dir / "input" / "0001_stmt.sql").read_text(encoding="utf-8")
            self.assertIn("\nFROM t\n", payload)
            self.assertTrue(payload.endswith("\n"))

    def test_split_statements_subcommand_supports_compact_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT a, b FROM t WHERE c = 1;", encoding="utf-8")
            out_dir = root / "split"
            result = run_cli(
                "split-statements",
                "--source",
                "postgres",
                "--compact",
                "--in",
                str(path),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            payload = (out_dir / "input" / "0001_stmt.sql").read_text(encoding="utf-8")
            self.assertNotIn("\nFROM t\n", payload)
            self.assertTrue(payload.endswith("\n"))

    def test_split_statements_subcommand_writes_error_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "broken.sql"
            path.write_text("SELECT FROM;", encoding="utf-8")
            out_dir = root / "split"
            error_path = root / "split-errors.json"
            result = run_cli(
                "split-statements",
                "--in",
                str(path),
                "--out",
                str(out_dir),
                "--errors",
                str(error_path),
                cwd=root,
            )
            self.assertEqual(result.returncode, 1)
            report_payload = read_error_report(error_path)
            payload = report_payload["errors"]
            self.assertEqual(report_payload["version_info"]["madsql"], MADSQL_VERSION)
            self.assertEqual(payload[0]["error_type"], "parse_error")
            self.assertTrue(payload[0]["path"].endswith("broken.sql"))

    def test_convert_writes_timestamped_log_with_debug_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "broken.sql"
            path.write_text("SELECT FROM;", encoding="utf-8")
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                "--in",
                str(path),
                "--out",
                str(root / "converted"),
                "--log",
                "1",
                cwd=root,
            )
            self.assertEqual(result.returncode, 1)
            logs = sorted((root / "converted").glob("*-madsql-convert.log"))
            self.assertEqual(len(logs), 1)
            payload = logs[0].read_text(encoding="utf-8")
            self.assertIn("command: madsql convert", payload)
            self.assertIn(f"madsql_version: {MADSQL_VERSION}", payload)
            self.assertIn("python_version: ", payload)
            self.assertIn("attempt_details:", payload)
            self.assertIn("parse_error", payload)

    def test_convert_continues_past_non_utf8_input_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sql_dir = root / "sql"
            sql_dir.mkdir()
            (sql_dir / "good.sql").write_text("SELECT 1;", encoding="utf-8")
            (sql_dir / "bad.sql").write_bytes(b"SELECT \x86 FROM dual;")
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "oracle",
                "--target",
                "oracle",
                "--input",
                str(sql_dir),
                "--out",
                str(out_dir),
                "--errors",
                "errors.json",
                "--log",
                "1",
                "--report",
                "--continue",
                cwd=root,
            )
            self.assertEqual(result.returncode, 1)
            self.assertNotIn("Traceback", result.stderr)
            self.assertIn("read_error", result.stderr)
            self.assertIn("debug_versions: madsql=", result.stderr)
            self.assertTrue((out_dir / "sql" / "good.oracle.sql").exists())
            report_payload = read_error_report(out_dir / "errors.json")
            error_payload = report_payload["errors"]
            self.assertEqual(error_payload[0]["error_type"], "read_error")
            self.assertTrue(error_payload[0]["path"].endswith("bad.sql"))
            logs = sorted(out_dir.glob("*-madsql-convert.log"))
            self.assertEqual(len(logs), 1)
            self.assertIn("read_error", logs[0].read_text(encoding="utf-8"))
            reports = sorted(out_dir.glob("*-madsql-convert-report.md"))
            self.assertEqual(len(reports), 1)

    def test_split_continues_past_non_utf8_input_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sql_dir = root / "sql"
            sql_dir.mkdir()
            (sql_dir / "good.sql").write_text("SELECT 1; SELECT 2;", encoding="utf-8")
            (sql_dir / "bad.sql").write_bytes(b"SELECT \x86 FROM dual;")
            out_dir = root / "split"
            result = run_cli(
                "split-statements",
                "--input",
                str(sql_dir),
                "--out",
                str(out_dir),
                "--errors",
                "errors.json",
                "--log",
                "1",
                "--report",
                "--continue",
                cwd=root,
            )
            self.assertEqual(result.returncode, 1)
            self.assertNotIn("Traceback", result.stderr)
            self.assertIn("read_error", result.stderr)
            self.assertIn("debug_versions: madsql=", result.stderr)
            self.assertTrue((out_dir / "sql" / "good" / "0001_stmt.sql").exists())
            self.assertTrue((out_dir / "sql" / "good" / "0002_stmt.sql").exists())
            report_payload = read_error_report(out_dir / "errors.json")
            error_payload = report_payload["errors"]
            self.assertEqual(error_payload[0]["error_type"], "read_error")
            self.assertTrue(error_payload[0]["path"].endswith("bad.sql"))
            logs = sorted(out_dir.glob("*-madsql-split-statements.log"))
            self.assertEqual(len(logs), 1)
            self.assertIn("read_error", logs[0].read_text(encoding="utf-8"))
            reports = sorted(out_dir.glob("*-madsql-split-statements-report.md"))
            self.assertEqual(len(reports), 1)

    def test_convert_log_and_report_use_parent_directory_when_output_is_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT 1;", encoding="utf-8")
            out_file = root / "converted.sql"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                str(path),
                "--out",
                str(out_file),
                "--log",
                "0",
                "--report",
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            logs = sorted(root.glob("*-madsql-convert.log"))
            reports = sorted(root.glob("*-madsql-convert-report.md"))
            self.assertEqual(len(logs), 1)
            self.assertEqual(len(reports), 1)
            self.assertFalse(any(root.glob("*-converted.report.md")))

    def test_split_statements_writes_timestamped_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT 1; SELECT 2;", encoding="utf-8")
            result = run_cli(
                "split-statements",
                "--in",
                str(path),
                "--out",
                str(root / "split"),
                "--report",
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            reports = sorted((root / "split").glob("*-madsql-split-statements-report.md"))
            self.assertEqual(len(reports), 1)
            payload = reports[0].read_text(encoding="utf-8")
            self.assertIn("# madsql Run Report", payload)
            self.assertIn("- Command Type: `split-statements`", payload)
            self.assertIn("- Source Dialect: `n/a`", payload)
            self.assertIn("- Success Rate: `1/1 (100.0%)`", payload)
            self.assertIn("## Version Information", payload)
            self.assertIn(f"- madsql: `{MADSQL_VERSION}`", payload)

    def test_split_statements_falls_back_to_sqlparse_and_reports_engine_usage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text(
                "CREATE TABLE t (id BIGINT, PRIMARY KEY((id) HASH)); SELECT 2;",
                encoding="utf-8",
            )
            result = run_cli(
                "split-statements",
                "--in",
                str(path),
                "--out",
                str(root / "split"),
                "--report",
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            split_dir = root / "split" / "input"
            self.assertTrue((split_dir / "0001_stmt.sql").exists())
            self.assertTrue((split_dir / "0002_stmt.sql").exists())
            reports = sorted((root / "split").glob("*-madsql-split-statements-report.md"))
            self.assertEqual(len(reports), 1)
            payload = reports[0].read_text(encoding="utf-8")
            self.assertIn("- Fallback Inputs (sqlparse): `1`", payload)
            self.assertIn("## Split Engine Usage", payload)
            self.assertIn("| `sqlparse` | 1 |", payload)

    def test_split_statements_preserves_semicolons_inside_literals_and_comments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT 'a; b'; SELECT 2 /* ; */;", encoding="utf-8")
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                str(path),
                "--out",
                str(out_dir),
                "--split-statements",
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            split_dir = out_dir / "input"
            self.assertEqual((split_dir / "0001_stmt.mysql.sql").read_text(encoding="utf-8"), "SELECT 'a; b'\n")
            self.assertEqual((split_dir / "0002_stmt.mysql.sql").read_text(encoding="utf-8"), "SELECT 2 /* ; */\n")

    def test_overwrite_protection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text("SELECT 1;", encoding="utf-8")
            out_dir = root / "converted"
            out_dir.mkdir()
            output = out_dir / "input.mysql.sql"
            output.write_text("existing\n", encoding="utf-8")
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                str(path),
                "--out",
                str(out_dir),
                cwd=root,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("Refusing to overwrite", result.stderr)

    def test_missing_input_is_fatal_cli_error(self) -> None:
        result = run_cli(
            "convert",
            "--source",
            "postgres",
            "--target",
            "mysql",
            "missing.sql",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("Input path not found", result.stderr)

    def test_error_report_contains_path_and_statement_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "broken.sql"
            path.write_text("SELECT FROM;", encoding="utf-8")
            error_path = root / "errors.json"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                str(path),
                "--errors",
                str(error_path),
                cwd=root,
            )
            self.assertEqual(result.returncode, 1)
            report_payload = read_error_report(error_path)
            payload = report_payload["errors"]
            self.assertEqual(report_payload["version_info"]["madsql"], MADSQL_VERSION)
            self.assertEqual(report_payload["version_info"]["sqlglot"], SQLGLOT_VERSION)
            self.assertEqual(payload[0]["statement_index"], 1)
            self.assertTrue(payload[0]["path"].endswith("broken.sql"))

    def test_relative_errors_path_writes_to_output_base_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "broken.sql"
            path.write_text("SELECT FROM;", encoding="utf-8")
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                str(path),
                "--out",
                str(out_dir),
                "--errors",
                "errors.json",
                cwd=root,
            )
            self.assertEqual(result.returncode, 1)
            self.assertTrue((out_dir / "errors.json").exists())
            self.assertFalse((root / "errors.json").exists())

    def test_relative_errors_path_uses_parent_when_output_is_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "broken.sql"
            path.write_text("SELECT FROM;", encoding="utf-8")
            out_file = root / "converted.sql"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                str(path),
                "--out",
                str(out_file),
                "--errors",
                "errors.json",
                cwd=root,
            )
            self.assertEqual(result.returncode, 1)
            self.assertTrue((root / "errors.json").exists())

    def test_report_requires_out(self) -> None:
        result = run_cli(
            "convert",
            "--source",
            "postgres",
            "--target",
            "mysql",
            "--report",
            input_text="SELECT 1;",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--report requires --out", result.stderr)

    def test_report_writes_statement_type_counts_and_percentage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text(
                "SELECT 1; INSERT INTO t VALUES (1); UPDATE t SET a = 1; DELETE FROM t; "
                "CREATE TABLE x(a INT); CREATE INDEX idx_x_a ON x(a);",
                encoding="utf-8",
            )
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                str(path),
                "--out",
                str(out_dir),
                "--report",
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            reports = sorted(out_dir.glob("*-madsql-convert-report.md"))
            self.assertEqual(len(reports), 1)
            report_path = reports[0]
            report = report_path.read_text(encoding="utf-8")
            self.assertIn("- Conversion Rate: `6/6 (100.0%)`", report)
            self.assertIn("| `SELECT` | 1 | 1 | 0 |", report)
            self.assertIn("| `INSERT` | 1 | 1 | 0 |", report)
            self.assertIn("| `UPDATE` | 1 | 1 | 0 |", report)
            self.assertIn("| `DELETE` | 1 | 1 | 0 |", report)
            self.assertIn("| `CREATE TABLE` | 1 | 1 | 0 |", report)
            self.assertIn("| `CREATE INDEX` | 1 | 1 | 0 |", report)

    def test_fail_fast_stops_after_first_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a.sql").write_text("SELECT FROM;", encoding="utf-8")
            (root / "b.sql").write_text("SELECT 2;", encoding="utf-8")
            out_dir = root / "converted"
            error_path = root / "errors.json"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                "a.sql",
                "b.sql",
                "--out",
                str(out_dir),
                "--errors",
                str(error_path),
                "--fail-fast",
                cwd=root,
            )
            self.assertEqual(result.returncode, 1)
            self.assertFalse((out_dir / "b.mysql.sql").exists())
            report_payload = read_error_report(error_path)
            payload = report_payload["errors"]
            self.assertEqual(len(payload), 1)
            self.assertTrue(payload[0]["path"].endswith("a.sql"))

    def test_convert_ignore_errors_returns_zero_and_preserves_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a.sql").write_text("SELECT FROM;", encoding="utf-8")
            (root / "b.sql").write_text("SELECT 2;", encoding="utf-8")
            out_dir = root / "converted"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                "a.sql",
                "b.sql",
                "--out",
                str(out_dir),
                "--errors",
                "errors.json",
                "--log",
                "1",
                "--report",
                "--ignore-errors",
                cwd=root,
            )
            self.assertEqual(result.returncode, 0)
            self.assertIn("parse_error", result.stderr)
            self.assertTrue((out_dir / "b.mysql.sql").exists())
            error_report = read_error_report(out_dir / "errors.json")
            self.assertEqual(error_report["version_info"]["madsql"], MADSQL_VERSION)
            self.assertEqual(error_report["errors"][0]["error_type"], "parse_error")
            log_payload = next(out_dir.glob("*-madsql-convert.log")).read_text(encoding="utf-8")
            self.assertIn("parse_error", log_payload)
            report_payload = next(out_dir.glob("*-madsql-convert-report.md")).read_text(encoding="utf-8")
            self.assertIn("## Error Type Counts", report_payload)

    def test_split_ignore_errors_returns_zero_and_preserves_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a.sql").write_text("SELECT FROM;", encoding="utf-8")
            (root / "b.sql").write_text("SELECT 1; SELECT 2;", encoding="utf-8")
            out_dir = root / "split"
            result = run_cli(
                "split-statements",
                "a.sql",
                "b.sql",
                "--out",
                str(out_dir),
                "--errors",
                "errors.json",
                "--log",
                "1",
                "--report",
                "--ignore-errors",
                cwd=root,
            )
            self.assertEqual(result.returncode, 0)
            self.assertIn("parse_error", result.stderr)
            self.assertTrue((out_dir / "b" / "0001_stmt.sql").exists())
            self.assertTrue((out_dir / "b" / "0002_stmt.sql").exists())
            error_report = read_error_report(out_dir / "errors.json")
            self.assertEqual(error_report["errors"][0]["error_type"], "parse_error")
            log_payload = next(out_dir.glob("*-madsql-split-statements.log")).read_text(encoding="utf-8")
            self.assertIn("parse_error", log_payload)
            report_payload = next(out_dir.glob("*-madsql-split-statements-report.md")).read_text(encoding="utf-8")
            self.assertIn("## Error Type Counts", report_payload)

    def test_infer_schema_ignore_errors_returns_zero_and_preserves_diagnostics(self) -> None:
        sql = """
SELECT id FROM users;
WITH broken AS (
    SELECT FROM
)
SELECT * FROM broken;
"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "input.sql"
            path.write_text(sql, encoding="utf-8")
            out_dir = root / "artifacts"
            result = run_cli(
                "infer-schema",
                "--source",
                "postgres",
                "--input",
                str(path),
                "--out",
                str(out_dir),
                "--errors",
                "errors.json",
                "--log",
                "1",
                "--report",
                "--ignore-errors",
                cwd=root,
            )
            self.assertEqual(result.returncode, 0)
            self.assertIn("parse_error", result.stderr)
            schema_payload = (out_dir / "inferred_schema-postgres.sql").read_text(encoding="utf-8")
            self.assertIn("CREATE TABLE users", schema_payload)
            error_report = read_error_report(out_dir / "errors.json")
            self.assertEqual(error_report["errors"][0]["error_type"], "parse_error")
            self.assertIn("WITH broken AS", error_report["errors"][0]["statement_sql"])
            log_payload = next(out_dir.glob("*-madsql-infer-schema.log")).read_text(encoding="utf-8")
            self.assertIn("parse_error", log_payload)
            report_payload = next(out_dir.glob("*-madsql-infer-schema-report.md")).read_text(encoding="utf-8")
            self.assertIn("## Error Details", report_payload)

    def test_external_inputs_preserve_deterministic_absolute_structure(self) -> None:
        with tempfile.TemporaryDirectory() as workspace_tmp, tempfile.TemporaryDirectory() as external_tmp:
            workspace = Path(workspace_tmp)
            external_root = Path(external_tmp)
            nested = external_root / "vendor" / "dialects"
            nested.mkdir(parents=True)
            external_file = nested / "sample.sql"
            external_file.write_text("SELECT 1;", encoding="utf-8")
            out_dir = workspace / "converted"
            result = run_cli(
                "convert",
                "--source",
                "postgres",
                "--target",
                "mysql",
                str(external_file),
                "--out",
                str(out_dir),
                cwd=workspace,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            expected = out_dir / "_external" / Path(*external_file.resolve().parts[1:]).with_name("sample.mysql.sql")
            self.assertTrue(expected.exists(), expected)


if __name__ == "__main__":
    unittest.main()
