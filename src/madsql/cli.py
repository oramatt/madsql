from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
import json
import platform
import sys
from pathlib import Path

import sqlparse
from sqlglot import Dialect, __version__ as SQLGLOT_VERSION

from madsql import __version__ as MADSQL_VERSION
from madsql.convert import convert_sql, split_sql
from madsql.errors import ConversionError
from madsql.infer_schema import infer_schema_many, render_schema_ddl, render_schema_json
from madsql.io import InputFile, ensure_out_path, expand_inputs, read_utf8, write_text

COMMON_DEFAULT_TYPE_VALUES = (
    "TEXT, VARCHAR(255), VARCHAR2(100), NUMBER, NUMBER(12), NUMBER(10,2), "
    "DOUBLE, BIGINT, DATE, TIMESTAMP, CLOB, CHAR(1), NCHAR(10), BLOB, GEOGRAPHY"
)

MAIN_HELP_TEXT = """madsql converts SQL between SQLGlot-supported dialects.

Start here:
  madsql dialects
      List supported source and target dialect names.

  madsql convert --help
      Show examples for stdin, files, directories, glob input, output
      writing, splitting, logging, reports, and error handling.

  madsql split-statements --help
      Show split-only examples for turning SQL files into one file per
      detected statement without dialect conversion.

  madsql infer-schema --help
      Show schema inference examples for stdin, files, directories, JSON,
      and creation DDL output.

Quick examples:
  madsql dialects
  python3 -m madsql dialects
  echo "SELECT TOP 3 [name] FROM dbo.users;" | madsql convert --source tsql --target postgres
  madsql convert --source postgres --target mysql --in ./sql --out ./converted
  madsql split-statements --in ./sql --out ./split
  madsql infer-schema --source singlestore ./queries.sql
"""

CONVERT_HELP_TEXT = """Convert SQL from one dialect to another.

Input rules:
  - Use positional inputs for files, directories, or glob patterns.
  - Use --in or --input for explicit file, directory, or glob inputs.
  - If you provide no input paths, convert reads from stdin.
  - If more than one file is resolved, --out or --output is required.

Output rules:
  - Without --out and with a single input, converted SQL is written to stdout.
  - With --out, converted files are written as UTF-8 under the output path.
  - Existing output files are protected unless you add --overwrite.
  - --split-statements writes one file per parsed statement and requires --out.
  - --infer-schema writes deterministic inferred schema artifacts such as
    inferred_schema-postgres-to-mysql.sql beside the normal outputs and
    requires --out.
  - Compact SQL output is the default.
  - Use --pretty for multiline formatting or --compact to be explicit.

Run control:
  - The tool continues through errors by default.
  - Use --continue when you want that behavior to be explicit in scripts.
  - Use --fail-fast to stop on the first error.
  - Use --ignore-errors to return exit code 0 even when errors are recorded.
    Errors are still written to stderr, --errors, --log, and --report.
  - --continue and --fail-fast cannot be used together.

Observability:
  - --errors writes a JSON error report.
  - --log 0 writes a timestamped log with the user command and summary stats.
  - --log 1 adds per-attempt debugging details and error explanations.
  - --report writes a timestamped markdown report for the run.

Schema artifacts:
  - --infer-schema writes deterministic inferred schema artifacts beside
    normal convert outputs.
  - Use --infer-schema-format, --infer-schema-default-type,
    --infer-schema-unqualified-columns, --infer-schema-if-not-exists,
    --infer-schema-create-schema,
    --infer-schema-create-user, and --infer-schema-create-user-password
    to control the side artifact.

Examples:
  Read from stdin and print to stdout:
    echo "SELECT `name` FROM users;" | madsql convert --source mysql --target postgres

  Convert one file and print to stdout:
    madsql convert --source postgres --target mysql ./input.sql

  Convert one file with explicit --in and write to an output directory:
    madsql convert --source postgres --target mysql --in ./input.sql --out ./converted

  Convert a directory tree and preserve relative paths:
    madsql convert --source postgres --target mysql --in ./sql --out ./converted

  Convert matching files from a glob:
    madsql convert --source tsql --target postgres "./sql/**/*.sql" --out ./converted

  Split multi-statement SQL into one file per statement:
    madsql convert --source postgres --target oracle --in ./input.sql --out ./converted --split-statements

  Pretty-print converted SQL and change the output suffix:
    madsql convert --source postgres --target mysql ./input.sql --pretty --suffix .converted.sql

  Continue through errors and write a detailed log and report:
    madsql convert --source postgres --target mysql --in ./sql --out ./converted --continue --log 1 --report

  Convert files and also emit inferred table DDL:
    madsql convert --source postgres --target mysql --in ./sql --out ./converted --infer-schema

Notes:
  - Use madsql dialects to see valid dialect names.
  - Split output files use deterministic names like 0001_stmt.<target>.sql.
  - Fatal CLI misuse exits with code 2. Statement conversion errors exit with code 1.
"""

SPLIT_HELP_TEXT = """Split SQL into one output file per detected statement.

Input rules:
  - Use positional inputs for files, directories, or glob patterns.
  - Use --in or --input for explicit file, directory, or glob inputs.
  - If you provide no input paths, split-statements reads from stdin.
  - --out or --output is required because the command writes one file per statement.

Parsing rules:
  - --source is optional.
  - Add --source when the input uses dialect-specific syntax.
  - If SQLGlot cannot parse a split-only input, madsql can fall back to sqlparse
    boundary detection for multi-statement inputs and report that engine usage.
  - Compact split output is the default when SQLGlot renders statements.
  - Use --pretty for multiline formatting or --compact to be explicit.

Run control:
  - The tool continues through errors by default.
  - Use --continue when you want that behavior to be explicit in scripts.
  - Use --fail-fast to stop on the first split error.
  - Use --ignore-errors to return exit code 0 even when errors are recorded.
    Errors are still written to stderr, --errors, --log, and --report.
  - --continue and --fail-fast cannot be used together.

Schema inference:
  - --infer-schema writes deterministic inferred schema artifacts at the
    top level of the output directory.
  - Use --infer-schema-format, --infer-schema-default-type,
    --infer-schema-unqualified-columns, --infer-schema-if-not-exists,
    --infer-schema-create-schema,
    --infer-schema-create-user, and --infer-schema-create-user-password
    to control the side artifact.
  - Schema inference uses SQLGlot parsing and may report additional parse errors
    if split fallback succeeded but schema inference could not parse an input.

Observability:
  - --errors writes a JSON error report.
  - --log 0 writes a timestamped log with the user command and summary stats.
  - --log 1 adds per-attempt debugging details and error explanations.
  - --report writes a timestamped markdown report for the run.

Examples:
  Split one file into per-statement output files:
    madsql split-statements --in ./input.sql --out ./split

  Split a directory tree and preserve relative paths:
    madsql split-statements --in ./sql --out ./split

  Split files matched by a glob:
    madsql split-statements "./sql/**/*.sql" --out ./split

  Split dialect-specific SQL with an explicit source dialect:
    madsql split-statements --source tsql --in ./queries.sql --out ./split

  Pretty-print split output files when SQLGlot handles rendering:
    madsql split-statements --source postgres --in ./queries.sql --out ./split --pretty

  Continue through errors and write a detailed log and report:
    madsql split-statements --in ./sql --out ./split --continue --log 1 --report

  Split files and also emit inferred table DDL:
    madsql split-statements --source postgres --in ./sql --out ./split --infer-schema

Notes:
  - Output files use deterministic names like 0001_stmt.sql.
  - Fatal CLI misuse exits with code 2. Statement split errors exit with code 1.
"""

INFER_HELP_TEXT = """Infer table schemas from SQL and emit creation DDL or JSON.

Input rules:
  - Use positional inputs for files, directories, or glob patterns.
  - Use --in or --input for explicit file, directory, or glob inputs.
  - If you provide no input paths, infer-schema reads from stdin.
  - Multiple inputs are merged into one inferred schema artifact.

Inference rules:
  - Supported statement classes: USE, SELECT, DELETE, INSERT, UPDATE,
    CREATE SCHEMA, CREATE DATABASE, CREATE TABLE, CREATE VIEW,
    CREATE MATERIALIZED VIEW, and CREATE INDEX.
  - CREATE SCHEMA and CREATE DATABASE register explicit schema names.
  - CREATE TABLE contributes explicit column types.
  - INSERT INTO table(col1, col2, ...) contributes table/column membership.
  - SELECT/UPDATE statements contribute referenced tables and columns.
  - CREATE VIEW and CREATE MATERIALIZED VIEW contribute referenced base
    tables and columns from the view query.
  - CREATE INDEX contributes the indexed table and indexed columns.
  - Unqualified columns in multi-table queries can be skipped or assigned to
    the first table in scope with low confidence.
  - Other parsed statement types are reported as unsupported and skipped.

Run control:
  - infer-schema continues through unsupported and parse errors by default.
  - Use --continue when you want that behavior to be explicit in scripts.
  - Use --fail-fast to stop on the first parse or unsupported statement error.
  - Use --ignore-errors to return exit code 0 even when errors are recorded.
    Errors are still written to stderr, --errors, --log, and --report.
  - --continue and --fail-fast cannot be used together.

Output rules:
  - Without --out, inferred schema is written to stdout.
  - With --out to a file, the inferred schema is written to that file.
  - With --out to a directory, madsql writes a deterministic schema artifact
    such as inferred_schema-postgres.sql or
    inferred_schema-postgres-to-mysql.sql.
  - DDL output is compact by default.
  - Use --pretty for multiline DDL or --compact to be explicit.
  - --pretty and --compact are ignored when --format json is used.
  - Input CREATE SCHEMA / CREATE DATABASE statements render namespace DDL
    directly. With --target oracle they render as CREATE USER statements and
    require --create-user-password.
  - --create-schema prepends inferred CREATE SCHEMA statements for non-Oracle
    DDL targets.
  - --create-user prepends Oracle CREATE USER and GRANT statements for inferred
    schema names and requires --target oracle, DDL output, and
    --create-user-password.

Observability:
  - --errors writes a JSON error report.
  - --log 0 writes a timestamped log with summary stats.
  - --log 1 adds error details, including unsupported statement SQL text.
  - --report writes a timestamped markdown report for the run, including
    unsupported statement details and SQL text.
  - --log and --report require --out.

Default type:
  - --default-type must be a SQLGlot-parsable data type.
  - Common supported values: {common_default_type_values}.

Examples:
  Infer DDL from stdin:
    cat queries.sql | madsql infer-schema --source singlestore

  Infer DDL from one file and write to stdout:
    madsql infer-schema --source postgres ./queries.sql

  Merge a directory of SQL files and write DDL to a file:
    madsql infer-schema --source mysql --in ./sql --out ./artifacts/schema.sql

  Emit structured JSON instead of DDL:
    madsql infer-schema --source singlestore --format json ./queries.sql

  Infer schema and write a log and markdown report:
    madsql infer-schema --source postgres --in ./sql --out ./artifacts/schema.sql --log 1 --report

  Prepend CREATE SCHEMA statements for inferred schema names:
    madsql infer-schema --source postgres --target postgres --create-schema ./queries.sql

  Prepend Oracle CREATE USER statements for inferred schema names:
    madsql infer-schema --source singlestore --target oracle --create-user --create-user-password ChangeMe123 ./queries.sql

Notes:
  - Query-only inputs infer table structure heuristically.
  - Low-confidence columns are noted in DDL comments and JSON output.
  - Fatal CLI misuse exits with code 2. Parse/read errors exit with code 1.
""".format(common_default_type_values=COMMON_DEFAULT_TYPE_VALUES)


class FatalCliError(Exception):
    """Raised for invalid usage or output contract violations."""


def _add_render_style_arguments(
    parser: argparse.ArgumentParser,
    *,
    pretty_help: str,
    compact_help: str,
) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--pretty", dest="pretty", action="store_true", help=pretty_help)
    group.add_argument("--compact", dest="pretty", action="store_false", help=compact_help)
    parser.set_defaults(pretty=False)


@dataclass(frozen=True)
class PayloadStats:
    output_written: bool
    output_file_count: int
    statement_count: int
    converted_statement_count: int
    statement_type_counts: dict[str, int]
    converted_statement_type_counts: dict[str, int]
    engine_used: str = "sqlglot"
    fallback_used: bool = False


@dataclass(frozen=True)
class AttemptRecord:
    path: str
    status: str
    statement_count: int
    converted_statement_count: int
    error_count: int
    errors: list[ConversionError]


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        args.invoked_command = _command_text(argv)
        return args.func(args)
    except FatalCliError as exc:
        sys.stderr.write(f"{exc}\n")
        return 2
    except BrokenPipeError:
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="madsql",
        description=MAIN_HELP_TEXT,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version",
        action="version",
        version=_version_text(),
        help="Show version and runtime dependency information",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    dialects_parser = subparsers.add_parser(
        "dialects",
        help="List supported dialects",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="List supported SQLGlot dialect names for use with --source and --target.\n\nExamples:\n  madsql dialects\n  python3 -m madsql dialects",
    )
    dialects_parser.set_defaults(func=run_dialects)

    convert_parser = subparsers.add_parser(
        "convert",
        help="Convert SQL between dialects",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=CONVERT_HELP_TEXT,
    )
    convert_parser.add_argument("inputs", nargs="*", help="Input files, directories, or glob patterns")
    convert_parser.add_argument(
        "--in",
        "--input",
        dest="flag_inputs",
        action="append",
        default=[],
        help="Input file, directory, or glob pattern (repeatable)",
    )
    convert_parser.add_argument("--source", required=True, help="Source SQL dialect")
    convert_parser.add_argument("--target", required=True, help="Target SQL dialect")
    convert_parser.add_argument("--out", "--output", dest="out", help="Output file or directory")
    convert_parser.add_argument("--split-statements", action="store_true", help="Write one file per statement")
    convert_parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    _add_render_style_arguments(
        convert_parser,
        pretty_help="Pretty-print output SQL",
        compact_help="Emit compact output SQL (default)",
    )
    convert_parser.add_argument("--continue", dest="continue_on_error", action="store_true", help="Continue processing remaining inputs after errors")
    convert_parser.add_argument("--fail-fast", action="store_true", help="Stop on first conversion error")
    convert_parser.add_argument(
        "--ignore-errors",
        action="store_true",
        help="Return exit code 0 even when errors are recorded",
    )
    convert_parser.add_argument("--errors", help="Write structured JSON error report")
    convert_parser.add_argument("--log", type=int, choices=[0, 1], help="Write a timestamped log with verbosity level 0 or 1")
    convert_parser.add_argument("--report", action="store_true", help="Write a markdown conversion report")
    convert_parser.add_argument(
        "--suffix",
        default=".sql",
        help="Output file suffix to append after the target dialect name",
    )
    _add_infer_schema_artifact_arguments(convert_parser)
    convert_parser.set_defaults(func=run_convert)

    split_parser = subparsers.add_parser(
        "split-statements",
        help="Split SQL into one output file per statement",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=SPLIT_HELP_TEXT,
    )
    split_parser.add_argument("inputs", nargs="*", help="Input files, directories, or glob patterns")
    split_parser.add_argument(
        "--in",
        "--input",
        dest="flag_inputs",
        action="append",
        default=[],
        help="Input file, directory, or glob pattern (repeatable)",
    )
    split_parser.add_argument("--source", help="Optional source SQL dialect")
    split_parser.add_argument("--out", "--output", dest="out", required=True, help="Output directory")
    split_parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    _add_render_style_arguments(
        split_parser,
        pretty_help="Pretty-print split SQL when SQLGlot renders the statement",
        compact_help="Emit compact split SQL when SQLGlot renders the statement (default)",
    )
    split_parser.add_argument("--continue", dest="continue_on_error", action="store_true", help="Continue processing remaining inputs after errors")
    split_parser.add_argument("--fail-fast", action="store_true", help="Stop on first split error")
    split_parser.add_argument(
        "--ignore-errors",
        action="store_true",
        help="Return exit code 0 even when errors are recorded",
    )
    split_parser.add_argument("--errors", help="Write structured JSON error report")
    split_parser.add_argument("--log", type=int, choices=[0, 1], help="Write a timestamped log with verbosity level 0 or 1")
    split_parser.add_argument("--report", action="store_true", help="Write a markdown split report")
    _add_infer_schema_artifact_arguments(split_parser)
    split_parser.set_defaults(func=run_split_statements)

    infer_parser = subparsers.add_parser(
        "infer-schema",
        help="Infer table schemas and emit DDL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=INFER_HELP_TEXT,
    )
    infer_parser.add_argument("inputs", nargs="*", help="Input files, directories, or glob patterns")
    infer_parser.add_argument(
        "--in",
        "--input",
        dest="flag_inputs",
        action="append",
        default=[],
        help="Input file, directory, or glob pattern (repeatable)",
    )
    infer_parser.add_argument("--source", help="Optional source SQL dialect for parsing")
    infer_parser.add_argument("--target", help="Optional dialect for emitted DDL")
    infer_parser.add_argument("--out", "--output", dest="out", help="Output file or directory")
    infer_parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output")
    infer_parser.add_argument("--errors", help="Write structured JSON error report")
    infer_parser.add_argument("--log", type=int, choices=[0, 1], help="Write a timestamped log with verbosity level 0 or 1")
    infer_parser.add_argument("--report", action="store_true", help="Write a markdown infer-schema report")
    _add_render_style_arguments(
        infer_parser,
        pretty_help="Pretty-print inferred DDL output",
        compact_help="Emit compact inferred DDL output (default; ignored for JSON output)",
    )
    infer_parser.add_argument("--format", choices=["ddl", "json"], default="ddl", help="Output format")
    infer_parser.add_argument(
        "--default-type",
        default="TEXT",
        help=(
            "Fallback SQL type for inferred columns without stronger type evidence. "
            f"Common values: {COMMON_DEFAULT_TYPE_VALUES}"
        ),
    )
    infer_parser.add_argument(
        "--unqualified-columns",
        choices=["first-table", "skip"],
        default="first-table",
        help="How to handle unqualified columns in multi-table queries",
    )
    infer_parser.add_argument(
        "--if-not-exists",
        action="store_true",
        help="Emit CREATE TABLE IF NOT EXISTS in DDL output",
    )
    infer_parser.add_argument(
        "--create-schema",
        action="store_true",
        help="For non-Oracle DDL output, prepend CREATE SCHEMA statements for inferred schema names",
    )
    infer_parser.add_argument(
        "--create-user",
        action="store_true",
        help="For Oracle DDL output, prepend CREATE USER and GRANT statements for inferred schema names",
    )
    infer_parser.add_argument(
        "--create-user-password",
        help="Quoted Oracle password used by --create-user and Oracle rendering of input CREATE SCHEMA/CREATE DATABASE statements",
    )
    infer_parser.add_argument("--continue", dest="continue_on_error", action="store_true", help="Continue processing remaining inputs after errors")
    infer_parser.add_argument("--fail-fast", action="store_true", help="Stop on first parse or read error")
    infer_parser.add_argument(
        "--ignore-errors",
        action="store_true",
        help="Return exit code 0 even when errors are recorded",
    )
    infer_parser.set_defaults(func=run_infer_schema, command_parser=infer_parser)

    return parser


def run_dialects(_: argparse.Namespace) -> int:
    names = sorted(name for name in Dialect.classes if name)
    sys.stdout.write("\n".join(names) + "\n")
    return 0


def run_convert(args: argparse.Namespace) -> int:
    _validate_error_strategy(args)
    _validate_dialect(args.source, "source")
    _validate_dialect(args.target, "target")
    _validate_create_user_options(
        target=args.target,
        output_format=args.infer_schema_format,
        create_user=args.infer_schema_create_user,
        create_user_password=args.infer_schema_create_user_password,
        create_user_flag="--infer-schema-create-user",
        password_flag="--infer-schema-create-user-password",
    )
    _validate_create_schema_options(
        target=args.target,
        output_format=args.infer_schema_format,
        create_schema=args.infer_schema_create_schema,
        create_schema_flag="--infer-schema-create-schema",
    )

    out_path = ensure_out_path(args.out)
    if args.split_statements and out_path is None:
        raise FatalCliError("--split-statements requires --out")
    if args.infer_schema and out_path is None:
        raise FatalCliError("--infer-schema requires --out")
    if args.report and out_path is None:
        raise FatalCliError("--report requires --out")
    if args.log is not None and out_path is None:
        raise FatalCliError("--log requires --out")

    input_files: list[InputFile] = []
    stdin_sql: str | None = None
    raw_inputs = [*args.inputs, *args.flag_inputs]
    if raw_inputs:
        try:
            input_files = expand_inputs(raw_inputs)
        except FileNotFoundError as exc:
            raise FatalCliError(str(exc)) from exc
        if len(input_files) > 1 and out_path is None:
            raise FatalCliError("--out is required when converting multiple input files")
    else:
        stdin_sql = sys.stdin.read()

    all_errors: list[ConversionError] = []
    output_written = False
    files_processed = 0
    outputs_written = 0
    total_statement_count = 0
    converted_statement_count = 0
    statement_type_counts: Counter[str] = Counter()
    converted_statement_type_counts: Counter[str] = Counter()
    attempts: list[AttemptRecord] = []
    stop_after_current = False
    schema_payloads: list[tuple[str, Path | None]] = []

    if stdin_sql is not None:
        schema_payloads.append((stdin_sql, None))
        payload_stats, errors = _convert_single_payload_stats(
            sql=stdin_sql,
            path=None,
            relative_path=None,
            source=args.source,
            target=args.target,
            pretty=args.pretty,
            out_path=out_path,
            overwrite=args.overwrite,
            split_statements=args.split_statements,
            suffix=args.suffix,
            write_stdout=out_path is None,
        )
        files_processed += 1
        output_written = payload_stats.output_written
        outputs_written += payload_stats.output_file_count
        total_statement_count += payload_stats.statement_count
        converted_statement_count += payload_stats.converted_statement_count
        statement_type_counts.update(payload_stats.statement_type_counts)
        converted_statement_type_counts.update(payload_stats.converted_statement_type_counts)
        all_errors.extend(errors)
        attempts.append(_attempt_record(path=None, payload_stats=payload_stats, errors=errors))
        if args.fail_fast and errors:
            stop_after_current = True
    else:
        for input_file in input_files:
            sql, read_errors = _read_input_sql(input_file.path)
            if sql is None:
                payload_stats = _empty_payload_stats()
                errors = read_errors
            else:
                schema_payloads.append((sql, input_file.path))
                payload_stats, errors = _convert_single_payload_stats(
                    sql=sql,
                    path=input_file.path,
                    relative_path=input_file.relative_path,
                    source=args.source,
                    target=args.target,
                    pretty=args.pretty,
                    out_path=out_path,
                    overwrite=args.overwrite,
                    split_statements=args.split_statements,
                    suffix=args.suffix,
                    write_stdout=out_path is None,
                )
            files_processed += 1
            output_written = output_written or payload_stats.output_written
            outputs_written += payload_stats.output_file_count
            total_statement_count += payload_stats.statement_count
            converted_statement_count += payload_stats.converted_statement_count
            statement_type_counts.update(payload_stats.statement_type_counts)
            converted_statement_type_counts.update(payload_stats.converted_statement_type_counts)
            all_errors.extend(errors)
            attempts.append(_attempt_record(path=input_file.path, payload_stats=payload_stats, errors=errors))
            if args.fail_fast and errors:
                stop_after_current = True
                break

    if args.infer_schema:
        assert out_path is not None
        all_errors.extend(
            _write_inferred_schema_artifact(
                payloads=schema_payloads,
                source=args.source,
                target=args.target,
                out_path=out_path,
                overwrite=args.overwrite,
                output_format=args.infer_schema_format,
                default_type=args.infer_schema_default_type,
                unqualified_columns=args.infer_schema_unqualified_columns,
                if_not_exists=args.infer_schema_if_not_exists,
                create_schema=args.infer_schema_create_schema,
                create_user=args.infer_schema_create_user,
                create_user_password=args.infer_schema_create_user_password,
                pretty=args.pretty,
                existing_errors=all_errors,
                fail_fast=args.fail_fast,
            )
        )

    error_report_path = _error_report_output_file(args.errors, out_path) if args.errors else None
    if error_report_path is not None:
        _write_error_report(
            error_report_path,
            all_errors,
            overwrite=False if stop_after_current else args.overwrite,
        )

    if args.log is not None:
        log_path = _log_output_file("convert", out_path)
        _write_log(
            path=log_path,
            command_text=args.invoked_command,
            command_type="convert",
            source=args.source,
            target=args.target,
            attempts=attempts,
            verbosity=args.log,
            overwrite=args.overwrite,
        )

    if args.report:
        report_path = _report_output_file(out_path)
        _write_markdown_report(
            path=report_path,
            source=args.source,
            target=args.target,
            input_count=files_processed,
            output_count=outputs_written,
            total_statement_count=total_statement_count,
            converted_statement_count=converted_statement_count,
            split_statements=args.split_statements,
            statement_type_counts=dict(statement_type_counts),
            converted_statement_type_counts=dict(converted_statement_type_counts),
            errors=all_errors,
            overwrite=args.overwrite,
        )

    if all_errors:
        _write_stderr_summary(all_errors)
        if not args.ignore_errors:
            return 1

    if stdin_sql is not None and not output_written and out_path is None:
        sys.stdout.write("")

    return 0


def run_split_statements(args: argparse.Namespace) -> int:
    _validate_error_strategy(args)
    if args.source:
        _validate_dialect(args.source, "source")
    _validate_create_user_options(
        target=args.source,
        output_format=args.infer_schema_format,
        create_user=args.infer_schema_create_user,
        create_user_password=args.infer_schema_create_user_password,
        create_user_flag="--infer-schema-create-user",
        password_flag="--infer-schema-create-user-password",
    )
    _validate_create_schema_options(
        target=args.source,
        output_format=args.infer_schema_format,
        create_schema=args.infer_schema_create_schema,
        create_schema_flag="--infer-schema-create-schema",
    )

    out_path = ensure_out_path(args.out)
    assert out_path is not None
    if out_path.suffix:
        raise FatalCliError("--out for split-statements must be a directory")

    input_files: list[InputFile] = []
    stdin_sql: str | None = None
    raw_inputs = [*args.inputs, *args.flag_inputs]
    if raw_inputs:
        try:
            input_files = expand_inputs(raw_inputs)
        except FileNotFoundError as exc:
            raise FatalCliError(str(exc)) from exc
    else:
        stdin_sql = sys.stdin.read()

    all_errors: list[ConversionError] = []
    files_processed = 0
    successful_inputs = 0
    outputs_written = 0
    total_statement_count = 0
    split_statement_count = 0
    statement_type_counts: Counter[str] = Counter()
    split_statement_type_counts: Counter[str] = Counter()
    engine_counts: Counter[str] = Counter()
    fallback_inputs = 0
    attempts: list[AttemptRecord] = []
    schema_payloads: list[tuple[str, Path | None]] = []

    if stdin_sql is not None:
        schema_payloads.append((stdin_sql, None))
        payload_stats, errors = _split_single_payload_stats(
            sql=stdin_sql,
            path=None,
            relative_path=None,
            source=args.source,
            pretty=args.pretty,
            out_path=out_path,
            overwrite=args.overwrite,
        )
        files_processed += 1
        if not errors:
            successful_inputs += 1
        outputs_written += payload_stats.output_file_count
        total_statement_count += payload_stats.statement_count
        split_statement_count += payload_stats.converted_statement_count
        statement_type_counts.update(payload_stats.statement_type_counts)
        split_statement_type_counts.update(payload_stats.converted_statement_type_counts)
        engine_counts[payload_stats.engine_used] += 1
        if payload_stats.fallback_used:
            fallback_inputs += 1
        all_errors.extend(errors)
        attempts.append(_attempt_record(path=None, payload_stats=payload_stats, errors=errors))
    else:
        for input_file in input_files:
            sql, read_errors = _read_input_sql(input_file.path)
            if sql is None:
                payload_stats = _empty_payload_stats()
                errors = read_errors
            else:
                schema_payloads.append((sql, input_file.path))
                payload_stats, errors = _split_single_payload_stats(
                    sql=sql,
                    path=input_file.path,
                    relative_path=input_file.relative_path,
                    source=args.source,
                    pretty=args.pretty,
                    out_path=out_path,
                    overwrite=args.overwrite,
                )
            files_processed += 1
            if not errors:
                successful_inputs += 1
            outputs_written += payload_stats.output_file_count
            total_statement_count += payload_stats.statement_count
            split_statement_count += payload_stats.converted_statement_count
            statement_type_counts.update(payload_stats.statement_type_counts)
            split_statement_type_counts.update(payload_stats.converted_statement_type_counts)
            engine_counts[payload_stats.engine_used] += 1
            if payload_stats.fallback_used:
                fallback_inputs += 1
            all_errors.extend(errors)
            attempts.append(_attempt_record(path=input_file.path, payload_stats=payload_stats, errors=errors))
            if args.fail_fast and errors:
                break

    if args.infer_schema:
        all_errors.extend(
            _write_inferred_schema_artifact(
                payloads=schema_payloads,
                source=args.source,
                target=args.source,
                out_path=out_path,
                overwrite=args.overwrite,
                output_format=args.infer_schema_format,
                default_type=args.infer_schema_default_type,
                unqualified_columns=args.infer_schema_unqualified_columns,
                if_not_exists=args.infer_schema_if_not_exists,
                create_schema=args.infer_schema_create_schema,
                create_user=args.infer_schema_create_user,
                create_user_password=args.infer_schema_create_user_password,
                pretty=args.pretty,
                existing_errors=all_errors,
                fail_fast=args.fail_fast,
            )
        )

    error_report_path = _error_report_output_file(args.errors, out_path) if args.errors else None
    if error_report_path is not None:
        _write_error_report(error_report_path, all_errors, overwrite=args.overwrite)

    if args.log is not None:
        log_path = _log_output_file("split-statements", out_path)
        _write_log(
            path=log_path,
            command_text=args.invoked_command,
            command_type="split-statements",
            source=args.source,
            target=None,
            attempts=attempts,
            verbosity=args.log,
            overwrite=args.overwrite,
        )

    if args.report:
        report_path = _split_report_output_file(out_path)
        _write_split_markdown_report(
            path=report_path,
            source=args.source,
            input_count=files_processed,
            success_count=successful_inputs,
            output_count=outputs_written,
            total_statement_count=total_statement_count,
            split_statement_count=split_statement_count,
            statement_type_counts=dict(statement_type_counts),
            split_statement_type_counts=dict(split_statement_type_counts),
            engine_counts=dict(engine_counts),
            fallback_inputs=fallback_inputs,
            errors=all_errors,
            overwrite=args.overwrite,
        )

    if all_errors:
        _write_stderr_summary(all_errors)
        if not args.ignore_errors:
            return 1

    return 0


def run_infer_schema(args: argparse.Namespace) -> int:
    if args.invoked_command == "madsql infer-schema":
        args.command_parser.print_help(sys.stderr)
        return 2

    _validate_error_strategy(args)
    if args.source:
        _validate_dialect(args.source, "source")
    if args.target:
        _validate_dialect(args.target, "target")
    rendered_target = args.target or args.source
    _validate_create_user_options(
        target=rendered_target,
        output_format=args.format,
        create_user=args.create_user,
        create_user_password=args.create_user_password,
        create_user_flag="--create-user",
        password_flag="--create-user-password",
    )
    _validate_create_schema_options(
        target=rendered_target,
        output_format=args.format,
        create_schema=args.create_schema,
        create_schema_flag="--create-schema",
    )

    out_path = ensure_out_path(args.out)
    if args.report and out_path is None:
        raise FatalCliError("--report requires --out")
    if args.log is not None and out_path is None:
        raise FatalCliError("--log requires --out")

    input_files: list[InputFile] = []
    stdin_sql: str | None = None
    raw_inputs = [*args.inputs, *args.flag_inputs]
    if raw_inputs:
        try:
            input_files = expand_inputs(raw_inputs)
        except FileNotFoundError as exc:
            raise FatalCliError(str(exc)) from exc
    else:
        stdin_sql = sys.stdin.read()

    payloads: list[tuple[str, Path | None]] = []
    all_errors: list[ConversionError] = []
    read_error_count = 0

    if stdin_sql is not None:
        payloads.append((stdin_sql, None))
    else:
        for input_file in input_files:
            sql, read_errors = _read_input_sql(input_file.path)
            if sql is None:
                read_error_count += 1
                all_errors.extend(read_errors)
                if args.fail_fast:
                    break
                continue
            payloads.append((sql, input_file.path))

    result = infer_schema_many(
        payloads,
        source=args.source,
        default_type=args.default_type,
        unqualified_columns=args.unqualified_columns,
        fail_fast=args.fail_fast,
    )
    all_errors.extend(result.errors)
    _validate_declared_oracle_schema_output(
        result=result,
        target=rendered_target,
        output_format=args.format,
        create_user_password=args.create_user_password,
        password_flag="--create-user-password",
    )

    if args.format == "json":
        payload = render_schema_json(result)
    else:
        payload = render_schema_ddl(
            result,
            target=rendered_target,
            if_not_exists=args.if_not_exists,
            create_schema=args.create_schema,
            create_user=args.create_user,
            create_user_password=args.create_user_password,
            pretty=args.pretty,
        )

    if out_path is None:
        if payload:
            sys.stdout.write(payload)
    else:
        output_file = _schema_output_file(
            out_path=out_path,
            output_format=args.format,
            source=args.source,
            target=args.target,
        )
        _safe_write_text(output_file, payload, overwrite=args.overwrite)

    error_report_path = _error_report_output_file(args.errors, out_path) if args.errors else None
    if error_report_path is not None:
        _write_error_report(error_report_path, all_errors, overwrite=args.overwrite)

    total_inputs = read_error_count + result.input_count
    successful_inputs = result.successful_input_count

    if args.log is not None:
        assert out_path is not None
        log_path = _log_output_file("infer-schema", out_path)
        _write_infer_schema_log(
            path=log_path,
            command_text=args.invoked_command,
            source=args.source,
            target=rendered_target,
            output_format=args.format,
            input_count=total_inputs,
            success_count=successful_inputs,
            result=result,
            errors=all_errors,
            verbosity=args.log,
            overwrite=args.overwrite,
        )

    if args.report:
        assert out_path is not None
        report_path = _infer_schema_report_output_file(out_path)
        _write_infer_schema_markdown_report(
            path=report_path,
            source=args.source,
            target=rendered_target,
            output_format=args.format,
            input_count=total_inputs,
            success_count=successful_inputs,
            result=result,
            errors=all_errors,
            overwrite=args.overwrite,
        )

    if all_errors:
        _write_stderr_summary(all_errors)
        if not args.ignore_errors:
            return 1

    return 0


def _convert_single_payload(
    *,
    sql: str,
    path: Path | None,
    relative_path: Path | None,
    source: str,
    target: str,
    pretty: bool,
    out_path: Path | None,
    overwrite: bool,
    split_statements: bool,
    suffix: str,
    write_stdout: bool,
) -> tuple[bool, list[ConversionError], int]:
    payload_stats, errors = _convert_single_payload_stats(
        sql=sql,
        path=path,
        relative_path=relative_path,
        source=source,
        target=target,
        pretty=pretty,
        out_path=out_path,
        overwrite=overwrite,
        split_statements=split_statements,
        suffix=suffix,
        write_stdout=write_stdout,
    )
    return payload_stats.output_written, errors, payload_stats.converted_statement_count


def _convert_single_payload_stats(
    *,
    sql: str,
    path: Path | None,
    relative_path: Path | None,
    source: str,
    target: str,
    pretty: bool,
    out_path: Path | None,
    overwrite: bool,
    split_statements: bool,
    suffix: str,
    write_stdout: bool,
) -> tuple[PayloadStats, list[ConversionError]]:
    result = convert_sql(sql, source=source, target=target, pretty=pretty, path=path)
    statement_type_counts = Counter(result.statement_types_by_index.values())
    converted_statement_type_counts = Counter(statement.statement_type for statement in result.statements)
    payload_stats = PayloadStats(
        output_written=False,
        output_file_count=0,
        statement_count=len(result.statement_types_by_index),
        converted_statement_count=len(result.statements),
        statement_type_counts=dict(statement_type_counts),
        converted_statement_type_counts=dict(converted_statement_type_counts),
    )

    if write_stdout:
        payload = ";\n".join(statement.sql for statement in result.statements)
        if payload:
            sys.stdout.write(payload)
            if not payload.endswith("\n"):
                sys.stdout.write("\n")
            return (
                PayloadStats(
                    output_written=True,
                    output_file_count=1,
                    statement_count=payload_stats.statement_count,
                    converted_statement_count=payload_stats.converted_statement_count,
                    statement_type_counts=payload_stats.statement_type_counts,
                    converted_statement_type_counts=payload_stats.converted_statement_type_counts,
                ),
                result.errors,
            )
        return payload_stats, result.errors

    assert out_path is not None
    if split_statements:
        base_dir = _split_output_dir(out_path=out_path, relative_path=relative_path, path=path)
        for statement in result.statements:
            file_name = f"{statement.statement_index:04d}_stmt.{target}{suffix}"
            _safe_write_text(base_dir / file_name, statement.sql + "\n", overwrite=overwrite)
        return (
            PayloadStats(
                output_written=bool(result.statements),
                output_file_count=len(result.statements),
                statement_count=payload_stats.statement_count,
                converted_statement_count=payload_stats.converted_statement_count,
                statement_type_counts=payload_stats.statement_type_counts,
                converted_statement_type_counts=payload_stats.converted_statement_type_counts,
            ),
            result.errors,
        )

    output_file = _single_output_file(out_path=out_path, relative_path=relative_path, path=path, target=target, suffix=suffix)
    payload = ";\n".join(statement.sql for statement in result.statements)
    if payload:
        payload += "\n"
        _safe_write_text(output_file, payload, overwrite=overwrite)
        return (
            PayloadStats(
                output_written=True,
                output_file_count=1,
                statement_count=payload_stats.statement_count,
                converted_statement_count=payload_stats.converted_statement_count,
                statement_type_counts=payload_stats.statement_type_counts,
                converted_statement_type_counts=payload_stats.converted_statement_type_counts,
            ),
            result.errors,
        )
    return payload_stats, result.errors


def _split_single_payload(
    *,
    sql: str,
    path: Path | None,
    relative_path: Path | None,
    source: str | None,
    pretty: bool,
    out_path: Path,
    overwrite: bool,
) -> tuple[bool, list[ConversionError], int]:
    payload_stats, errors = _split_single_payload_stats(
        sql=sql,
        path=path,
        relative_path=relative_path,
        source=source,
        pretty=pretty,
        out_path=out_path,
        overwrite=overwrite,
    )
    return payload_stats.output_written, errors, payload_stats.converted_statement_count


def _split_single_payload_stats(
    *,
    sql: str,
    path: Path | None,
    relative_path: Path | None,
    source: str | None,
    pretty: bool,
    out_path: Path,
    overwrite: bool,
) -> tuple[PayloadStats, list[ConversionError]]:
    result = split_sql(sql, source=source, pretty=pretty, path=path)
    statement_type_counts = Counter(result.statement_types_by_index.values())
    split_statement_type_counts = Counter(statement.statement_type for statement in result.statements)
    base_dir = _split_output_dir(out_path=out_path, relative_path=relative_path, path=path)
    for statement in result.statements:
        file_name = f"{statement.statement_index:04d}_stmt.sql"
        _safe_write_text(base_dir / file_name, statement.sql + "\n", overwrite=overwrite)
    return (
        PayloadStats(
            output_written=bool(result.statements),
            output_file_count=len(result.statements),
            statement_count=len(result.statement_types_by_index),
            converted_statement_count=len(result.statements),
            statement_type_counts=dict(statement_type_counts),
            converted_statement_type_counts=dict(split_statement_type_counts),
            engine_used=result.engine_used,
            fallback_used=result.engine_used != "sqlglot",
        ),
        result.errors,
    )


def _single_output_file(
    *,
    out_path: Path,
    relative_path: Path | None,
    path: Path | None,
    target: str,
    suffix: str,
) -> Path:
    if relative_path is None or path is None:
        if out_path.suffix:
            return out_path
        return out_path / f"stdin.{target}{suffix}"

    stem = relative_path.stem
    extension = f".{target}{suffix}"
    relative_parent = relative_path.parent
    return out_path / relative_parent / f"{stem}{extension}"


def _split_output_dir(*, out_path: Path, relative_path: Path | None, path: Path | None) -> Path:
    if relative_path is None or path is None:
        return out_path / "stdin"
    relative_parent = relative_path.parent
    return out_path / relative_parent / relative_path.stem


def _schema_output_file(
    *,
    out_path: Path,
    output_format: str,
    source: str | None,
    target: str | None,
) -> Path:
    if out_path.suffix:
        return out_path
    extension = ".json" if output_format == "json" else ".sql"
    return out_path / f"inferred_schema{_infer_schema_name_suffix(source=source, target=target)}{extension}"


def _infer_schema_artifact_output_file(
    *,
    out_path: Path,
    output_format: str,
    source: str | None,
    target: str | None,
) -> Path:
    extension = ".json" if output_format == "json" else ".sql"
    return _artifact_output_dir(out_path) / f"inferred_schema{_infer_schema_name_suffix(source=source, target=target)}{extension}"


def _infer_schema_name_suffix(*, source: str | None, target: str | None) -> str:
    if source and target and source != target:
        return f"-{source}-to-{target}"
    if source:
        return f"-{source}"
    if target:
        return f"-to-{target}"
    return ""


def _write_error_report(path: Path, errors: list[ConversionError], *, overwrite: bool) -> None:
    payload = json.dumps(
        {
            "version_info": _version_info(),
            "errors": [error.to_dict() for error in errors],
        },
        indent=2,
    ) + "\n"
    _safe_write_text(path, payload, overwrite=overwrite)


def _write_inferred_schema_artifact(
    *,
    payloads: list[tuple[str, Path | None]],
    source: str | None,
    target: str | None,
    out_path: Path,
    overwrite: bool,
    output_format: str,
    default_type: str,
    unqualified_columns: str,
    if_not_exists: bool,
    create_schema: bool,
    create_user: bool,
    create_user_password: str | None,
    pretty: bool,
    existing_errors: list[ConversionError],
    fail_fast: bool,
) -> list[ConversionError]:
    if not payloads:
        return []

    result = infer_schema_many(
        payloads,
        source=source,
        default_type=default_type,
        unqualified_columns=unqualified_columns,
        fail_fast=fail_fast,
    )
    _validate_declared_oracle_schema_output(
        result=result,
        target=target,
        output_format=output_format,
        create_user_password=create_user_password,
        password_flag="--infer-schema-create-user-password",
    )
    artifact_path = _infer_schema_artifact_output_file(
        out_path=out_path,
        output_format=output_format,
        source=source,
        target=target,
    )
    if output_format == "json":
        payload = render_schema_json(result)
    else:
        payload = render_schema_ddl(
            result,
            target=target,
            if_not_exists=if_not_exists,
            create_schema=create_schema,
            create_user=create_user,
            create_user_password=create_user_password,
            pretty=pretty,
        )
    _safe_write_text(artifact_path, payload, overwrite=overwrite)
    return _dedupe_errors(existing=existing_errors, additional=result.errors)


def _read_input_sql(path: Path) -> tuple[str | None, list[ConversionError]]:
    try:
        return read_utf8(path), []
    except UnicodeDecodeError as exc:
        return None, [
            ConversionError.from_exception(
                path=path,
                statement_index=1,
                error_type="read_error",
                message=f"Unable to decode input as UTF-8: {exc}",
            )
        ]
    except OSError as exc:
        return None, [
            ConversionError.from_exception(
                path=path,
                statement_index=1,
                error_type="read_error",
                message=f"Unable to read input file: {exc}",
            )
        ]


def _empty_payload_stats() -> PayloadStats:
    return PayloadStats(
        output_written=False,
        output_file_count=0,
        statement_count=0,
        converted_statement_count=0,
        statement_type_counts={},
        converted_statement_type_counts={},
    )


def _write_markdown_report(
    *,
    path: Path,
    source: str,
    target: str,
    input_count: int,
    output_count: int,
    total_statement_count: int,
    converted_statement_count: int,
    split_statements: bool,
    statement_type_counts: dict[str, int],
    converted_statement_type_counts: dict[str, int],
    errors: list[ConversionError],
    overwrite: bool,
) -> None:
    failed_statement_count = max(0, total_statement_count - converted_statement_count)
    conversion_rate = 0.0
    if total_statement_count > 0:
        conversion_rate = (converted_statement_count / total_statement_count) * 100.0

    error_type_counts = Counter(error.error_type for error in errors)
    statement_types = sorted(statement_type_counts)

    lines = [
        "# madsql Conversion Report",
        "",
        "## Summary",
        f"- Source Dialect: `{source}`",
        f"- Target Dialect: `{target}`",
        f"- Split Statements Mode: `{'yes' if split_statements else 'no'}`",
        f"- Inputs Processed: `{input_count}`",
        f"- Output Files Written: `{output_count}`",
        f"- Total Statements: `{total_statement_count}`",
        f"- Successfully Converted: `{converted_statement_count}`",
        f"- Failed: `{failed_statement_count}`",
        f"- Conversion Rate: `{converted_statement_count}/{total_statement_count} ({conversion_rate:.1f}%)`",
        "",
    ]
    lines.extend(_report_version_section())

    if statement_types:
        lines.extend(
            [
                "## Statement Type Counts",
                "",
                "| Statement Type | Total | Converted | Failed |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for statement_type in statement_types:
            total = statement_type_counts.get(statement_type, 0)
            converted = converted_statement_type_counts.get(statement_type, 0)
            failed = max(0, total - converted)
            lines.append(f"| `{statement_type}` | {total} | {converted} | {failed} |")
        lines.append("")

    if error_type_counts:
        lines.extend(
            [
                "## Error Type Counts",
                "",
                "| Error Type | Count |",
                "| --- | ---: |",
            ]
        )
        for error_type in sorted(error_type_counts):
            lines.append(f"| `{error_type}` | {error_type_counts[error_type]} |")
        lines.append("")

    payload = "\n".join(lines)
    if not payload.endswith("\n"):
        payload += "\n"
    _safe_write_text(path, payload, overwrite=overwrite)


def _report_output_file(out_path: Path | None) -> Path:
    if out_path is None:
        raise FatalCliError("--report requires --out")
    timestamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    base_dir = _artifact_output_dir(out_path)
    return base_dir / f"{timestamp}-madsql-convert-report.md"


def _write_split_markdown_report(
    *,
    path: Path,
    source: str | None,
    input_count: int,
    success_count: int,
    output_count: int,
    total_statement_count: int,
    split_statement_count: int,
    statement_type_counts: dict[str, int],
    split_statement_type_counts: dict[str, int],
    engine_counts: dict[str, int],
    fallback_inputs: int,
    errors: list[ConversionError],
    overwrite: bool,
) -> None:
    failure_count = max(0, input_count - success_count)
    success_rate = 0.0
    if input_count > 0:
        success_rate = (success_count / input_count) * 100.0

    failed_statement_count = max(0, total_statement_count - split_statement_count)
    statement_types = sorted(statement_type_counts)
    error_type_counts = Counter(error.error_type for error in errors)
    source_value = source if source else "n/a"

    lines = [
        "# madsql Run Report",
        "",
        "## Summary",
        "- Command Type: `split-statements`",
        f"- Source Dialect: `{source_value}`",
        f"- Inputs Processed: `{input_count}`",
        f"- Successful Inputs: `{success_count}`",
        f"- Failed Inputs: `{failure_count}`",
        f"- Success Rate: `{success_count}/{input_count} ({success_rate:.1f}%)`",
        f"- Output Files Written: `{output_count}`",
        f"- Total Statements: `{total_statement_count}`",
        f"- Statements Split: `{split_statement_count}`",
        f"- Statements Failed: `{failed_statement_count}`",
        f"- Fallback Inputs (sqlparse): `{fallback_inputs}`",
        "",
    ]
    lines.extend(_report_version_section())

    if engine_counts:
        lines.extend(
            [
                "## Split Engine Usage",
                "",
                "| Engine | Inputs |",
                "| --- | ---: |",
            ]
        )
        for engine in sorted(engine_counts):
            lines.append(f"| `{engine}` | {engine_counts[engine]} |")
        lines.append("")

    if statement_types:
        lines.extend(
            [
                "## Statement Type Counts",
                "",
                "| Statement Type | Total | Split | Failed |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for statement_type in statement_types:
            total = statement_type_counts.get(statement_type, 0)
            split = split_statement_type_counts.get(statement_type, 0)
            failed = max(0, total - split)
            lines.append(f"| `{statement_type}` | {total} | {split} | {failed} |")
        lines.append("")

    if error_type_counts:
        lines.extend(
            [
                "## Error Type Counts",
                "",
                "| Error Type | Count |",
                "| --- | ---: |",
            ]
        )
        for error_type in sorted(error_type_counts):
            lines.append(f"| `{error_type}` | {error_type_counts[error_type]} |")
        lines.append("")

    payload = "\n".join(lines)
    if not payload.endswith("\n"):
        payload += "\n"
    _safe_write_text(path, payload, overwrite=overwrite)


def _split_report_output_file(out_path: Path) -> Path:
    timestamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    return _artifact_output_dir(out_path) / f"{timestamp}-madsql-split-statements-report.md"


def _infer_schema_report_output_file(out_path: Path) -> Path:
    timestamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    return _artifact_output_dir(out_path) / f"{timestamp}-madsql-infer-schema-report.md"


def _error_report_output_file(error_report_arg: str, out_path: Path | None) -> Path:
    error_report_path = Path(error_report_arg)
    if out_path is None or error_report_path.is_absolute():
        return error_report_path
    output_base = out_path.parent if out_path.suffix else out_path
    return output_base / error_report_path.name


def _log_output_file(command_name: str, out_path: Path | None) -> Path:
    if out_path is None:
        raise FatalCliError("--log requires --out")
    timestamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    return _artifact_output_dir(out_path) / f"{timestamp}-madsql-{command_name}.log"


def _artifact_output_dir(out_path: Path) -> Path:
    return out_path.parent if out_path.suffix else out_path


def _write_log(
    *,
    path: Path,
    command_text: str,
    command_type: str,
    source: str | None,
    target: str | None,
    attempts: list[AttemptRecord],
    verbosity: int,
    overwrite: bool,
) -> None:
    success_count = sum(1 for attempt in attempts if attempt.error_count == 0)
    failure_count = len(attempts) - success_count
    success_rate = _rate(success_count, len(attempts))

    lines = [
        f"timestamp: {datetime.now().astimezone().isoformat()}",
        f"command: {command_text}",
        f"command_type: {command_type}",
        f"source_dialect: {source or 'n/a'}",
        f"target_dialect: {target or 'n/a'}",
        *_log_version_lines(),
        f"attempts: {len(attempts)}",
        f"successes: {success_count}",
        f"failures: {failure_count}",
        f"success_rate: {success_rate}",
    ]

    if verbosity == 1:
        lines.append("attempt_details:")
        for attempt in attempts:
            lines.append(
                "  - "
                f"path: {attempt.path}, status: {attempt.status}, statements: {attempt.statement_count}, "
                f"converted: {attempt.converted_statement_count}, errors: {attempt.error_count}"
            )
            for error in attempt.errors:
                lines.append(
                    "    error: "
                    f"path={error.path or '<stdin>'}, statement_index={error.statement_index}, "
                    f"type={error.error_type}, message={error.message}"
                )

    payload = "\n".join(lines)
    if not payload.endswith("\n"):
        payload += "\n"
    _safe_write_text(path, payload, overwrite=overwrite)


def _write_infer_schema_log(
    *,
    path: Path,
    command_text: str,
    source: str | None,
    target: str | None,
    output_format: str,
    input_count: int,
    success_count: int,
    result,
    errors: list[ConversionError],
    verbosity: int,
    overwrite: bool,
) -> None:
    failure_count = max(0, input_count - success_count)
    success_rate = _rate(success_count, input_count)
    render_dialect = target or "n/a"
    low_confidence_columns = sum(
        1 for table in result.tables for column in table.columns if column.confidence == "low"
    )

    lines = [
        f"timestamp: {datetime.now().astimezone().isoformat()}",
        f"command: {command_text}",
        "command_type: infer-schema",
        f"source_dialect: {source or 'n/a'}",
        f"render_dialect: {render_dialect}",
        f"output_format: {output_format}",
        *_log_version_lines(),
        f"inputs_processed: {input_count}",
        f"successes: {success_count}",
        f"failures: {failure_count}",
        f"success_rate: {success_rate}",
        f"statements_parsed: {result.statement_count}",
        f"tables_inferred: {result.table_count}",
        f"columns_inferred: {result.column_count}",
        f"low_confidence_columns: {low_confidence_columns}",
    ]

    if verbosity == 1:
        lines.append("error_details:")
        if errors:
            for error in errors:
                lines.append(
                    "  - "
                    f"path={error.path or '<stdin>'}, statement_index={error.statement_index}, "
                    f"type={error.error_type}, "
                    f"statement_type={error.statement_type or 'n/a'}, message={error.message}"
                )
                if error.statement_sql:
                    lines.append("    statement_sql:")
                    for sql_line in error.statement_sql.splitlines():
                        lines.append(f"      {sql_line}")
        else:
            lines.append("  - none")

    payload = "\n".join(lines)
    if not payload.endswith("\n"):
        payload += "\n"
    _safe_write_text(path, payload, overwrite=overwrite)


def _write_infer_schema_markdown_report(
    *,
    path: Path,
    source: str | None,
    target: str | None,
    output_format: str,
    input_count: int,
    success_count: int,
    result,
    errors: list[ConversionError],
    overwrite: bool,
) -> None:
    failure_count = max(0, input_count - success_count)
    success_rate = 0.0
    if input_count > 0:
        success_rate = (success_count / input_count) * 100.0

    error_type_counts = Counter(error.error_type for error in errors)
    confidence_counts = Counter(column.confidence for table in result.tables for column in table.columns)
    render_dialect = target or "n/a"

    lines = [
        "# madsql Infer Schema Report",
        "",
        "## Summary",
        "- Command Type: `infer-schema`",
        f"- Source Dialect: `{source or 'n/a'}`",
        f"- Render Dialect: `{render_dialect}`",
        f"- Output Format: `{output_format}`",
        f"- Inputs Processed: `{input_count}`",
        f"- Successful Inputs: `{success_count}`",
        f"- Failed Inputs: `{failure_count}`",
        f"- Success Rate: `{success_count}/{input_count} ({success_rate:.1f}%)`",
        f"- Statements Parsed: `{result.statement_count}`",
        f"- Tables Inferred: `{result.table_count}`",
        f"- Columns Inferred: `{result.column_count}`",
        "",
    ]
    lines.extend(_report_version_section())

    if confidence_counts:
        lines.extend(
            [
                "## Column Confidence",
                "",
                "| Confidence | Count |",
                "| --- | ---: |",
            ]
        )
        for confidence in sorted(confidence_counts):
            lines.append(f"| `{confidence}` | {confidence_counts[confidence]} |")
        lines.append("")

    if error_type_counts:
        lines.extend(
            [
                "## Error Type Counts",
                "",
                "| Error Type | Count |",
                "| --- | ---: |",
            ]
        )
        for error_type in sorted(error_type_counts):
            lines.append(f"| `{error_type}` | {error_type_counts[error_type]} |")
        lines.append("")

    if errors:
        lines.extend(["## Error Details", ""])
        for error in errors:
            lines.extend(
                [
                    f"### Statement {error.statement_index}",
                    f"- Path: `{error.path or '<stdin>'}`",
                    f"- Error Type: `{error.error_type}`",
                    f"- Statement Type: `{error.statement_type or 'n/a'}`",
                    "- Message:",
                    "```text",
                    error.message,
                    "```",
                ]
            )
            if error.statement_sql:
                lines.extend(
                    [
                        "- Statement:",
                        "```sql",
                        error.statement_sql,
                        "```",
                    ]
                )
            lines.append("")

    payload = "\n".join(lines)
    if not payload.endswith("\n"):
        payload += "\n"
    _safe_write_text(path, payload, overwrite=overwrite)


def _write_stderr_summary(errors: list[ConversionError]) -> None:
    for error in errors:
        location = error.path if error.path is not None else "<stdin>"
        sys.stderr.write(
            f"{location}: statement {error.statement_index}: {error.error_type}: {error.message}\n"
        )
    if errors:
        sys.stderr.write(f"{_debug_version_summary()}\n")


def _add_infer_schema_artifact_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--infer-schema",
        action="store_true",
        help="Infer schemas from the input SQL and write deterministic inferred_schema-<dialect>.sql artifacts beside normal outputs",
    )
    parser.add_argument(
        "--infer-schema-format",
        choices=["ddl", "json"],
        default="ddl",
        help="Artifact format for --infer-schema",
    )
    parser.add_argument(
        "--infer-schema-default-type",
        default="TEXT",
        help=(
            "Fallback SQL type used by --infer-schema when stronger evidence is unavailable. "
            f"Common values: {COMMON_DEFAULT_TYPE_VALUES}"
        ),
    )
    parser.add_argument(
        "--infer-schema-unqualified-columns",
        choices=["first-table", "skip"],
        default="first-table",
        help="How --infer-schema handles unqualified columns in multi-table queries",
    )
    parser.add_argument(
        "--infer-schema-if-not-exists",
        action="store_true",
        help="Emit CREATE TABLE IF NOT EXISTS when --infer-schema outputs DDL",
    )
    parser.add_argument(
        "--infer-schema-create-schema",
        action="store_true",
        help="For non-Oracle DDL artifacts, prepend CREATE SCHEMA statements for inferred schema names",
    )
    parser.add_argument(
        "--infer-schema-create-user",
        action="store_true",
        help="For Oracle DDL artifacts, prepend CREATE USER and GRANT statements for inferred schema names",
    )
    parser.add_argument(
        "--infer-schema-create-user-password",
        help="Quoted Oracle password used by --infer-schema-create-user and Oracle rendering of input CREATE SCHEMA/CREATE DATABASE statements",
    )


def _validate_dialect(name: str, flag_name: str) -> None:
    if name not in Dialect.classes:
        raise FatalCliError(f"Unsupported {flag_name} dialect: {name}")


def _validate_error_strategy(args: argparse.Namespace) -> None:
    if getattr(args, "continue_on_error", False) and getattr(args, "fail_fast", False):
        raise FatalCliError("Use either --continue or --fail-fast, not both.")


def _validate_create_user_options(
    *,
    target: str | None,
    output_format: str,
    create_user: bool,
    create_user_password: str | None,
    create_user_flag: str,
    password_flag: str,
) -> None:
    if not create_user:
        return
    if output_format != "ddl":
        raise FatalCliError(f"{create_user_flag} requires DDL output")
    if (target or "").lower() != "oracle":
        raise FatalCliError(f"{create_user_flag} is only supported when the rendered target dialect is oracle")
    if not create_user_password:
        raise FatalCliError(f"{password_flag} is required with {create_user_flag}")


def _validate_create_schema_options(
    *,
    target: str | None,
    output_format: str,
    create_schema: bool,
    create_schema_flag: str,
) -> None:
    if not create_schema:
        return
    if output_format != "ddl":
        raise FatalCliError(f"{create_schema_flag} requires DDL output")
    if (target or "").lower() == "oracle":
        raise FatalCliError(f"{create_schema_flag} is not supported for oracle; use --create-user instead")


def _validate_declared_oracle_schema_output(
    *,
    result,
    target: str | None,
    output_format: str,
    create_user_password: str | None,
    password_flag: str,
) -> None:
    if output_format != "ddl":
        return
    if (target or "").lower() != "oracle":
        return
    if not result.declared_schema_names:
        return
    if create_user_password:
        return
    raise FatalCliError(
        f"{password_flag} is required to render input CREATE SCHEMA/CREATE DATABASE statements for oracle"
    )


def _safe_write_text(path: Path, content: str, *, overwrite: bool) -> None:
    try:
        write_text(path, content, overwrite=overwrite)
    except FileExistsError as exc:
        raise FatalCliError(str(exc)) from exc


def _attempt_record(*, path: Path | None, payload_stats: PayloadStats, errors: list[ConversionError]) -> AttemptRecord:
    return AttemptRecord(
        path=str(path) if path is not None else "<stdin>",
        status="failed" if errors else "success",
        statement_count=payload_stats.statement_count,
        converted_statement_count=payload_stats.converted_statement_count,
        error_count=len(errors),
        errors=errors,
    )


def _command_text(argv: list[str] | None) -> str:
    args = argv if argv is not None else sys.argv[1:]
    return "madsql" if not args else f"madsql {' '.join(args)}"


def _version_info() -> dict[str, str]:
    return {
        "madsql": MADSQL_VERSION,
        "python": sys.version.split()[0],
        "sqlglot": SQLGLOT_VERSION,
        "sqlparse": sqlparse.__version__,
        "platform": platform.platform(),
    }


def _version_text() -> str:
    version_info = _version_info()
    return "\n".join(
        [
            f"madsql {version_info['madsql']}",
            f"python {version_info['python']}",
            f"sqlglot {version_info['sqlglot']}",
            f"sqlparse {version_info['sqlparse']}",
            f"platform {version_info['platform']}",
        ]
    )


def _log_version_lines() -> list[str]:
    version_info = _version_info()
    return [
        f"madsql_version: {version_info['madsql']}",
        f"python_version: {version_info['python']}",
        f"sqlglot_version: {version_info['sqlglot']}",
        f"sqlparse_version: {version_info['sqlparse']}",
        f"platform: {version_info['platform']}",
    ]


def _report_version_section() -> list[str]:
    version_info = _version_info()
    return [
        "## Version Information",
        "",
        f"- madsql: `{version_info['madsql']}`",
        f"- Python: `{version_info['python']}`",
        f"- SQLGlot: `{version_info['sqlglot']}`",
        f"- SQLParse: `{version_info['sqlparse']}`",
        f"- Platform: `{version_info['platform']}`",
        "",
    ]


def _debug_version_summary() -> str:
    version_info = _version_info()
    return (
        "debug_versions: "
        f"madsql={version_info['madsql']}, "
        f"python={version_info['python']}, "
        f"sqlglot={version_info['sqlglot']}, "
        f"sqlparse={version_info['sqlparse']}, "
        f"platform={version_info['platform']}"
    )


def _rate(successes: int, total: int) -> str:
    if total == 0:
        return "0/0 (0.0%)"
    percentage = (successes / total) * 100.0
    return f"{successes}/{total} ({percentage:.1f}%)"


def _dedupe_errors(
    *,
    existing: list[ConversionError],
    additional: list[ConversionError],
) -> list[ConversionError]:
    seen = {
        (
            error.path,
            error.statement_index,
            error.error_type,
            error.message,
            error.statement_type,
            error.statement_sql,
        )
        for error in existing
    }
    deduped: list[ConversionError] = []
    for error in additional:
        key = (
            error.path,
            error.statement_index,
            error.error_type,
            error.message,
            error.statement_type,
            error.statement_sql,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(error)
    return deduped
