from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re

import sqlparse
from sqlglot import parse
from sqlglot.expressions import Expression
from sqlglot.errors import ParseError

from madsql.errors import ConversionError


@dataclass(frozen=True)
class RenderedStatement:
    statement_index: int
    sql: str
    statement_type: str = "UNKNOWN"


@dataclass(frozen=True)
class ConversionResult:
    statements: list[RenderedStatement]
    errors: list[ConversionError]
    statement_types_by_index: dict[int, str] = field(default_factory=dict)
    engine_used: str = "sqlglot"
    fallback_reason: str | None = None


def convert_sql(
    sql: str,
    *,
    source: str,
    target: str,
    pretty: bool,
    path: Path | None,
) -> ConversionResult:
    try:
        expressions = parse(sql, read=source)
    except ParseError as exc:
        return ConversionResult(
            statements=[],
            statement_types_by_index={1: "UNPARSED"},
            errors=[
                ConversionError.from_exception(
                    path=path,
                    statement_index=1,
                    error_type="parse_error",
                    message=str(exc),
                )
            ],
        )

    statements: list[RenderedStatement] = []
    statement_types_by_index: dict[int, str] = {}
    errors: list[ConversionError] = []

    statement_index = 0
    for expression in expressions:
        # SQLGlot can emit None for empty statements created by repeated delimiters.
        if expression is None:
            continue
        statement_index += 1
        statement_type = _statement_type(expression)
        statement_types_by_index[statement_index] = statement_type
        try:
            statements.append(
                RenderedStatement(
                    statement_index=statement_index,
                    statement_type=statement_type,
                    sql=expression.sql(dialect=target, pretty=pretty),
                )
            )
        except Exception as exc:  # pragma: no cover
            errors.append(
                ConversionError.from_exception(
                    path=path,
                    statement_index=statement_index,
                    error_type="convert_error",
                    message=str(exc),
                )
            )

    return ConversionResult(
        statements=statements,
        statement_types_by_index=statement_types_by_index,
        errors=errors,
    )


def split_sql(
    sql: str,
    *,
    source: str | None,
    pretty: bool,
    path: Path | None,
) -> ConversionResult:
    try:
        expressions = parse(sql, read=source)
    except ParseError as parse_exc:
        fallback_statements = _split_with_sqlparse(sql)
        if len(fallback_statements) > 1:
            rendered_statements = [
                RenderedStatement(
                    statement_index=index,
                    statement_type=_statement_type_from_sql_text(statement),
                    sql=statement,
                )
                for index, statement in enumerate(fallback_statements, start=1)
            ]
            return ConversionResult(
                statements=rendered_statements,
                statement_types_by_index={
                    statement.statement_index: statement.statement_type for statement in rendered_statements
                },
                errors=[],
                engine_used="sqlparse",
                fallback_reason=str(parse_exc),
            )

        return ConversionResult(
            statements=[],
            statement_types_by_index={1: "UNPARSED"},
            errors=[
                ConversionError.from_exception(
                    path=path,
                    statement_index=1,
                    error_type="parse_error",
                    message=str(parse_exc),
                )
            ],
            engine_used="sqlglot",
        )

    statements: list[RenderedStatement] = []
    statement_types_by_index: dict[int, str] = {}
    errors: list[ConversionError] = []

    statement_index = 0
    for expression in expressions:
        if expression is None:
            continue
        statement_index += 1
        statement_type = _statement_type(expression)
        statement_types_by_index[statement_index] = statement_type
        try:
            if source:
                rendered = expression.sql(dialect=source, pretty=pretty)
            else:
                rendered = expression.sql(pretty=pretty)
            statements.append(
                RenderedStatement(
                    statement_index=statement_index,
                    statement_type=statement_type,
                    sql=rendered,
                )
            )
        except Exception as exc:  # pragma: no cover
            errors.append(
                ConversionError.from_exception(
                    path=path,
                    statement_index=statement_index,
                    error_type="split_error",
                    message=str(exc),
                )
            )

    return ConversionResult(
        statements=statements,
        statement_types_by_index=statement_types_by_index,
        errors=errors,
        engine_used="sqlglot",
    )


def _statement_type(expression: Expression) -> str:
    if expression.key == "create":
        kind = expression.args.get("kind")
        if isinstance(kind, str):
            return f"CREATE {kind.upper()}"
        return "CREATE"
    return expression.key.upper()


def _split_with_sqlparse(sql: str) -> list[str]:
    return [statement.strip().rstrip(";") for statement in sqlparse.split(sql) if statement.strip()]


def _statement_type_from_sql_text(statement: str) -> str:
    match = re.match(r"\s*([A-Za-z]+)(?:\s+([A-Za-z]+))?", statement)
    if not match:
        return "UNKNOWN"
    first = match.group(1).upper()
    second = (match.group(2) or "").upper()
    if first == "CREATE" and second:
        return f"CREATE {second}"
    return first
