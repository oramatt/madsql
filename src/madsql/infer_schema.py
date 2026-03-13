from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
import re

import sqlparse
from sqlglot import exp, parse
from sqlglot.errors import ParseError, TokenError
from sqlglot.optimizer.scope import Scope, build_scope

from madsql.errors import ConversionError


SUPPORTED_INFER_SCHEMA_STATEMENT_TYPES = {
    "USE",
    "SELECT",
    "DELETE",
    "INSERT",
    "UPDATE",
    "CREATE SCHEMA",
    "CREATE DATABASE",
    "CREATE TABLE",
    "CREATE VIEW",
    "CREATE MATERIALIZED VIEW",
    "CREATE INDEX",
}


@dataclass(frozen=True)
class InferredColumn:
    name: str
    data_type: str
    type_source: str
    confidence: str
    evidence: tuple[str, ...] = ()


@dataclass(frozen=True)
class InferredTable:
    name: str
    db: str | None
    catalog: str | None
    columns: list[InferredColumn] = field(default_factory=list)

    @property
    def qualified_name(self) -> str:
        parts = [part for part in (self.catalog, self.db, self.name) if part]
        return ".".join(parts)


@dataclass(frozen=True)
class SchemaInferenceResult:
    tables: list[InferredTable]
    errors: list[ConversionError]
    statement_count: int
    declared_schema_names: tuple[str, ...] = ()
    input_count: int = 0
    successful_input_count: int = 0

    @property
    def table_count(self) -> int:
        return len(self.tables)

    @property
    def column_count(self) -> int:
        return sum(len(table.columns) for table in self.tables)


@dataclass
class _MutableColumn:
    name: str
    evidence_counts: Counter[str] = field(default_factory=Counter)
    explicit_type_counts: Counter[str] = field(default_factory=Counter)
    inferred_type_counts: Counter[str] = field(default_factory=Counter)

    def add_evidence(self, evidence: str) -> None:
        self.evidence_counts[evidence] += 1

    def add_explicit_type(self, data_type: str | None) -> None:
        if data_type:
            self.explicit_type_counts[data_type.upper()] += 1

    def add_inferred_type(self, data_type: str | None) -> None:
        if data_type:
            self.inferred_type_counts[data_type.upper()] += 1

    def render(self, *, default_type: str) -> InferredColumn:
        explicit_type = _most_common(self.explicit_type_counts)
        if explicit_type is not None:
            data_type = explicit_type
            type_source = "explicit"
        else:
            data_type, type_source = _resolve_inferred_type(
                name=self.name,
                inferred_type_counts=self.inferred_type_counts,
                default_type=default_type,
            )

        confidence = "high"
        if self.evidence_counts.get("query_unqualified", 0):
            confidence = "low"
        elif self.evidence_counts.get("query_single_source", 0):
            confidence = "medium"

        return InferredColumn(
            name=self.name,
            data_type=data_type,
            type_source=type_source,
            confidence=confidence,
            evidence=tuple(sorted(self.evidence_counts)),
        )


@dataclass
class _MutableTable:
    name: str
    db: str | None
    catalog: str | None
    columns: dict[str, _MutableColumn] = field(default_factory=dict)

    def column(self, name: str) -> _MutableColumn:
        key = name.lower()
        if key not in self.columns:
            self.columns[key] = _MutableColumn(name=name)
        return self.columns[key]

    def render(self, *, default_type: str) -> InferredTable:
        rendered_columns = [
            column.render(default_type=default_type)
            for _, column in sorted(self.columns.items(), key=lambda item: item[0])
        ]
        return InferredTable(
            name=self.name,
            db=self.db,
            catalog=self.catalog,
            columns=rendered_columns,
        )


class _SchemaCollector:
    def __init__(self, *, unqualified_columns: str) -> None:
        self._tables: dict[tuple[str | None, str | None, str], _MutableTable] = {}
        self._declared_schema_names: set[str] = set()
        self.statement_count = 0
        self.unqualified_columns = unqualified_columns

    def add_sql(
        self,
        sql: str,
        *,
        source: str | None,
        path: Path | None,
        fail_fast: bool = False,
    ) -> list[ConversionError]:
        current_db: str | None = None
        errors: list[ConversionError] = []
        for statement_index, statement_sql in enumerate(_split_sql_for_inference(sql), start=1):
            expression, statement_error = _parse_statement_for_inference(
                statement_sql,
                source=source,
                path=path,
                statement_index=statement_index,
            )
            if statement_error is not None:
                errors.append(statement_error)
                if fail_fast:
                    break
                continue
            if expression is None:
                continue
            statement_type = _infer_schema_statement_type(expression)
            if statement_type not in SUPPORTED_INFER_SCHEMA_STATEMENT_TYPES:
                errors.append(
                    ConversionError.from_exception(
                        path=path,
                        statement_index=statement_index,
                        error_type="unsupported_statement",
                        statement_type=statement_type,
                        statement_sql=statement_sql,
                        message=f"Statement type is not supported for infer-schema: {statement_type}",
                    )
                )
                if fail_fast:
                    break
                continue
            self.statement_count += 1
            current_db = self._ingest_expression(expression, current_db=current_db)

        return errors

    def render(self, *, default_type: str) -> list[InferredTable]:
        rendered_tables: list[InferredTable] = []
        for _, table in sorted(
            self._tables.items(),
            key=lambda item: tuple(part or "" for part in item[0]),
        ):
            rendered_table = table.render(default_type=default_type)
            if rendered_table.columns:
                rendered_tables.append(rendered_table)
        return rendered_tables

    @property
    def declared_schema_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._declared_schema_names, key=str.lower))

    def _ingest_expression(self, expression: exp.Expression, *, current_db: str | None) -> str | None:
        if isinstance(expression, exp.Use):
            database = _identifier_name(expression.this)
            return database or current_db

        create_kind = ""
        if isinstance(expression, exp.Create):
            create_kind = str(expression.args.get("kind", "")).upper()

        if create_kind in {"SCHEMA", "DATABASE"}:
            self._ingest_create_namespace(expression)
        elif create_kind == "TABLE":
            self._ingest_create_table(expression, current_db=current_db)
        elif create_kind == "INDEX":
            self._ingest_create_index(expression, current_db=current_db)
        elif isinstance(expression, exp.Insert):
            self._ingest_insert(expression, current_db=current_db)
        elif isinstance(expression, exp.Update):
            self._ingest_update(expression, current_db=current_db)

        scope = build_scope(expression)
        if scope is not None:
            for current_scope in scope.traverse():
                self._ingest_scope(current_scope, current_db=current_db)

        return current_db

    def _ingest_create_namespace(self, expression: exp.Create) -> None:
        schema_name = _schema_name_from_create(expression)
        if schema_name:
            self._declared_schema_names.add(schema_name)

    def _ingest_create_table(self, expression: exp.Create, *, current_db: str | None) -> None:
        schema = expression.this if isinstance(expression.this, exp.Schema) else None
        table_expression = schema.this if schema is not None else expression.this
        table = self._table_for_expression(table_expression, current_db=current_db)
        if schema is None:
            return

        for column_def in schema.expressions:
            if not isinstance(column_def, exp.ColumnDef):
                continue
            column_name = _identifier_name(column_def.this)
            if not column_name:
                continue
            column = table.column(column_name)
            column.add_evidence("ddl")
            data_type = column_def.args.get("kind")
            if isinstance(data_type, exp.DataType):
                column.add_explicit_type(data_type.sql())

    def _ingest_insert(self, expression: exp.Insert, *, current_db: str | None) -> None:
        schema = expression.this if isinstance(expression.this, exp.Schema) else None
        table_expression = schema.this if schema is not None else expression.this
        table = self._table_for_expression(table_expression, current_db=current_db)
        if schema is None:
            return

        for identifier in schema.expressions:
            column_name = _identifier_name(identifier)
            if not column_name:
                continue
            column = table.column(column_name)
            column.add_evidence("insert")

    def _ingest_update(self, expression: exp.Update, *, current_db: str | None) -> None:
        if not isinstance(expression.this, exp.Table):
            return

        table = self._table_for_expression(expression.this, current_db=current_db)
        for assignment in expression.expressions:
            if not isinstance(assignment, exp.EQ):
                continue
            column_name = _identifier_name(assignment.this)
            if not column_name:
                continue
            column = table.column(column_name)
            column.add_evidence("update")
            literal_hint = _literal_data_type(assignment.expression)
            if literal_hint is not None:
                column.add_inferred_type(literal_hint)

    def _ingest_create_index(self, expression: exp.Create, *, current_db: str | None) -> None:
        if not isinstance(expression.this, exp.Index):
            return
        table_expression = expression.this.args.get("table")
        if not isinstance(table_expression, exp.Table):
            return

        table = self._table_for_expression(table_expression, current_db=current_db)
        for column in expression.this.find_all(exp.Column):
            column_name = _identifier_name(column)
            if not column_name:
                continue
            table.column(column_name).add_evidence("index")

    def _ingest_scope(self, scope: Scope, *, current_db: str | None) -> None:
        physical_tables = [table for table in scope.tables if isinstance(table, exp.Table) and table.name]
        if not physical_tables:
            return

        for table in physical_tables:
            self._table_for_expression(table, current_db=current_db)

        scope_columns = _scope_columns(scope)
        scope_column_ids = {id(column) for column in scope_columns}
        for column in scope_columns:
            resolved = self._resolve_column(
                column,
                scope=scope,
                physical_tables=physical_tables,
                current_db=current_db,
            )
            if resolved is None:
                continue
            column_name = _identifier_name(column)
            if not column_name:
                continue
            table_key, evidence = resolved
            rendered_table = self._tables[table_key]
            rendered_column = rendered_table.column(column_name)
            rendered_column.add_evidence(evidence)

        for node in scope.expression.walk():
            self._ingest_type_hints(
                node,
                scope=scope,
                physical_tables=physical_tables,
                current_db=current_db,
                local_column_ids=scope_column_ids,
            )

    def _ingest_type_hints(
        self,
        node: exp.Expression,
        *,
        scope: Scope,
        physical_tables: list[exp.Table],
        current_db: str | None,
        local_column_ids: set[int],
    ) -> None:
        if isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)):
            self._apply_hint_to_node(
                node,
                inferred_type="DOUBLE",
                scope=scope,
                physical_tables=physical_tables,
                current_db=current_db,
                local_column_ids=local_column_ids,
            )
            return

        if isinstance(node, (exp.Cast, exp.TryCast)):
            data_type = node.args.get("to") or node.args.get("kind")
            if isinstance(data_type, exp.DataType):
                self._apply_hint_to_node(
                    node.this,
                    inferred_type=data_type.sql(),
                    scope=scope,
                    physical_tables=physical_tables,
                    current_db=current_db,
                    local_column_ids=local_column_ids,
                )
            return

        if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
            self._apply_comparison_hint(
                node.this,
                node.expression,
                scope=scope,
                physical_tables=physical_tables,
                current_db=current_db,
                local_column_ids=local_column_ids,
            )
            self._apply_comparison_hint(
                node.expression,
                node.this,
                scope=scope,
                physical_tables=physical_tables,
                current_db=current_db,
                local_column_ids=local_column_ids,
            )
            return

        if isinstance(node, exp.Func):
            if isinstance(node, exp.Anonymous):
                function_name = node.name.upper()
            else:
                function_name = node.sql_name().upper()
            if "GEOGRAPHY" in function_name or function_name.startswith("ST_"):
                self._apply_hint_to_node(
                    node,
                    inferred_type="GEOGRAPHY",
                    scope=scope,
                    physical_tables=physical_tables,
                    current_db=current_db,
                    local_column_ids=local_column_ids,
                )
                return

            if function_name in {"AVG", "SUM", "ROUND", "ABS", "STDDEV", "STDDEV_POP", "STDDEV_SAMP"}:
                self._apply_hint_to_node(
                    node,
                    inferred_type="DOUBLE",
                    scope=scope,
                    physical_tables=physical_tables,
                    current_db=current_db,
                    local_column_ids=local_column_ids,
                )

    def _apply_comparison_hint(
        self,
        candidate: exp.Expression,
        other_side: exp.Expression,
        *,
        scope: Scope,
        physical_tables: list[exp.Table],
        current_db: str | None,
        local_column_ids: set[int],
    ) -> None:
        literal_type = _literal_data_type(other_side)
        if literal_type is None:
            return
        self._apply_hint_to_node(
            candidate,
            inferred_type=literal_type,
            scope=scope,
            physical_tables=physical_tables,
            current_db=current_db,
            local_column_ids=local_column_ids,
        )

    def _apply_hint_to_node(
        self,
        node: exp.Expression,
        *,
        inferred_type: str,
        scope: Scope,
        physical_tables: list[exp.Table],
        current_db: str | None,
        local_column_ids: set[int],
    ) -> None:
        for column in node.find_all(exp.Column):
            if id(column) not in local_column_ids:
                continue
            resolved = self._resolve_column(
                column,
                scope=scope,
                physical_tables=physical_tables,
                current_db=current_db,
            )
            if resolved is None:
                continue
            column_name = _identifier_name(column)
            if not column_name:
                continue
            table_key, _ = resolved
            rendered_table = self._tables[table_key]
            rendered_table.column(column_name).add_inferred_type(inferred_type)

    def _resolve_column(
        self,
        column: exp.Column,
        *,
        scope: Scope,
        physical_tables: list[exp.Table],
        current_db: str | None,
    ) -> tuple[tuple[str | None, str | None, str], str] | None:
        source_by_alias = {
            alias: source
            for alias, source in scope.sources.items()
            if isinstance(source, exp.Table) and source.name
        }

        if column.table:
            source = source_by_alias.get(column.table)
            if source is None:
                return None
            return self._table_key(source, current_db=current_db), "query"

        if len(physical_tables) == 1:
            return self._table_key(physical_tables[0], current_db=current_db), "query_single_source"

        if self.unqualified_columns == "first-table" and physical_tables:
            return self._table_key(physical_tables[0], current_db=current_db), "query_unqualified"

        return None

    def _table_for_expression(self, table_expression: exp.Expression, *, current_db: str | None) -> _MutableTable:
        if not isinstance(table_expression, exp.Table) or not table_expression.name:
            raise ValueError("Expected a table expression with a table name")

        key = self._table_key(table_expression, current_db=current_db)
        if key not in self._tables:
            catalog, db, name = key
            self._tables[key] = _MutableTable(name=name, db=db, catalog=catalog)
        return self._tables[key]

    def _table_key(
        self,
        table_expression: exp.Table,
        *,
        current_db: str | None,
    ) -> tuple[str | None, str | None, str]:
        catalog = _canonicalize_identifier_name(table_expression.catalog or None)
        db = _canonicalize_identifier_name(table_expression.db or current_db or None)
        name = _canonicalize_identifier_name(table_expression.name)
        assert name is not None
        return catalog, db, name


def infer_schema(
    sql: str,
    *,
    source: str | None,
    path: Path | None,
    default_type: str,
    unqualified_columns: str,
) -> SchemaInferenceResult:
    collector = _SchemaCollector(unqualified_columns=unqualified_columns)
    errors = collector.add_sql(sql, source=source, path=path)
    return SchemaInferenceResult(
        tables=collector.render(default_type=default_type),
        errors=errors,
        statement_count=collector.statement_count,
        declared_schema_names=collector.declared_schema_names,
        input_count=1,
        successful_input_count=0 if errors else 1,
    )


def infer_schema_many(
    payloads: list[tuple[str, Path | None]],
    *,
    source: str | None,
    default_type: str,
    unqualified_columns: str,
    fail_fast: bool = False,
) -> SchemaInferenceResult:
    collector = _SchemaCollector(unqualified_columns=unqualified_columns)
    errors: list[ConversionError] = []
    input_count = 0
    successful_input_count = 0
    for sql, path in payloads:
        input_count += 1
        current_errors = collector.add_sql(sql, source=source, path=path, fail_fast=fail_fast)
        errors.extend(current_errors)
        if not current_errors:
            successful_input_count += 1
        if fail_fast and current_errors:
            break
    return SchemaInferenceResult(
        tables=collector.render(default_type=default_type),
        errors=errors,
        statement_count=collector.statement_count,
        declared_schema_names=collector.declared_schema_names,
        input_count=input_count,
        successful_input_count=successful_input_count,
    )


def render_schema_ddl(
    result: SchemaInferenceResult,
    *,
    target: str | None,
    if_not_exists: bool,
    create_schema: bool,
    create_user: bool,
    create_user_password: str | None,
    pretty: bool,
) -> str:
    lines: list[str] = []
    lines.extend(_schema_preamble(result=result, target=target, create_schema=create_schema))
    lines.extend(_oracle_user_preamble(result=result, target=target, create_user=create_user, password=create_user_password))
    low_confidence_tables = {
        table.qualified_name: [column.name for column in table.columns if column.confidence == "low"]
        for table in result.tables
    }
    low_confidence_tables = {
        table_name: columns for table_name, columns in low_confidence_tables.items() if columns
    }

    if low_confidence_tables:
        lines.append("-- Low-confidence columns were assigned from unqualified references:")
        for table_name in sorted(low_confidence_tables):
            column_list = ", ".join(sorted(low_confidence_tables[table_name], key=str.lower))
            lines.append(f"-- {table_name}: {column_list}")
        lines.append("")

    current_schema_name: str | None = None
    for index, table in enumerate(result.tables):
        if create_user and (target or "").lower() == "oracle" and table.db and table.db != current_schema_name:
            lines.append(f"ALTER SESSION SET CURRENT_SCHEMA = {table.db};")
            lines.append("")
            current_schema_name = table.db
        create_expression = exp.Create(
            this=exp.Schema(
                this=exp.to_table(table.qualified_name),
                expressions=[
                    exp.ColumnDef(
                        this=exp.to_identifier(column.name),
                        kind=_render_data_type(column=column, target=target),
                    )
                    for column in table.columns
                ],
            ),
            kind="TABLE",
            exists=if_not_exists,
        )
        statement = create_expression.sql(dialect=target, pretty=pretty)
        lines.append(f"{statement};")
        if index != len(result.tables) - 1:
            lines.append("")

    payload = "\n".join(lines)
    if payload and not payload.endswith("\n"):
        payload += "\n"
    return payload


def _oracle_user_preamble(
    *,
    result: SchemaInferenceResult,
    target: str | None,
    create_user: bool,
    password: str | None,
) -> list[str]:
    if (target or "").lower() != "oracle":
        return []
    if not password:
        return []

    schema_names = _renderable_schema_names(result=result, include_inferred=create_user)
    if not schema_names:
        return []

    escaped_password = password.replace('"', '""')
    lines = [
        "-- Oracle schema note: schema names map to users.",
        "-- You may still need to configure default tablespaces or quotas for the created users.",
        "",
    ]
    for schema_name in schema_names:
        lines.append(f'CREATE USER {schema_name} IDENTIFIED BY "{escaped_password}";')
        lines.append(f"GRANT CREATE SESSION, CREATE TABLE TO {schema_name};")
        lines.append("")
    return lines


def _schema_preamble(
    *,
    result: SchemaInferenceResult,
    target: str | None,
    create_schema: bool,
) -> list[str]:
    if not target or target.lower() == "oracle":
        return []

    schema_names = _renderable_schema_names(result=result, include_inferred=create_schema)
    if not schema_names:
        return []

    lines: list[str] = []
    for schema_name in schema_names:
        statement = exp.Create(
            this=exp.to_table(schema_name),
            kind="SCHEMA",
            exists=True,
        ).sql(dialect=target)
        lines.append(f"{statement};")
        lines.append("")
    return lines


def _renderable_schema_names(
    *,
    result: SchemaInferenceResult,
    include_inferred: bool,
) -> list[str]:
    schema_names = set(result.declared_schema_names)
    if include_inferred:
        schema_names.update(table.db for table in result.tables if table.db)
    return sorted(schema_names, key=str.lower)


def render_schema_json(result: SchemaInferenceResult) -> str:
    payload = {
        "statement_count": result.statement_count,
        "tables": [
            {
                "name": table.name,
                "db": table.db,
                "catalog": table.catalog,
                "qualified_name": table.qualified_name,
                "columns": [
                    {
                        "name": column.name,
                        "type": column.data_type,
                        "confidence": column.confidence,
                        "evidence": list(column.evidence),
                    }
                    for column in table.columns
                ],
            }
            for table in result.tables
        ],
    }
    import json

    return json.dumps(payload, indent=2) + "\n"


def _try_parse_expressions(
    sql: str,
    *,
    source: str | None,
) -> tuple[list[exp.Expression] | None, ParseError | TokenError | None]:
    try:
        return parse(sql, read=source), None
    except (ParseError, TokenError) as exc:
        return None, exc


def _first_parsed_expression(
    sql: str,
    *,
    source: str | None,
) -> tuple[exp.Expression | None, ParseError | TokenError | None]:
    expressions, error = _try_parse_expressions(sql, source=source)
    if expressions is None:
        return None, error
    for expression in expressions:
        if expression is not None:
            return expression, None
    return None, None


def _parse_statement_for_inference(
    sql: str,
    *,
    source: str | None,
    path: Path | None,
    statement_index: int,
) -> tuple[exp.Expression | None, ConversionError | None]:
    recovered_expression = _recover_create_table_expression(sql, source=source)
    if recovered_expression is not None:
        return recovered_expression, None

    expression, error = _first_parsed_expression(sql, source=source)
    if expression is not None:
        return expression, None

    normalized_sql = _normalize_sql_for_inference(sql)
    if normalized_sql != sql:
        recovered_expression = _recover_create_table_expression(normalized_sql, source=source)
        if recovered_expression is not None:
            return recovered_expression, None
        normalized_expression, normalized_error = _first_parsed_expression(normalized_sql, source=source)
        if normalized_expression is not None:
            return normalized_expression, None
        if normalized_error is not None:
            error = normalized_error

    if error is None:
        return None, None

    assert error is not None
    return None, ConversionError.from_exception(
        path=path,
        statement_index=statement_index,
        error_type="parse_error",
        statement_sql=sql,
        message=str(error),
    )


def _split_sql_for_inference(sql: str) -> list[str]:
    return [statement.strip().rstrip(";") for statement in sqlparse.split(sql) if statement.strip()]


def _normalize_sql_for_inference(sql: str) -> str:
    normalized = re.sub(r"\bIN\s*\(\s*\)", "IN (NULL)", sql, flags=re.IGNORECASE)
    normalized = re.sub(r'""([^"\n]*)""', lambda match: f'"{match.group(1)}"', normalized)
    normalized = re.sub(r"\$\{[^}]+\}", "madsql_param", normalized)
    normalized = re.sub(
        r"\$[A-Za-z_][A-Za-z0-9_]*",
        lambda match: f"madsql_{match.group(0)[1:]}",
        normalized,
    )
    return normalized


def _recover_create_table_expression(
    sql: str,
    *,
    source: str | None,
) -> exp.Expression | None:
    if not _looks_like_create_table(sql):
        return None

    normalized_sql = _normalize_create_table_for_inference(sql)
    if not normalized_sql:
        return None

    recovered_expression, _ = _first_parsed_expression(normalized_sql, source=source)
    if _is_create_table_expression(recovered_expression):
        return recovered_expression

    if source is None or source.lower() != "oracle":
        recovered_expression, _ = _first_parsed_expression(normalized_sql, source="oracle")
        if _is_create_table_expression(recovered_expression):
            return recovered_expression

    return None


def _looks_like_create_table(sql: str) -> bool:
    return bool(re.match(r"(?is)^\s*(?:--[^\n]*\n|\s|/\*.*?\*/)*CREATE\s+TABLE\b", sql))


def _normalize_create_table_for_inference(sql: str) -> str | None:
    statements = sqlparse.parse(sql)
    if not statements:
        return None

    statement = statements[0]
    if statement.get_type() != "CREATE":
        return None

    fragments: list[str] = []
    saw_create = False
    saw_table = False
    for token in statement.tokens:
        token_text = str(token)
        if not saw_create:
            if token_text.upper() == "CREATE":
                saw_create = True
                fragments.append(token_text)
            continue

        fragments.append(token_text)
        if token_text.upper() == "TABLE":
            saw_table = True
            continue
        if saw_table and isinstance(token, sqlparse.sql.Parenthesis):
            candidate_sql = "".join(fragments).strip().rstrip(";")
            normalized_candidate = _normalize_sql_for_inference(candidate_sql)
            return normalized_candidate

    return None


def _is_create_table_expression(expression: exp.Expression | None) -> bool:
    if not isinstance(expression, exp.Create):
        return False
    return str(expression.args.get("kind", "")).upper() == "TABLE"


def _infer_schema_statement_type(expression: exp.Expression) -> str:
    if isinstance(expression, (exp.Alter, exp.Drop)):
        kind = str(expression.args.get("kind", "")).upper()
        if kind:
            return f"{expression.key.upper()} {kind}"
        return expression.key.upper()
    if isinstance(expression, exp.Create):
        kind = str(expression.args.get("kind", "")).upper()
        if kind == "VIEW" and _is_materialized_view(expression):
            return "CREATE MATERIALIZED VIEW"
        if kind:
            return f"CREATE {kind}"
        return "CREATE"
    return expression.key.upper()


def _schema_name_from_create(expression: exp.Create) -> str | None:
    if not isinstance(expression.this, exp.Table):
        return None
    kind = str(expression.args.get("kind", "")).upper()
    if kind == "SCHEMA":
        return _canonicalize_identifier_name(expression.this.db or None)
    if kind == "DATABASE":
        return _canonicalize_identifier_name(expression.this.name or None)
    return None


def _is_materialized_view(expression: exp.Create) -> bool:
    properties = expression.args.get("properties")
    if not isinstance(properties, exp.Properties):
        return False
    return any(isinstance(property_expression, exp.MaterializedProperty) for property_expression in properties.expressions)


def _identifier_name(expression: exp.Expression | None) -> str | None:
    if expression is None:
        return None
    if isinstance(expression, exp.Column):
        return _canonicalize_identifier_name(expression.name or None)
    if isinstance(expression, exp.Identifier):
        return _canonicalize_identifier_name(expression.name or None)
    if hasattr(expression, "name"):
        value = getattr(expression, "name")
        if isinstance(value, str) and value:
            return _canonicalize_identifier_name(value)
    return None


def _canonicalize_identifier_name(value: str | None) -> str | None:
    if not value:
        return None
    if value.startswith("madsql_"):
        return value
    if value.startswith("$"):
        candidate = value[1:]
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", candidate):
            return f"madsql_{candidate}"
    return value


def _literal_data_type(expression: exp.Expression | None) -> str | None:
    if not isinstance(expression, exp.Literal):
        return None
    if expression.is_string:
        return "TEXT"
    return "DOUBLE"


def _most_common(counter: Counter[str]) -> str | None:
    if not counter:
        return None
    return sorted(counter.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _resolve_inferred_type(
    *,
    name: str,
    inferred_type_counts: Counter[str],
    default_type: str,
) -> tuple[str, str]:
    if inferred_type_counts.get("GEOGRAPHY"):
        return "GEOGRAPHY", "inferred"
    if inferred_type_counts.get("TEXT") and not (
        inferred_type_counts.get("DOUBLE") or inferred_type_counts.get("BIGINT")
    ):
        return "TEXT", "inferred"
    if inferred_type_counts.get("DOUBLE"):
        return "DOUBLE", "inferred"
    if inferred_type_counts.get("BIGINT"):
        return "BIGINT", "inferred"
    if name.lower() == "id" or name.lower().endswith("_id"):
        return "BIGINT", "inferred"
    return default_type.upper(), "default"


def _render_data_type(*, column: InferredColumn, target: str | None) -> exp.Expression:
    if target and target.lower() == "oracle" and column.data_type == "GEOGRAPHY":
        return exp.to_identifier("SDO_GEOMETRY")
    if target and target.lower() == "oracle" and column.type_source == "inferred" and column.data_type == "DOUBLE":
        return exp.DataType.build("NUMBER")
    return exp.DataType.build(column.data_type)


def _scope_columns(scope: Scope) -> list[exp.Column]:
    nested_column_ids: set[int] = set()
    child_scopes = [
        *scope.cte_scopes,
        *scope.derived_table_scopes,
        *scope.subquery_scopes,
        *scope.union_scopes,
        *scope.udtf_scopes,
    ]
    for child_scope in child_scopes:
        for column in child_scope.expression.find_all(exp.Column):
            nested_column_ids.add(id(column))

    return [column for column in scope.columns if id(column) not in nested_column_ids]
