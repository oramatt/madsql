from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import re


_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


@dataclass(frozen=True)
class ConversionError:
    path: str | None
    statement_index: int
    error_type: str
    message: str
    statement_type: str | None = None
    statement_sql: str | None = None

    @classmethod
    def from_exception(
        cls,
        *,
        path: Path | None,
        statement_index: int,
        error_type: str,
        message: str,
        statement_type: str | None = None,
        statement_sql: str | None = None,
    ) -> "ConversionError":
        return cls(
            path=str(path) if path is not None else None,
            statement_index=statement_index,
            error_type=error_type,
            message=_clean_artifact_text(message),
            statement_type=statement_type,
            statement_sql=_clean_artifact_text(statement_sql),
        )

    def to_dict(self) -> dict[str, str | int | None]:
        return asdict(self)


def _clean_artifact_text(value: str | None) -> str | None:
    if value is None:
        return None
    return _ANSI_ESCAPE_RE.sub("", value)
