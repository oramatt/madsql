from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import glob
import os


@dataclass(frozen=True)
class InputFile:
    path: Path
    relative_path: Path


def read_utf8(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def expand_inputs(raw_inputs: list[str]) -> list[InputFile]:
    files: dict[Path, InputFile] = {}
    cwd = Path.cwd()

    for raw in raw_inputs:
        matches = glob.glob(raw, recursive=True)
        if not matches:
            path = Path(raw)
            if path.exists():
                matches = [raw]
            else:
                raise FileNotFoundError(f"Input path not found: {raw}")

        for match in matches:
            matched_path = Path(match)
            if matched_path.is_dir():
                root_dir = matched_path.resolve()
                for child in sorted(matched_path.rglob("*")):
                    if child.is_file():
                        resolved = child.resolve()
                        relative = _relative_from_input_root(resolved, root_dir)
                        files[resolved] = InputFile(path=resolved, relative_path=relative)
                continue

            if matched_path.is_file():
                resolved = matched_path.resolve()
                relative = _relative_output_path(resolved, cwd)
                files[resolved] = InputFile(path=resolved, relative_path=relative)
                continue

            raise FileNotFoundError(f"Input path not found: {raw}")

    return [files[path] for path in sorted(files, key=lambda item: item.as_posix())]


def ensure_out_path(out: str | None) -> Path | None:
    if out is None:
        return None
    return Path(out)


def write_text(path: Path, content: str, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file: {path}")

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    tmp_path.write_text(content, encoding="utf-8")
    tmp_path.replace(path)


def _relative_output_path(path: Path, cwd: Path) -> Path:
    try:
        return path.relative_to(cwd)
    except ValueError:
        return Path("_external", *_external_parts(path))


def _relative_from_input_root(path: Path, root_dir: Path) -> Path:
    return Path(root_dir.name) / path.relative_to(root_dir)


def _external_parts(path: Path) -> list[str]:
    parts: list[str] = []
    for index, part in enumerate(path.parts):
        if index == 0 and part in {path.anchor, os.sep, "\\"}:
            continue
        if part == path.anchor:
            continue
        parts.append(part.replace(":", ""))
    return parts or [path.name]
