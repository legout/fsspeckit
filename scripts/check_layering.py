#!/usr/bin/env python3
"""Check package import layering rules.

The canonical layering is documented in docs/architecture/0001-layering-rules.md.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "fsspeckit"

DISALLOWED_PREFIXES = {
    "common": ("fsspeckit.core", "fsspeckit.datasets", "fsspeckit.sql"),
    "core": ("fsspeckit.datasets", "fsspeckit.sql"),
    "core.ext": (),
    "datasets": ("fsspeckit.sql",),
}


def imported_modules(tree: ast.AST) -> list[tuple[int, str]]:
    """Return imported module names with line numbers."""
    imports: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend((node.lineno, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append((node.lineno, node.module))
    return imports


def package_for(relative_path: Path) -> str | None:
    first = relative_path.parts[0]
    if first not in DISALLOWED_PREFIXES:
        return None
    if first == "core" and len(relative_path.parts) > 1 and relative_path.parts[1] == "ext":
        return "core.ext"
    return first


def main() -> int:
    violations: list[str] = []
    for py_file in SRC.rglob("*.py"):
        if "__pycache__" in py_file.parts:
            continue
        relative = py_file.relative_to(SRC)
        package = package_for(relative)
        if package is None:
            continue
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for line, module in imported_modules(tree):
            for prefix in DISALLOWED_PREFIXES[package]:
                if module == prefix or module.startswith(f"{prefix}."):
                    violations.append(
                        f"{py_file.relative_to(ROOT)}:{line}: {package} must not import {module}"
                    )

    if violations:
        print("Import layering violations found:")
        print("\n".join(violations))
        return 1

    print("No import layering violations found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
