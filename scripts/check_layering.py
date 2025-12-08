#!/usr/bin/env python3
"""
Import Layering Check Script

This script verifies that the codebase follows proper architectural layering rules:
- fsspeckit.common can be imported by anyone
- fsspeckit.core can import from common (but not from datasets or sql)
- fsspeckit.datasets can import from common and core (but not from sql)
- fsspeckit.sql can import from common, core, and datasets

Violations will be reported as errors.
"""

import ast
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple


class LayeringChecker(ast.NodeVisitor):
    """Check import layering violations in Python code."""

    # Define allowed import paths for each package
    ALLOWED_IMPORTS = {
        "fsspeckit.core": {
            "fsspeckit.common",
            "fsspeckit.storage_options",
            "fsspec",
        },
        "fsspeckit.datasets": {
            "fsspeckit.common",
            "fsspeckit.core",
            "fsspeckit.storage_options",
            "fsspec",
        },
        "fsspeckit.sql": {
            "fsspeckit.common",
            "fsspeckit.core",
            "fsspeckit.datasets",
            "fsspeckit.storage_options",
            "fsspec",
        },
    }

    # Define violating imports (packages that should not import from higher-level packages)
    VIOLATIONS = {
        "fsspeckit.core": {"fsspeckit.datasets", "fsspeckit.sql"},
        "fsspeckit.datasets": {"fsspeckit.sql"},
    }

    def __init__(self, package_root: Path):
        self.package_root = package_root
        self.violations: List[Tuple[str, str, str, int]] = []  # (package, file, import, line)
        self.current_package: str = ""

    def visit_Module(self, node: ast.Module) -> None:
        """Extract package name from module path."""
        # Get the file path relative to package root
        file_path = Path(self.file_path).relative_to(self.package_root)

        # Determine package from file path
        parts = file_path.parts
        if len(parts) >= 2 and parts[0] == "src" and parts[1] == "fsspeckit":
            if len(parts) >= 3:
                # Determine package from directory structure
                if parts[2] in ["core", "datasets", "sql"]:
                    self.current_package = f"fsspeckit.{parts[2]}"
                elif parts[2] == "common":
                    self.current_package = "fsspeckit.common"

        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Check import-from statements."""
        if node.module is None:
            return

        module = node.module

        # Check if this is a violation
        if self.current_package in self.VIOLATIONS:
            for violation_package in self.VIOLATIONS[self.current_package]:
                if module.startswith(violation_package):
                    self.violations.append((
                        self.current_package,
                        self.file_path,
                        module,
                        node.lineno
                    ))
                    return

        self.generic_visit(node)

    def check_file(self, file_path: str) -> None:
        """Check a single file for layering violations."""
        self.file_path = file_path
        self.current_package = ""

        with open(file_path, "r", encoding="utf-8") as f:
            try:
                tree = ast.parse(f.read(), filename=file_path)
                self.visit(tree)
            except (SyntaxError, UnicodeDecodeError) as e:
                print(f"Warning: Could not parse {file_path}: {e}", file=sys.stderr)

    def check_directory(self, directory: Path) -> bool:
        """Recursively check all Python files in directory."""
        violations_found = False

        for py_file in directory.rglob("*.py"):
            # Skip __pycache__ and test files
            if "__pycache__" in str(py_file) or "test" in str(py_file):
                continue

            self.check_file(str(py_file))

        return len(self.violations) > 0


def main():
    """Main entry point."""
    package_root = Path(__file__).parent.parent / "src" / "fsspeckit"

    if not package_root.exists():
        print(f"Error: Package root not found at {package_root}")
        sys.exit(1)

    checker = LayeringChecker(package_root)
    has_violations = checker.check_directory(package_root)

    if checker.violations:
        print("\n❌ Import Layering Violations Found!\n")
        print("=" * 80)

        # Group violations by package
        by_package: Dict[str, List[Tuple[str, str, int]]] = {}
        for pkg, file, imp, line in checker.violations:
            by_package.setdefault(pkg, []).append((file, imp, line))

        for package, violations in sorted(by_package.items()):
            print(f"\n📦 Package: {package}")
            print("-" * 80)

            for file, imp, line in violations:
                # Make file path relative to package root
                rel_file = Path(file).relative_to(package_root)
                print(f"  ✗ Line {line:3d}: imports {imp}")
                print(f"    → File: {rel_file}")

        print("\n" + "=" * 80)
        print(f"\n❌ Found {len(checker.violations)} layering violations")
        print("\nLayering Rules:")
        print("  - fsspeckit.core   must NOT import from datasets or sql")
        print("  - fsspeckit.datasets must NOT import from sql")
        print("\nFix the violations above and run this script again.\n")
        sys.exit(1)
    else:
        print("✅ No import layering violations found!")
        print("\nLayering rules satisfied:")
        print("  ✓ fsspeckit.core does not import from datasets or sql")
        print("  ✓ fsspeckit.datasets does not import from sql")
        sys.exit(0)


if __name__ == "__main__":
    main()
