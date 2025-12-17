#!/usr/bin/env python3
"""
Test Suite for fsspeckit Examples

This script provides automated testing for the fsspeckit examples to ensure
they work correctly and follow best practices.

Usage:
    python test_examples.py                    # Run all tests
    python test_examples.py --category sql    # Test specific category
    python test_examples.py --syntax-only     # Only check syntax
"""

import ast
import argparse
import importlib.util
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import subprocess


class ExampleTester:
    """Test suite for fsspeckit examples."""

    def __init__(self):
        self.results = {
            "syntax_tests": {"passed": 0, "failed": 0, "details": []},
            "import_tests": {"passed": 0, "failed": 0, "details": []},
            "runtime_tests": {"passed": 0, "failed": 0, "details": []},
            "style_tests": {"passed": 0, "failed": 0, "details": []},
        }

    def test_syntax(self, file_path: Path) -> Tuple[bool, str]:
        """Test if Python file has valid syntax."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Parse the AST to check syntax
            ast.parse(content)
            return True, "Syntax valid"

        except SyntaxError as e:
            return False, f"Syntax error: {e}"
        except Exception as e:
            return False, f"Error reading file: {e}"

    def test_import(self, file_path: Path) -> Tuple[bool, str]:
        """Test if example can be imported."""
        try:
            spec = importlib.util.spec_from_file_location("test_module", file_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return True, "Import successful"
            else:
                return False, "Could not create module spec"

        except ImportError as e:
            return False, f"Import error: {e}"
        except Exception as e:
            return False, f"Import failed: {e}"

    def test_runtime(self, file_path: Path, timeout: int = 30) -> Tuple[bool, str]:
        """Test if example runs without errors (quick execution)."""
        try:
            # Run the script with timeout
            result = subprocess.run(
                [sys.executable, str(file_path)],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=file_path.parent,
            )

            if result.returncode == 0:
                return True, "Runtime successful"
            else:
                return False, f"Runtime error: {result.stderr.strip()}"

        except subprocess.TimeoutExpired:
            return False, f"Runtime timeout after {timeout}s"
        except Exception as e:
            return False, f"Runtime test failed: {e}"

    def test_style(self, file_path: Path) -> Tuple[bool, str]:
        """Test basic code style (lenient for examples)."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            issues = []

            # Only check for basic syntax issues, not strict style
            # Examples might not follow all style conventions

            # Check if file has any content
            if not content.strip():
                return False, "File is empty"

            # Check if file looks like Python (has basic Python keywords)
            python_keywords = [
                "def ",
                "class ",
                "import ",
                "from ",
                "if ",
                "for ",
                "while ",
            ]
            has_python_content = any(keyword in content for keyword in python_keywords)

            if not has_python_content:
                issues.append("File doesn't appear to contain Python code")

            # For examples, we only warn about major issues, don't fail
            if issues:
                return True, f"Basic style check passed (warnings: {', '.join(issues)})"
            else:
                return True, "Basic style check passed"

        except Exception as e:
            return False, f"Style test failed: {e}"

    def test_file(
        self, file_path: Path, test_types: List[str] = None
    ) -> Dict[str, Any]:
        """Run all tests on a single file."""
        if test_types is None:
            test_types = [
                "syntax",
                "import",
                "style",
            ]  # Skip runtime by default for speed

        results = {"file": str(file_path), "tests": {}}

        for test_type in test_types:
            if test_type == "syntax":
                passed, message = self.test_syntax(file_path)
                self.results["syntax_tests"]["details"].append(
                    {"file": str(file_path), "passed": passed, "message": message}
                )
                if passed:
                    self.results["syntax_tests"]["passed"] += 1
                else:
                    self.results["syntax_tests"]["failed"] += 1
                results["tests"]["syntax"] = {"passed": passed, "message": message}

            elif test_type == "import":
                passed, message = self.test_import(file_path)
                self.results["import_tests"]["details"].append(
                    {"file": str(file_path), "passed": passed, "message": message}
                )
                if passed:
                    self.results["import_tests"]["passed"] += 1
                else:
                    self.results["import_tests"]["failed"] += 1
                results["tests"]["import"] = {"passed": passed, "message": message}

            elif test_type == "runtime":
                passed, message = self.test_runtime(file_path, timeout=smoke_timeout)
                self.results["runtime_tests"]["details"].append(
                    {"file": str(file_path), "passed": passed, "message": message}
                )
                if passed:
                    self.results["runtime_tests"]["passed"] += 1
                else:
                    self.results["runtime_tests"]["failed"] += 1
                results["tests"]["runtime"] = {"passed": passed, "message": message}

            elif test_type == "style":
                passed, message = self.test_style(file_path)
                self.results["style_tests"]["details"].append(
                    {"file": str(file_path), "passed": passed, "message": message}
                )
                if passed:
                    self.results["style_tests"]["passed"] += 1
                else:
                    self.results["style_tests"]["failed"] += 1
                results["tests"]["style"] = {"passed": passed, "message": message}

        return results

    def test_category(
        self, category_path: Path, test_types: List[str] = None
    ) -> Dict[str, Any]:
        """Test all examples in a category."""
        if not category_path.exists():
            return {"error": f"Category path not found: {category_path}"}

        python_files = list(category_path.glob("*.py"))
        if not python_files:
            return {"error": f"No Python files found in {category_path}"}

        results = {
            "category": str(category_path),
            "files_tested": len(python_files),
            "file_results": [],
        }

        for file_path in python_files:
            file_result = self.test_file(file_path, test_types)
            results["file_results"].append(file_result)

        return results

    def get_summary(self) -> Dict[str, Any]:
        """Get test summary."""
        total_tests = 0
        total_passed = 0

        for test_type, test_results in self.results.items():
            total_tests += test_results["passed"] + test_results["failed"]
            total_passed += test_results["passed"]

        success_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0

        return {
            "total_tests": total_tests,
            "total_passed": total_passed,
            "total_failed": total_tests - total_passed,
            "success_rate": success_rate,
            "test_breakdown": {
                test_type: {
                    "passed": results["passed"],
                    "failed": results["failed"],
                    "total": results["passed"] + results["failed"],
                }
                for test_type, results in self.results.items()
            },
        }


def get_all_example_files() -> List[Path]:
    """Get all Python example files."""
    examples_dir = Path(".")
    python_files = []

    # Recursively find all Python files in example directories
    for pattern in ["*.py", "**/*.py"]:
        python_files.extend(examples_dir.glob(pattern))

    # Filter out non-example files
    exclude_patterns = ["test_", "run_examples.py", "__pycache__"]
    python_files = [
        f
        for f in python_files
        if not any(pattern in f.name for pattern in exclude_patterns)
        and f.parent != Path(".")  # Exclude root directory
    ]

    return sorted(python_files)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Test fsspeckit examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--category", "-c", help="Test specific category (path to directory)"
    )

    parser.add_argument("--file", "-f", help="Test specific file")

    parser.add_argument(
        "--syntax-only", action="store_true", help="Only run syntax tests"
    )
    parser.add_argument(
        "--include-runtime", action="store_true", help="Include runtime tests (slower)"
    )

    parser.add_argument(
        "--smoke-run",
        action="store_true",
        help="Quick smoke test with very short timeouts (5s per example)",
    )
    parser.add_argument(
        "--timeout",
        "-t",
        type=int,
        default=30,
        help="Timeout for runtime tests (seconds)",
    )

    args = parser.parse_args()

    # Determine test types
    test_types = ["syntax", "import", "style"]
    if args.include_runtime:
        test_types.append("runtime")

    if args.syntax_only:
        test_types = ["syntax"]

    # Handle smoke-run mode
    smoke_timeout = 5 if args.smoke_run else args.timeout

    tester = ExampleTester()

    print("🧪 fsspeckit Examples Test Suite")
    print("=" * 60)

    try:
        if args.file:
            # Test specific file
            file_path = Path(args.file)
            if not file_path.exists():
                print(f"❌ File not found: {file_path}")
                sys.exit(1)

            result = tester.test_file(file_path, test_types)
            print(f"\n📋 Results for {file_path.name}")
            for test_type, test_result in result["tests"].items():
                status = "✅" if test_result["passed"] else "❌"
                print(f"   {status} {test_type.title()}: {test_result['message']}")

        elif args.category:
            # Test specific category
            category_path = Path(args.category)
            result = tester.test_category(category_path, test_types)

            if "error" in result:
                print(f"❌ {result['error']}")
                sys.exit(1)

            print(f"\n📂 Category Results: {category_path.name}")
            for file_result in result["file_results"]:
                file_name = Path(file_result["file"]).name
                print(f"\n   📄 {file_name}")
                for test_type, test_result in file_result["tests"].items():
                    status = "✅" if test_result["passed"] else "❌"
                    print(
                        f"      {status} {test_type.title()}: {test_result['message']}"
                    )

        else:
            # Test all examples
            all_files = get_all_example_files()
            print(f"🔍 Found {len(all_files)} example files")

            for file_path in all_files:
                print(f"   Testing: {file_path}")
                tester.test_file(file_path, test_types)

        # Print summary
        summary = tester.get_summary()
        print(f"\n📊 Test Summary")
        print("=" * 60)
        print(f"   Total tests: {summary['total_tests']}")
        print(f"   Passed: {summary['total_passed']}")
        print(f"   Failed: {summary['total_failed']}")
        print(f"   Success rate: {summary['success_rate']:.1f}%")

        print(f"\n📈 Test Breakdown:")
        for test_type, breakdown in summary["test_breakdown"].items():
            status = "✅" if breakdown["failed"] == 0 else "❌"
            print(f"   {status} {test_type.replace('_', ' ').title()}:")
            print(f"      Passed: {breakdown['passed']}/{breakdown['total']}")
            if breakdown["failed"] > 0:
                print(f"      Failed: {breakdown['failed']}/{breakdown['total']}")

        # Exit with error code if any tests failed
        if summary["total_failed"] > 0:
            print(f"\n❌ {summary['total_failed']} test(s) failed")
            sys.exit(1)
        else:
            print(f"\n✅ All tests passed!")

    except KeyboardInterrupt:
        print("\n\n🛑 Testing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
