#!/usr/bin/env python3
"""
Example Runner Script

This script provides a convenient way to run and validate fsspeckit examples.
It includes options for running specific examples, categories, or the complete learning path.

Usage:
    python run_examples.py --list                    # List all examples
    python run_examples.py --category getting_started  # Run category
    python run_examples.py --file path/to/example.py  # Run specific file
    python run_examples.py --learning-path            # Run complete learning path
    python run_examples.py --validate                 # Validate all examples
"""

import argparse
import asyncio
import importlib.util
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
import traceback

# Get the examples directory (where this script is located)
EXAMPLES_DIR = Path(__file__).parent


EXAMPLE_CATEGORIES = {
    "getting_started": {
        "path": "datasets/getting_started",
        "description": "Beginner-friendly tutorials",
        "examples": [
            "01_duckdb_basics.py",
            "02_pyarrow_basics.py",
            "03_simple_merges.py",
            "04_pyarrow_merges.py",
        ],
    },
    "workflows": {
        "path": "datasets/workflows",
        "description": "Intermediate workflow examples",
        "examples": ["cloud_datasets.py", "performance_optimization.py"],
    },
    "sql": {
        "path": "sql",
        "description": "SQL operations and filters",
        "examples": [
            "sql_filter_basic.py",
            "sql_filter_advanced.py",
            "cross_platform_filters.py",
        ],
    },
    "schema": {
        "path": "datasets/schema",
        "description": "Schema management",
        "examples": [
            "schema_basics.py",
            "schema_unification.py",
            "type_optimization.py",
        ],
    },
    "common": {
        "path": "common",
        "description": "Common utilities",
        "examples": [
            "logging_setup.py",
            "parallel_processing.py",
            "type_conversion.py",
        ],
    },
    "batch_processing": {
        "path": "batch_processing",
        "description": "Batch processing patterns",
        "examples": [
            "batch_processing_example.py",
            "batch_processing_example_mamo.py",
            "generate_batched_data.py",
        ],
    },
    "caching": {
        "path": "caching",
        "description": "Caching strategies",
        "examples": ["caching_example.py", "caching_example_mamo.py", "setup_data.py"],
    },
    "dir_file_system": {
        "path": "dir_file_system",
        "description": "Directory and filesystem operations",
        "examples": ["dir_file_system_example.py", "dir_file_system_example_mamo.py"],
    },
    "read_folder": {
        "path": "read_folder",
        "description": "Data ingestion patterns",
        "examples": [
            "create_dataset.py",
            "read_folder_example.py",
            "read_folder_example_mamo.py",
        ],
    },
    "storage_options": {
        "path": "storage_options",
        "description": "Cloud storage configuration",
        "examples": ["storage_options_example.py", "storage_options_example_mamo.py"],
    },
}

# Learning path order
LEARNING_PATH_ORDER = [
    "getting_started",
    "workflows",
    "sql",
    "schema",
    "common",
    "batch_processing",
    "caching",
    "dir_file_system",
    "read_folder",
    "storage_options",
]

# Examples that require special handling (async, long runtime, etc.)
SPECIAL_EXAMPLES = {}


def list_examples():
    """List all available examples by category."""
    print("📚 Available fsspeckit Examples")
    print("=" * 60)

    for category, info in EXAMPLE_CATEGORIES.items():
        print(f"\n📂 {category}: {info['description']}")
        print(f"   Path: {info['path']}")
        for example in info["examples"]:
            example_path = Path(info["path"]) / example
            if example_path.exists():
                print(f"   ✅ {example}")
            else:
                print(f"   ❌ {example} (missing)")

    print(f"\n🎯 Learning Path Order:")
    for i, category in enumerate(LEARNING_PATH_ORDER, 1):
        print(f"   {i}. {category}")


def run_example_file(file_path: str, timeout: int = 120) -> Dict[str, Any]:
    """Run a single example file."""
    file_path = Path(file_path)

    # If the path is not absolute, resolve it relative to examples directory
    if not file_path.is_absolute():
        file_path = EXAMPLES_DIR / file_path

    if not file_path.exists():
        return {
            "success": False,
            "error": f"File not found: {file_path}",
            "duration": 0,
        }

    print(f"🚀 Running: {file_path.name}")
    start_time = time.time()

    try:
        # Check if this is a special example
        special_info = SPECIAL_EXAMPLES.get(
            str(file_path.relative_to(EXAMPLES_DIR)), {}
        )

        if special_info.get("async", False):
            # Run async example
            return run_async_example(file_path, special_info.get("timeout", timeout))
        else:
            # Run regular sync example
            return run_sync_example(file_path, timeout)

    except Exception as e:
        duration = time.time() - start_time
        print(f"❌ Failed: {file_path.name}")
        print(f"   Error: {str(e)}")

        return {"success": False, "error": str(e), "duration": duration}


def run_sync_example(file_path: Path, timeout: int) -> Dict[str, Any]:
    """Run a synchronous example."""
    start_time = time.time()

    try:
        # Load and run the module
        spec = importlib.util.spec_from_file_location("example_module", file_path)
        module = importlib.util.module_from_spec(spec)

        # Add the example's directory to sys.path for imports
        example_dir = file_path.parent
        if str(example_dir) not in sys.path:
            sys.path.insert(0, str(example_dir))

        # Execute the module
        spec.loader.exec_module(module)

        # Look for and run main function if it exists
        if hasattr(module, "main"):
            module.main()

        duration = time.time() - start_time
        print(f"✅ Completed: {file_path.name} ({duration:.2f}s)")

        return {"success": True, "duration": duration}

    except Exception as e:
        duration = time.time() - start_time
        print(f"❌ Failed: {file_path.name} ({duration:.2f}s)")
        print(f"   Error: {str(e)}")

        return {"success": False, "error": str(e), "duration": duration}


def run_async_example(file_path: Path, timeout: int) -> Dict[str, Any]:
    """Run an asynchronous example."""
    start_time = time.time()

    try:
        # Load the module
        spec = importlib.util.spec_from_file_location("example_module", file_path)
        module = importlib.util.module_from_spec(spec)

        # Add the example's directory to sys.path for imports
        example_dir = file_path.parent
        if str(example_dir) not in sys.path:
            sys.path.insert(0, str(example_dir))

        # Execute the module
        spec.loader.exec_module(module)

        # Run the async main function
        if hasattr(module, "main"):
            # Run with timeout
            try:
                asyncio.run(asyncio.wait_for(module.main(), timeout=timeout))
            except asyncio.TimeoutError:
                duration = time.time() - start_time
                print(f"⏰ Timeout: {file_path.name} ({timeout}s limit)")

                return {
                    "success": False,
                    "error": f"Timeout after {timeout}s",
                    "duration": duration,
                }

        duration = time.time() - start_time
        print(f"✅ Completed: {file_path.name} ({duration:.2f}s)")

        return {"success": True, "duration": duration}

    except Exception as e:
        duration = time.time() - start_time
        print(f"❌ Failed: {file_path.name} ({duration:.2f}s)")
        print(f"   Error: {str(e)}")

        return {"success": False, "error": str(e), "duration": duration}


def run_category(category_name: str) -> Dict[str, Any]:
    """Run all examples in a category."""
    if category_name not in EXAMPLE_CATEGORIES:
        print(f"❌ Unknown category: {category_name}")
        print(f"Available categories: {', '.join(EXAMPLE_CATEGORIES.keys())}")
        return {"success": False, "error": f"Unknown category: {category_name}"}

    category_info = EXAMPLE_CATEGORIES[category_name]
    category_path = Path(category_info["path"])

    print(f"\n📂 Running category: {category_name}")
    print(f"   Description: {category_info['description']}")
    print(f"   Path: {category_path}")

    results = {
        "category": category_name,
        "total_examples": len(category_info["examples"]),
        "successful": 0,
        "failed": 0,
        "total_duration": 0,
        "examples": [],
    }

    for example_file in category_info["examples"]:
        example_path = category_path / example_file

        if example_path.exists():
            result = run_example_file(example_path)
            results["examples"].append(
                {
                    "file": example_file,
                    "success": result["success"],
                    "duration": result["duration"],
                    "error": result.get("error"),
                }
            )

            if result["success"]:
                results["successful"] += 1
            else:
                results["failed"] += 1

            results["total_duration"] += result["duration"]
        else:
            print(f"❌ Missing: {example_file}")
            results["examples"].append(
                {
                    "file": example_file,
                    "success": False,
                    "duration": 0,
                    "error": "File not found",
                }
            )
            results["failed"] += 1

    # Print summary
    print(f"\n📊 Category Summary: {category_name}")
    print(f"   Total examples: {results['total_examples']}")
    print(f"   Successful: {results['successful']}")
    print(f"   Failed: {results['failed']}")
    print(f"   Total duration: {results['total_duration']:.2f}s")

    return results


def run_learning_path(interactive: bool = False) -> Dict[str, Any]:
    """Run the complete learning path."""
    print("\n🎓 Running Complete Learning Path")
    print("=" * 60)

    results = {
        "total_categories": len(LEARNING_PATH_ORDER),
        "category_results": [],
        "overall_successful": 0,
        "overall_failed": 0,
        "total_duration": 0,
    }

    for i, category_name in enumerate(LEARNING_PATH_ORDER, 1):
        print(f"\n{'=' * 20} Stage {i}/{len(LEARNING_PATH_ORDER)} {'=' * 20}")

        category_result = run_category(category_name)
        results["category_results"].append(category_result)

        results["overall_successful"] += category_result["successful"]
        results["overall_failed"] += category_result["failed"]
        results["total_duration"] += category_result["total_duration"]

        # Ask user if they want to continue (only in interactive mode)
        if i < len(LEARNING_PATH_ORDER) and interactive:
            try:
                response = input(f"\nContinue to next stage? (Y/n): ").strip().lower()
                if response and response != "y":
                    print("Learning path interrupted by user")
                    break
            except (EOFError, KeyboardInterrupt):
                print("\nLearning path interrupted")
                break
        elif i < len(LEARNING_PATH_ORDER) and not interactive:
            print(f"\n⏭️  Continuing to next stage automatically...")

    # Print final summary
    print(f"\n🎓 Learning Path Summary")
    print("=" * 60)
    print(
        f"   Categories completed: {len(results['category_results'])}/{results['total_categories']}"
    )
    print(
        f"   Total examples: {results['overall_successful'] + results['overall_failed']}"
    )
    print(f"   Successful: {results['overall_successful']}")
    print(f"   Failed: {results['overall_failed']}")
    print(
        f"   Total duration: {results['total_duration']:.2f}s ({results['total_duration'] / 60:.1f}m)"
    )

    success_rate = (
        results["overall_successful"]
        / (results["overall_successful"] + results["overall_failed"])
        * 100
    )
    print(f"   Success rate: {success_rate:.1f}%")

    return results


def validate_examples() -> Dict[str, Any]:
    """Validate that all examples exist and can be imported."""
    print("\n🔍 Validating Examples")
    print("=" * 60)

    results = {
        "total_examples": 0,
        "valid_examples": 0,
        "missing_examples": 0,
        "import_errors": 0,
        "details": [],
    }

    for category_name, category_info in EXAMPLE_CATEGORIES.items():
        print(f"\n📂 Validating category: {category_name}")

        for example_file in category_info["examples"]:
            example_path = Path(category_info["path"]) / example_file
            results["total_examples"] += 1

            example_result = {
                "category": category_name,
                "file": example_file,
                "path": str(example_path),
                "exists": example_path.exists(),
                "importable": False,
                "error": None,
            }

            if example_path.exists():
                try:
                    # Try to import the module
                    spec = importlib.util.spec_from_file_location(
                        "validation_module", example_path
                    )
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        example_result["importable"] = True
                        print(f"   ✅ {example_file}")
                        results["valid_examples"] += 1
                    else:
                        print(f"   ⚠️ {example_file} (not importable)")
                        results["import_errors"] += 1

                except Exception as e:
                    print(f"   ❌ {example_file} (import error: {str(e)})")
                    example_result["error"] = str(e)
                    results["import_errors"] += 1
            else:
                print(f"   ❌ {example_file} (missing)")
                results["missing_examples"] += 1

            results["details"].append(example_result)

    # Print summary
    print(f"\n📊 Validation Summary")
    print("=" * 60)
    print(f"   Total examples: {results['total_examples']}")
    print(f"   Valid examples: {results['valid_examples']}")
    print(f"   Missing examples: {results['missing_examples']}")
    print(f"   Import errors: {results['import_errors']}")

    validation_rate = results["valid_examples"] / results["total_examples"] * 100
    print(f"   Validation rate: {validation_rate:.1f}%")

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run and validate fsspeckit examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --list                           # List all available examples
  %(prog)s --category getting_started       # Run beginner examples
  %(prog)s --file datasets/getting_started/01_duckdb_basics.py
  %(prog)s --learning-path                  # Run complete learning path
  %(prog)s --validate                       # Validate all examples
        """,
    )

    parser.add_argument(
        "--list", "-l", action="store_true", help="List all available examples"
    )

    parser.add_argument(
        "--category",
        "-c",
        choices=list(EXAMPLE_CATEGORIES.keys()),
        help="Run all examples in a category",
    )

    parser.add_argument("--file", "-f", help="Run a specific example file")

    parser.add_argument(
        "--learning-path",
        "-p",
        action="store_true",
        help="Run the complete learning path",
    )

    parser.add_argument(
        "--validate",
        "-v",
        action="store_true",
        help="Validate that all examples exist and can be imported",
    )

    parser.add_argument(
        "--timeout",
        "-t",
        type=int,
        default=120,
        help="Timeout for individual examples (seconds, default: 120)",
    )

    parser.add_argument(
        "--interactive",
        "-i",
        action="store_true",
        help="Enable interactive prompts (e.g., learning path confirmation)",
    )

    args = parser.parse_args()

    # If no arguments provided, show help
    if not any(
        [args.list, args.category, args.file, args.learning_path, args.validate]
    ):
        parser.print_help()
        return

    try:
        if args.list:
            list_examples()

        elif args.category:
            run_category(args.category)

        elif args.file:
            result = run_example_file(args.file, args.timeout)
            if not result["success"]:
                sys.exit(1)

        elif args.learning_path:
            run_learning_path(interactive=args.interactive)

        elif args.validate:
            validate_examples()

    except KeyboardInterrupt:
        print("\n\n🛑 Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
