"""
Logging Setup Example

This example demonstrates how to configure and use the logging utilities
from fsspeckit.common for effective application logging.

The example covers:
1. Basic logging configuration
2. Multiple output formats (console, file, JSON)
3. Log levels and filtering
4. Structured logging with context
5. Performance considerations for data processing
6. Integration with fsspeckit operations

This example is designed for users who need to implement proper logging
in their data processing applications.
"""

from __future__ import annotations

import tempfile
import time
from datetime import datetime
from pathlib import Path

from fsspeckit.common.logging import setup_logging, get_logger


def demonstrate_basic_logging():
    """Demonstrate basic logging setup and usage."""

    print("📝 Basic Logging Setup")

    # Basic configuration with console output
    setup_logging(
        level="INFO",
        format_string="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = get_logger("basic_example")

    print("✅ Logging configured")

    # Test different log levels
    logger.debug("This is a debug message (won't show with INFO level)")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")


def demonstrate_structured_logging():
    """Demonstrate structured logging with extra context."""

    print("\n🏗️  Structured Logging with Context")

    # Setup logging with JSON format for structured output
    setup_logging(
        level="INFO",
        format_string="%(message)s",  # Simple format for structured messages
    )
    logger = get_logger("structured_example")

    print("✅ Structured logging configured")

    # Create structured log messages
    logger.info(
        "Data processing started",
        extra={
            "job_id": "job_12345",
            "dataset": "sales_data_2024",
            "record_count": 50000,
            "start_time": datetime.now().isoformat(),
            "processing_type": "batch",
        },
    )

    logger.warning(
        "Performance threshold exceeded",
        extra={
            "job_id": "job_12345",
            "warning_type": "performance",
            "threshold_seconds": 300,
            "actual_seconds": 450,
            "records_processed": 25000,
            "records_per_second": 55.6,
        },
    )

    logger.error(
        "Data validation failed",
        extra={
            "job_id": "job_12345",
            "error_type": "validation",
            "failed_records": 125,
            "error_details": {
                "missing_values": 87,
                "invalid_types": 38,
                "out_of_range": 0,
            },
            "affected_columns": ["email", "phone", "postal_code"],
        },
    )


def demonstrate_contextual_logging():
    """Demonstrate logging with execution context."""

    print("\n🎯 Contextual Logging")

    # Setup logging for a data processing context
    setup_logging(
        level="DEBUG",
        format_string="%(asctime)s [%(levelname)s] %(name)s:%(funcName)s - %(message)s",
    )
    logger = get_logger("contextual_example")

    # Simulate a data processing workflow with context
    def process_dataset(dataset_name: str, batch_size: int):
        """Simulate processing a dataset with contextual logging."""

        logger.info(
            f"Starting dataset processing",
            extra={
                "dataset_name": dataset_name,
                "batch_size": batch_size,
                "workflow": "data_processing",
            },
        )

        # Simulate processing phases
        phases = ["validation", "transformation", "loading"]

        for phase in phases:
            logger.debug(
                f"Entering phase: {phase}",
                extra={
                    "dataset_name": dataset_name,
                    "phase": phase,
                    "workflow": "data_processing",
                },
            )

            # Simulate work with timing
            start_time = time.time()

            if phase == "validation":
                time.sleep(0.1)  # Simulate validation work
                logger.info(
                    f"Validation completed",
                    extra={
                        "dataset_name": dataset_name,
                        "phase": phase,
                        "validation_errors": 0,
                        "records_validated": batch_size,
                    },
                )

            elif phase == "transformation":
                time.sleep(0.2)  # Simulate transformation work
                logger.warning(
                    f"Some records required type conversion",
                    extra={
                        "dataset_name": dataset_name,
                        "phase": phase,
                        "converted_records": int(batch_size * 0.05),
                        "conversion_types": ["string_to_int", "date_format"],
                    },
                )

            elif phase == "loading":
                time.sleep(0.15)  # Simulate loading work
                logger.info(
                    f"Data loaded successfully",
                    extra={
                        "dataset_name": dataset_name,
                        "phase": phase,
                        "destination": "data_warehouse",
                        "records_loaded": batch_size,
                    },
                )

            phase_time = time.time() - start_time
            logger.debug(
                f"Phase completed in {phase_time:.2f}s",
                extra={
                    "dataset_name": dataset_name,
                    "phase": phase,
                    "duration_seconds": phase_time,
                },
            )

        logger.info(
            f"Dataset processing completed",
            extra={
                "dataset_name": dataset_name,
                "total_phases": len(phases),
                "workflow": "data_processing",
                "status": "success",
            },
        )

    # Run the processing workflow
    process_dataset("customer_data_2024", 1000)
    process_dataset("transaction_data_Q1", 5000)


def demonstrate_performance_logging():
    """Demonstrate performance-focused logging patterns."""

    print("\n⚡ Performance Logging Patterns")

    setup_logging(level="INFO", format_string="%(asctime)s - %(message)s")
    logger = get_logger("performance_example")

    def timing_logger(operation_name: str):
        """Context manager for timing operations."""
        import contextlib

        @contextlib.contextmanager
        def _timer():
            start_time = time.time()
            logger.info(f"Starting: {operation_name}")
            try:
                yield
            finally:
                duration = time.time() - start_time
                logger.info(
                    f"Completed: {operation_name}",
                    extra={
                        "operation": operation_name,
                        "duration_seconds": duration,
                        "performance_metric": True,
                    },
                )

        return _timer()

    operations = [
        ("load_dataset", lambda: time.sleep(0.5)),
        ("validate_schema", lambda: time.sleep(0.1)),
        ("transform_data", lambda: time.sleep(1.2)),
        ("save_results", lambda: time.sleep(0.3)),
    ]

    for op_name, op_func in operations:
        with timing_logger(op_name):
            op_func()


def demonstrate_error_logging():
    """Demonstrate comprehensive error logging."""

    print("\n❌ Error Logging and Exception Handling")

    setup_logging(
        level="DEBUG",
        format_string="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    logger = get_logger("error_example")

    def risky_operation(should_fail: bool = False, operation_type: str = "default"):
        """Simulate an operation that might fail."""

        logger.debug(
            f"Starting risky operation: {operation_type}",
            extra={"operation_type": operation_type, "risk_level": "medium"},
        )

        try:
            if should_fail:
                raise ValueError(f"Simulated failure in {operation_type} operation")
            else:
                time.sleep(0.1)
                logger.info(
                    f"Operation {operation_type} completed successfully",
                    extra={"operation_type": operation_type, "status": "success"},
                )
        except Exception as e:
            logger.error(
                f"Operation failed: {str(e)}",
                extra={
                    "operation_type": operation_type,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "status": "failed",
                },
                exc_info=True,
            )  # Include stack trace

            # Re-raise if needed
            # raise

    # Test successful and failed operations
    risky_operation(False, "data_validation")
    risky_operation(True, "file_processing")
    risky_operation(False, "report_generation")


def demonstrate_multi_output_logging():
    """Demonstrate logging to multiple outputs simultaneously."""

    print("\n📤 Multi-Output Logging")

    temp_dir = Path(tempfile.mkdtemp())

    # Setup logging to both console and file
    setup_logging(level="INFO", format_string="%(asctime)s [%(levelname)s] %(message)s")
    logger = get_logger("multi_output_example")

    print("✅ Multi-output logging configured")

    # Generate various log messages
    messages = [
        ("INFO", "Application started"),
        ("INFO", "Configuration loaded"),
        ("DEBUG", "Debug information"),
        ("WARNING", "Configuration uses default values"),
        ("INFO", "Processing data..."),
        ("ERROR", "Failed to process record #42"),
        ("INFO", "Processing completed"),
        ("INFO", "Application shutting down"),
    ]

    for level, message in messages:
        getattr(logger, level.lower())(message)

    print(f"\n✅ Generated {len(messages)} log messages")


def demonstrate_fsspeckit_integration():
    """Demonstrate logging integration with fsspeckit operations."""

    print("\n🔗 fsspeckit Integration Logging")

    # Setup logging for fsspeckit operations
    setup_logging(
        level="INFO",
        format_string="%(asctime)s [%(levelname)s] fsspeckit.%(name)s - %(message)s",
    )
    logger = get_logger("fsspeckit_integration")

    # Simulate fsspeckit operations with logging
    try:
        logger.info(
            "Initializing fsspeckit filesystem",
            extra={"operation": "filesystem_init", "backend": "local"},
        )

        # Simulate file operations
        operations = [
            ("scan_directory", "/data/input", 1250),
            ("read_files", "/data/input", 1250),
            ("process_data", "memory", 1250),
            ("write_results", "/data/output", 1250),
        ]

        for op_name, target, record_count in operations:
            logger.info(
                f"Executing {op_name}",
                extra={
                    "operation": op_name,
                    "target": target,
                    "record_count": record_count,
                },
            )

            # Simulate operation
            time.sleep(0.1)

            if op_name == "process_data":
                logger.warning(
                    f"Some records had validation warnings",
                    extra={
                        "operation": op_name,
                        "warning_count": int(record_count * 0.02),
                        "warning_types": ["null_values", "type_conversion"],
                    },
                )

        logger.info(
            "All fsspeckit operations completed",
            extra={
                "operations_completed": len(operations),
                "total_records": sum(rec for _, _, rec in operations),
                "status": "success",
            },
        )

    except Exception as e:
        logger.error(
            f"fsspeckit operation failed: {str(e)}",
            extra={"error_type": type(e).__name__, "status": "failed"},
            exc_info=True,
        )


def main():
    """Run all logging setup examples."""

    print("📝 Logging Setup Example")
    print("=" * 50)

    try:
        # Run all logging demonstrations
        demonstrate_basic_logging()
        demonstrate_structured_logging()
        demonstrate_contextual_logging()
        demonstrate_performance_logging()
        demonstrate_error_logging()
        demonstrate_multi_output_logging()
        demonstrate_fsspeckit_integration()

        print("\n" + "=" * 50)
        print("✅ All logging setup examples completed!")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    main()
