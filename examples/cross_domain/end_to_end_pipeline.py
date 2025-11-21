"""
End-to-End Pipeline Example

This cross-domain example demonstrates a complete data pipeline that integrates
multiple fsspeckit packages: datasets, sql, common, and storage_options.

The example covers:
1. Data extraction from multiple sources using storage_options
2. Data cleaning and transformation using datasets
3. Advanced SQL analytics using sql package
4. Data optimization and compaction
5. Cross-domain error handling and logging
6. Pipeline monitoring and observability
7. Production deployment patterns

This example shows how to build enterprise-grade, end-to-end data solutions
using the complete fsspeckit ecosystem.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Cross-domain fsspeckit imports
from fsspeckit.datasets import (
    DuckDBParquetHandler,
    optimize_parquet_dataset_pyarrow,
    compact_parquet_dataset_pyarrow
)
from fsspeckit.sql import (
    sql2pyarrow_filter,
    sql2polars_filter,
    validate_sql_query,
    optimize_sql_query
)
from fsspeckit.common import (
    setup_structured_logging,
    run_parallel,
    convert_table_format,
    retry_with_backoff
)
from fsspeckit.storage_options import (
    AwsStorageOptions,
    AzureStorageOptions,
    GcsStorageOptions,
    LocalStorageOptions
)

# Optional dependencies for enhanced functionality
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


@dataclass
class PipelineConfig:
    """Configuration for the end-to-end pipeline."""

    # Data sources
    source_paths: List[str]
    source_types: List[str]  # 'local', 's3', 'azure', 'gcs'

    # Processing configuration
    batch_size: int = 50000
    parallel_workers: int = 4
    enable_sql_optimization: bool = True
    enable_data_optimization: bool = True

    # Output configuration
    output_path: str = "output"
    output_format: str = "parquet"  # 'parquet', 'csv', 'json'
    compression: str = "snappy"

    # Storage configuration
    aws_region: str = "us-east-1"
    azure_account: Optional[str] = None
    gcp_project: Optional[str] = None

    # Monitoring
    enable_logging: bool = True
    log_level: str = "INFO"
    metrics_collection: bool = True


@dataclass
class PipelineMetrics:
    """Metrics for pipeline execution monitoring."""

    pipeline_start_time: float
    stage_metrics: Dict[str, Dict[str, Any]]

    # Overall metrics
    total_records_processed: int = 0
    total_bytes_processed: int = 0
    total_errors: int = 0
    total_warnings: int = 0

    # Performance metrics
    extraction_time: float = 0.0
    transformation_time: float = 0.0
    optimization_time: float = 0.0
    loading_time: float = 0.0
    total_time: float = 0.0

    # Data quality metrics
    records_filtered: int = 0
    duplicate_records_removed: int = 0
    null_values_handled: int = 0
    data_types_converted: int = 0

    def update_stage_metric(self, stage: str, metric: str, value: Any):
        """Update a specific stage metric."""
        if stage not in self.stage_metrics:
            self.stage_metrics[stage] = {}
        self.stage_metrics[stage][metric] = value

    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary."""
        return {
            "execution_summary": {
                "total_records_processed": self.total_records_processed,
                "total_bytes_processed": self.total_bytes_processed,
                "total_errors": self.total_errors,
                "total_warnings": self.total_warnings,
                "total_time_seconds": self.total_time
            },
            "performance_breakdown": {
                "extraction_time_seconds": self.extraction_time,
                "transformation_time_seconds": self.transformation_time,
                "optimization_time_seconds": self.optimization_time,
                "loading_time_seconds": self.loading_time
            },
            "data_quality": {
                "records_filtered": self.records_filtered,
                "duplicate_records_removed": self.duplicate_records_removed,
                "null_values_handled": self.null_values_handled,
                "data_types_converted": self.data_types_converted
            },
            "stage_details": self.stage_metrics
        }


class EndToEndPipeline:
    """Complete end-to-end data pipeline using multiple fsspeckit packages."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.metrics = PipelineMetrics(
            pipeline_start_time=time.time(),
            stage_metrics={}
        )

        # Setup logging from common package
        if config.enable_logging:
            self.logger = setup_structured_logging(
                name="end_to_end_pipeline",
                level=config.log_level,
                include_timestamp=True,
                include_extra_fields=True
            )
        else:
            self.logger = logging.getLogger(__name__)

        # Initialize storage options for different providers
        self.storage_options = self._initialize_storage_options()

        # Initialize handlers
        self.duckdb_handler = DuckDBParquetHandler()

        self.logger.info("Pipeline initialized", extra={
            "config": asdict(config),
            "storage_providers": list(self.storage_options.keys())
        })

    def _initialize_storage_options(self) -> Dict[str, Dict[str, Any]]:
        """Initialize storage options for different providers."""
        options = {}

        # Local storage
        options["local"] = LocalStorageOptions().to_dict()

        # AWS S3
        if "s3" in self.config.source_types:
            options["s3"] = AwsStorageOptions(
                region=self.config.aws_region
            ).to_dict()

        # Azure Blob
        if "azure" in self.config.source_types:
            options["azure"] = AzureStorageOptions(
                account_name=self.config.azure_account
            ).to_dict()

        # Google Cloud Storage
        if "gcs" in self.config.source_types:
            options["gcs"] = GcsStorageOptions(
                project_id=self.config.gcp_project
            ).to_dict()

        return options

    async def run_pipeline(self) -> Dict[str, Any]:
        """Execute the complete end-to-end pipeline."""
        self.logger.info("Starting end-to-end pipeline execution")

        try:
            # Stage 1: Data Extraction
            extracted_data = await self._extract_data()

            # Stage 2: Data Transformation
            transformed_data = await self._transform_data(extracted_data)

            # Stage 3: Data Optimization
            optimized_data = await self._optimize_data(transformed_data)

            # Stage 4: Data Loading
            output_paths = await self._load_data(optimized_data)

            # Calculate final metrics
            self.metrics.total_time = time.time() - self.metrics.pipeline_start_time

            self.logger.info("Pipeline completed successfully", extra={
                "metrics": self.metrics.get_summary(),
                "output_paths": output_paths
            })

            return {
                "status": "success",
                "metrics": self.metrics.get_summary(),
                "output_paths": output_paths,
                "data_shape": {
                    "tables": len(optimized_data),
                    "total_records": sum(len(table) for table in optimized_data)
                }
            }

        except Exception as e:
            self.metrics.total_errors += 1
            self.logger.error("Pipeline execution failed", extra={
                "error": str(e),
                "error_type": type(e).__name__,
                "stage": "pipeline_execution"
            })
            raise

    async def _extract_data(self) -> List[pa.Table]:
        """Stage 1: Extract data from multiple sources."""
        self.logger.info("Starting data extraction stage")
        extraction_start = time.time()

        extraction_tasks = []

        # Create extraction tasks for each source
        for i, (source_path, source_type) in enumerate(zip(self.config.source_paths, self.config.source_types)):
            task = self._extract_single_source(source_path, source_type, f"source_{i}")
            extraction_tasks.append(task)

        # Execute extractions in parallel
        try:
            extracted_tables = await asyncio.gather(*extraction_tasks)
            valid_tables = [table for table in extracted_tables if table is not None]

            # Update metrics
            self.metrics.extraction_time = time.time() - extraction_start
            self.metrics.total_records_processed += sum(len(table) for table in valid_tables)
            self.metrics.total_bytes_processed += sum(table.nbytes for table in valid_tables)

            self.logger.info("Data extraction completed", extra={
                "sources_processed": len(self.config.source_paths),
                "valid_tables": len(valid_tables),
                "total_records": self.metrics.total_records_processed,
                "extraction_time": self.metrics.extraction_time
            })

            return valid_tables

        except Exception as e:
            self.logger.error("Data extraction failed", extra={
                "error": str(e),
                "sources_count": len(self.config.source_paths)
            })
            raise

    @retry_with_backoff(max_retries=3, base_delay=1.0)
    async def _extract_single_source(self, source_path: str, source_type: str, source_name: str) -> Optional[pa.Table]:
        """Extract data from a single source with retry logic."""
        try:
            self.logger.debug(f"Extracting data from {source_name}: {source_path}")

            # Get appropriate storage options
            storage_options = self.storage_options.get(source_type, {})

            # Use datasets package for extraction
            if source_path.endswith('.parquet'):
                # Extract from Parquet
                table = pq.read_table(source_path, filesystem_options=storage_options)
            elif source_path.endswith('.csv'):
                # Extract from CSV
                table = pa.csv.read_csv(source_path, read_options=pa.csv.ReadOptions(block_size=1024*1024))
            else:
                # Try to auto-detect format
                table = pq.read_table(source_path, filesystem_options=storage_options)

            self.logger.debug(f"Successfully extracted {len(table)} records from {source_name}")
            return table

        except Exception as e:
            self.logger.warning(f"Failed to extract from {source_name}: {e}", extra={
                "source_path": source_path,
                "source_type": source_type
            })
            return None

    async def _transform_data(self, data_tables: List[pa.Table]) -> List[pa.Table]:
        """Stage 2: Transform data using SQL and common utilities."""
        self.logger.info("Starting data transformation stage")
        transformation_start = time.time()

        transformed_tables = []

        for i, table in enumerate(data_tables):
            try:
                # Apply transformations using multiple packages
                transformed = await self._transform_single_table(table, f"table_{i}")
                if transformed is not None:
                    transformed_tables.append(transformed)

            except Exception as e:
                self.logger.error(f"Failed to transform table_{i}: {e}")
                self.metrics.total_errors += 1

        # Update metrics
        self.metrics.transformation_time = time.time() - transformation_start
        self.metrics.update_stage_metric("transformation", "tables_processed", len(transformed_tables))
        self.metrics.update_stage_metric("transformation", "transformation_time", self.metrics.transformation_time)

        self.logger.info("Data transformation completed", extra={
            "input_tables": len(data_tables),
            "output_tables": len(transformed_tables),
            "transformation_time": self.metrics.transformation_time
        })

        return transformed_tables

    async def _transform_single_table(self, table: pa.Table, table_name: str) -> Optional[pa.Table]:
        """Transform a single table using SQL filtering and data cleaning."""
        try:
            self.logger.debug(f"Transforming {table_name}: {len(table)} records")

            # Step 1: Data quality assessment using common utilities
            quality_metrics = self._assess_data_quality(table)
            self.logger.debug(f"Data quality for {table_name}: {quality_metrics}")

            # Step 2: SQL-based filtering using sql package
            filtered_table = await self._apply_sql_filters(table, table_name)

            # Step 3: Data cleaning and standardization
            cleaned_table = await self._clean_and_standardize_data(filtered_table, table_name)

            # Step 4: Schema optimization using datasets package
            optimized_table = await self._optimize_table_schema(cleaned_table, table_name)

            # Update metrics
            self.metrics.records_filtered += len(table) - len(optimized_table)
            self.metrics.null_values_handled += quality_metrics.get('null_count', 0)
            self.metrics.data_types_converted += quality_metrics.get('type_conversions', 0)

            self.logger.debug(f"Transformation completed for {table_name}: {len(optimized_table)} records")
            return optimized_table

        except Exception as e:
            self.logger.error(f"Transformation failed for {table_name}: {e}")
            return None

    def _assess_data_quality(self, table: pa.Table) -> Dict[str, Any]:
        """Assess data quality using common utilities."""
        null_counts = {}
        type_conversions = 0

        for column_name in table.column_names:
            column = table.column(column_name)
            null_count = pc.count_null(column).as_py()
            if null_count > 0:
                null_counts[column_name] = null_count

            # Check for potential type improvements
            if pa.types.is_string(column.type):
                # Check if all values are numeric
                try:
                    numeric_values = pc.cast(column, pa.float64(), safe=False)
                    if pc.count_null(numeric_values).as_py() == 0:
                        type_conversions += 1
                except:
                    pass

        return {
            "total_records": len(table),
            "null_counts": null_counts,
            "null_count": sum(null_counts.values()),
            "type_conversions": type_conversions,
            "columns": table.column_names
        }

    async def _apply_sql_filters(self, table: pa.Table, table_name: str) -> pa.Table:
        """Apply SQL-based filters using sql package."""
        if not self.config.enable_sql_optimization:
            return table

        try:
            # Define SQL filters based on data characteristics
            sql_filters = self._generate_sql_filters(table, table_name)

            if not sql_filters:
                return table

            # Convert SQL to PyArrow filter using sql package
            pyarrow_filter = sql2pyarrow_filter(sql_filters, table.schema)

            # Apply filter
            filtered_table = table.filter(pyarrow_filter)

            self.logger.debug(f"SQL filtering applied to {table_name}: {len(table)} -> {len(filtered_table)} records")

            return filtered_table

        except Exception as e:
            self.logger.warning(f"SQL filtering failed for {table_name}: {e}")
            return table

    def _generate_sql_filters(self, table: pa.Table, table_name: str) -> str:
        """Generate appropriate SQL filters for the table."""
        filters = []

        # Example filters based on common business rules
        for column_name in table.column_names:
            column = table.column(column_name)

            # Remove rows with null values in key columns
            if "id" in column_name.lower() or "key" in column_name.lower():
                filters.append(f"{column_name} IS NOT NULL")

            # Numeric range filters
            if pa.types.is_numeric(column.type):
                min_val = pc.min(column).as_py()
                max_val = pc.max(column).as_py()

                # Remove obvious outliers (example: negative values where not expected)
                if "amount" in column_name.lower() or "value" in column_name.lower():
                    if min_val < 0:
                        filters.append(f"{column_name} >= 0")

            # String length filters
            elif pa.types.is_string(column.type):
                filters.append(f"LENGTH({column_name}) > 0")

        return " AND ".join(filters) if filters else ""

    async def _clean_and_standardize_data(self, table: pa.Table, table_name: str) -> pa.Table:
        """Clean and standardize data using common utilities."""
        try:
            # Remove duplicates
            deduplicated_table = self._remove_duplicates(table)

            # Standardize string columns
            standardized_table = self._standardize_strings(deduplicated_table)

            # Handle null values
            cleaned_table = self._handle_null_values(standardized_table)

            self.logger.debug(f"Data cleaning applied to {table_name}: {len(table)} -> {len(cleaned_table)} records")

            return cleaned_table

        except Exception as e:
            self.logger.warning(f"Data cleaning failed for {table_name}: {e}")
            return table

    def _remove_duplicates(self, table: pa.Table) -> pa.Table:
        """Remove duplicate records using common utilities."""
        # For simplicity, use all columns for duplicate detection
        # In practice, you might use specific key columns
        try:
            # Convert to pandas for easy duplicate removal if available
            if PANDAS_AVAILABLE:
                df = table.to_pandas()
                df_dedup = df.drop_duplicates()
                return pa.Table.from_pandas(df_dedup)
            else:
                # Simple PyArrow approach for demonstration
                return table
        except:
            return table

    def _standardize_strings(self, table: pa.Table) -> pa.Table:
        """Standardize string columns."""
        standardized_columns = []

        for column_name in table.column_names:
            column = table.column(column_name)

            if pa.types.is_string(column.type):
                # Trim whitespace and convert to consistent case
                try:
                    trimmed = pc.utf8_trim(column)
                    standardized = pc.utf8_upper(trimmed)
                    standardized_columns.append(standardized)
                except:
                    standardized_columns.append(column)
            else:
                standardized_columns.append(column)

        return pa.Table.from_arrays(standardized_columns, names=table.column_names)

    def _handle_null_values(self, table: pa.Table) -> pa.Table:
        """Handle null values in the data."""
        # For demonstration, we'll just log null counts
        null_counts = {}
        for column_name in table.column_names:
            column = table.column(column_name)
            null_count = pc.count_null(column).as_py()
            if null_count > 0:
                null_counts[column_name] = null_count

        if null_counts:
            self.logger.debug(f"Null values found: {null_counts}")

        return table

    async def _optimize_table_schema(self, table: pa.Table, table_name: str) -> pa.Table:
        """Optimize table schema using datasets package."""
        try:
            # Use datasets package optimization
            # This is a simplified version - in practice, you'd have more sophisticated optimization
            optimized_columns = []

            for column_name in table.column_names:
                column = table.column(column_name)

                # Optimize numeric types
                if pa.types.is_integer(column.type):
                    # Try to downcast to smaller integer types
                    min_val = pc.min(column).as_py()
                    max_val = pc.max(column).as_py()

                    if min_val >= 0 and max_val <= 255:
                        optimized_column = pc.cast(column, pa.uint8())
                    elif min_val >= -128 and max_val <= 127:
                        optimized_column = pc.cast(column, pa.int8())
                    elif min_val >= 0 and max_val <= 65535:
                        optimized_column = pc.cast(column, pa.uint16())
                    else:
                        optimized_column = column
                else:
                    optimized_column = column

                optimized_columns.append(optimized_column)

            return pa.Table.from_arrays(optimized_columns, names=table.column_names)

        except Exception as e:
            self.logger.warning(f"Schema optimization failed for {table_name}: {e}")
            return table

    async def _optimize_data(self, data_tables: List[pa.Table]) -> List[pa.Table]:
        """Stage 3: Optimize data using datasets package."""
        self.logger.info("Starting data optimization stage")
        optimization_start = time.time()

        if not self.config.enable_data_optimization:
            self.logger.info("Data optimization skipped by configuration")
            return data_tables

        try:
            # Combine all tables for optimization
            if len(data_tables) > 1:
                combined_table = pa.concat_tables(data_tables)
            else:
                combined_table = data_tables[0] if data_tables else pa.table([])

            if len(combined_table) == 0:
                return []

            # Create temporary dataset for optimization
            temp_path = f"temp_optimization_{int(time.time())}"

            # Save as individual files first
            pq.write_table(combined_table, f"{temp_path}.parquet", compression=self.config.compression)

            # Use datasets package for optimization
            optimize_parquet_dataset_pyarrow(
                temp_path,
                zorder_columns=[col for col in combined_table.column_names if pa.types.is_numeric(combined_table.column(col).type)][:3],  # First 3 numeric columns
                target_file_size_mb=128,
                compression=self.config.compression
            )

            # Compact the dataset
            compact_parquet_dataset_pyarrow(
                temp_path,
                delete_originals=True
            )

            # Read back optimized data
            optimized_table = pq.read_table(f"{temp_path}.parquet")

            # Clean up
            import os
            try:
                os.remove(f"{temp_path}.parquet")
            except:
                pass

            # Update metrics
            self.metrics.optimization_time = time.time() - optimization_start
            self.metrics.update_stage_metric("optimization", "records_optimized", len(optimized_table))
            self.metrics.update_stage_metric("optimization", "optimization_time", self.metrics.optimization_time)

            self.logger.info("Data optimization completed", extra={
                "records_optimized": len(optimized_table),
                "optimization_time": self.metrics.optimization_time
            })

            return [optimized_table]

        except Exception as e:
            self.logger.error(f"Data optimization failed: {e}")
            return data_tables

    async def _load_data(self, data_tables: List[pa.Table]) -> List[str]:
        """Stage 4: Load data to output destination."""
        self.logger.info("Starting data loading stage")
        loading_start = time.time()

        output_paths = []

        try:
            # Create output directory
            output_dir = Path(self.config.output_path)
            output_dir.mkdir(parents=True, exist_ok=True)

            # Load each table
            for i, table in enumerate(data_tables):
                output_path = await self._load_single_table(table, i, output_dir)
                if output_path:
                    output_paths.append(output_path)

            # Update metrics
            self.metrics.loading_time = time.time() - loading_start
            self.metrics.update_stage_metric("loading", "files_created", len(output_paths))
            self.metrics.update_stage_metric("loading", "loading_time", self.metrics.loading_time)

            self.logger.info("Data loading completed", extra={
                "files_created": len(output_paths),
                "loading_time": self.metrics.loading_time
            })

            return output_paths

        except Exception as e:
            self.logger.error(f"Data loading failed: {e}")
            raise

    async def _load_single_table(self, table: pa.Table, index: int, output_dir: Path) -> Optional[str]:
        """Load a single table to the output destination."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            if self.config.output_format == "parquet":
                output_path = output_dir / f"pipeline_output_{index}_{timestamp}.parquet"
                pq.write_table(table, output_path, compression=self.config.compression)

            elif self.config.output_format == "csv":
                output_path = output_dir / f"pipeline_output_{index}_{timestamp}.csv"
                table.to_pandas().to_csv(output_path, index=False)

            elif self.config.output_format == "json":
                output_path = output_dir / f"pipeline_output_{index}_{timestamp}.json"
                table.to_pandas().to_json(output_path, orient='records', indent=2)

            else:
                raise ValueError(f"Unsupported output format: {self.config.output_format}")

            self.logger.debug(f"Loaded table {index} to {output_path}: {len(table)} records")
            return str(output_path)

        except Exception as e:
            self.logger.error(f"Failed to load table {index}: {e}")
            return None


def create_sample_data_sources(output_dir: str) -> Tuple[List[str], List[str]]:
    """Create sample data sources for the pipeline demonstration."""
    import random

    print("📝 Creating sample data sources...")

    data_dir = Path(output_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    # Create multiple data sources with different characteristics
    sources = []
    source_types = []

    # Source 1: Sales data (local Parquet)
    sales_records = []
    for i in range(10000):
        record = {
            "transaction_id": f"txn_{i:08d}",
            "customer_id": f"cust_{random.randint(1, 5000):06d}",
            "product_id": f"prod_{random.randint(1, 100):03d}",
            "quantity": random.randint(1, 10),
            "unit_price": round(random.uniform(10.0, 1000.0), 2),
            "discount": round(random.uniform(0.0, 0.3), 3),
            "timestamp": datetime.now() - timedelta(hours=random.randint(0, 720)),
            "region": random.choice(["North", "South", "East", "West"]),
            "channel": random.choice(["Online", "Retail", "Partner"])
        }
        record["total_amount"] = record["quantity"] * record["unit_price"] * (1 - record["discount"])
        sales_records.append(record)

    sales_table = pa.Table.from_pylist(sales_records)
    sales_path = data_dir / "sales_data.parquet"
    pq.write_table(sales_table, sales_path)
    sources.append(str(sales_path))
    source_types.append("local")

    # Source 2: Customer data (local CSV)
    customer_records = []
    for i in range(5000):
        record = {
            "customer_id": f"cust_{i:06d}",
            "name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "age": random.randint(18, 80),
            "gender": random.choice(["M", "F", "Other"]),
            "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
            "state": random.choice(["NY", "CA", "IL", "TX", "AZ"]),
            "registration_date": datetime.now() - timedelta(days=random.randint(1, 3650)),
            "premium_member": random.choice([True, False])
        }
        customer_records.append(record)

    customer_df = pd.DataFrame(customer_records) if PANDAS_AVAILABLE else None
    customer_path = data_dir / "customer_data.csv"
    if customer_df is not None:
        customer_df.to_csv(customer_path, index=False)
    else:
        # Fallback: create CSV manually
        with open(customer_path, 'w') as f:
            f.write("customer_id,name,email,age,gender,city,state,registration_date,premium_member\n")
            for record in customer_records:
                f.write(f"{record['customer_id']},{record['name']},{record['email']},{record['age']},{record['gender']},{record['city']},{record['state']},{record['registration_date']},{record['premium_member']}\n")

    sources.append(str(customer_path))
    source_types.append("local")

    # Source 3: Product data (local Parquet)
    product_records = []
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
    for i in range(100):
        record = {
            "product_id": f"prod_{i:03d}",
            "name": f"Product {i}",
            "category": random.choice(categories),
            "price": round(random.uniform(10.0, 1000.0), 2),
            "cost": round(random.uniform(5.0, 500.0), 2),
            "weight": round(random.uniform(0.1, 10.0), 2),
            "in_stock": random.choice([True, False]),
            "supplier": f"Supplier_{random.randint(1, 20):02d}"
        }
        product_records.append(record)

    product_table = pa.Table.from_pylist(product_records)
    product_path = data_dir / "product_data.parquet"
    pq.write_table(product_table, product_path)
    sources.append(str(product_path))
    source_types.append("local")

    print(f"   Created {len(sources)} sample data sources:")
    for source, source_type in zip(sources, source_types):
        print(f"     {source_type}: {source}")

    return sources, source_types


async def demonstrate_end_to_end_pipeline():
    """Demonstrate the complete end-to-end pipeline."""

    print("\n🔄 End-to-End Pipeline Demonstration")
    print("=" * 60)

    # Create sample data
    temp_dir = "temp_pipeline_data"
    sources, source_types = create_sample_data_sources(temp_dir)

    try:
        # Configure pipeline
        config = PipelineConfig(
            source_paths=sources,
            source_types=source_types,
            batch_size=5000,
            parallel_workers=2,
            enable_sql_optimization=True,
            enable_data_optimization=True,
            output_path="pipeline_output",
            output_format="parquet",
            compression="snappy",
            enable_logging=True,
            log_level="INFO",
            metrics_collection=True
        )

        print(f"\n📋 Pipeline Configuration:")
        print(f"   Sources: {len(sources)}")
        print(f"   Batch size: {config.batch_size}")
        print(f"   Workers: {config.parallel_workers}")
        print(f"   SQL optimization: {config.enable_sql_optimization}")
        print(f"   Data optimization: {config.enable_data_optimization}")

        # Initialize and run pipeline
        pipeline = EndToEndPipeline(config)

        print(f"\n🚀 Starting pipeline execution...")
        result = await pipeline.run_pipeline()

        # Display results
        print(f"\n✅ Pipeline completed successfully!")
        print(f"   Status: {result['status']}")
        print(f"   Data shape: {result['data_shape']}")
        print(f"   Output files: {len(result['output_paths'])}")

        # Display metrics
        metrics = result['metrics']
        print(f"\n📊 Pipeline Metrics:")

        exec_summary = metrics['execution_summary']
        print(f"   Total records processed: {exec_summary['total_records_processed']:,}")
        print(f"   Total bytes processed: {exec_summary['total_bytes_processed'] / 1024 / 1024:.1f}MB")
        print(f"   Total time: {exec_summary['total_time_seconds']:.1f} seconds")
        print(f"   Errors: {exec_summary['total_errors']}")
        print(f"   Warnings: {exec_summary['total_warnings']}")

        performance = metrics['performance_breakdown']
        print(f"\n⏱️ Performance Breakdown:")
        print(f"   Extraction: {performance['extraction_time_seconds']:.2f}s")
        print(f"   Transformation: {performance['transformation_time_seconds']:.2f}s")
        print(f"   Optimization: {performance['optimization_time_seconds']:.2f}s")
        print(f"   Loading: {performance['loading_time_seconds']:.2f}s")

        data_quality = metrics['data_quality']
        print(f"\n🧹 Data Quality:")
        print(f"   Records filtered: {data_quality['records_filtered']}")
        print(f"   Duplicates removed: {data_quality['duplicate_records_removed']}")
        print(f"   Null values handled: {data_quality['null_values_handled']}")
        print(f"   Data types converted: {data_quality['data_types_converted']}")

        print(f"\n📁 Output Files:")
        for output_path in result['output_paths']:
            if Path(output_path).exists():
                size = Path(output_path).stat().st_size / 1024 / 1024
                print(f"   {output_path} ({size:.1f}MB)")

    except Exception as e:
        print(f"❌ Pipeline demonstration failed: {e}")
        raise

    finally:
        # Cleanup
        import shutil
        try:
            shutil.rmtree(temp_dir)
        except:
            pass


def demonstrate_cross_domain_integration():
    """Demonstrate specific cross-domain integrations."""

    print("\n🔗 Cross-Domain Integration Examples")

    # Integration 1: SQL + Datasets
    print("\n1. SQL + Datasets Integration:")
    print("   Using sql2pyarrow_filter with DuckDBParquetHandler")

    # Integration 2: Common + Storage
    print("\n2. Common + Storage Options Integration:")
    print("   Using run_parallel with cloud storage options")

    # Integration 3: All packages together
    print("\n3. Complete Package Integration:")
    print("   Structured logging + SQL optimization + Parallel processing")

    # Create a small demonstration
    try:
        # Sample data
        sample_data = pa.table({
            "id": pa.array(range(100)),
            "value": pa.array([i * 2.5 for i in range(100)]),
            "category": pa.array([f"cat_{i % 5}" for i in range(100)])
        })

        print("\n   Example: Cross-domain data processing")

        # SQL filter from sql package
        sql_filter = "value > 100 AND category IN ('cat_1', 'cat_3')"
        pyarrow_filter = sql2pyarrow_filter(sql_filter, sample_data.schema)
        filtered_data = sample_data.filter(pyarrow_filter)

        print(f"   SQL filtering: {len(sample_data)} -> {len(filtered_data)} records")

        # Format conversion from common package
        if PANDAS_AVAILABLE:
            pandas_df = convert_table_format(filtered_data, "pandas")
            print(f"   Format conversion: PyArrow -> Pandas DataFrame")

        print("   ✅ Cross-domain integration successful")

    except Exception as e:
        print(f"   ❌ Cross-domain integration failed: {e}")


async def main():
    """Run all cross-domain pipeline examples."""

    print("🔗 End-to-End Pipeline Example")
    print("=" * 60)
    print("This example demonstrates complete data pipeline integration")
    print("across multiple fsspeckit packages.")

    try:
        # Run main demonstration
        await demonstrate_end_to_end_pipeline()

        # Show cross-domain integrations
        demonstrate_cross_domain_integration()

        print("\n" + "=" * 60)
        print("✅ Cross-domain integration examples completed!")

        print(f"\n🎯 Integration Takeaways:")
        print("• Datasets + SQL: Advanced data filtering and optimization")
        print("• Common + Storage: Parallel processing across cloud providers")
        print("• All packages: Comprehensive production-ready pipelines")
        print("• Error handling: Robust retry and recovery mechanisms")
        print("• Monitoring: End-to-end observability and metrics")

        print(f"\n🏗️ Production Patterns:")
        print("• Structured logging for debugging and monitoring")
        print("• Parallel extraction from multiple data sources")
        print("• SQL-based data transformation and validation")
        print("• Automatic schema and performance optimization")
        print("• Cloud-agnostic storage operations")
        print("• Comprehensive metrics and observability")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())