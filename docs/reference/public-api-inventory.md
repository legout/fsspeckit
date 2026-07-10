# Public API Inventory

This inventory records every supported public API in `fsspeckit`. Each row
states the canonical import, the generated reference page that documents it,
the optional extra required to use it (when applicable), and its compatibility
status.

For the authoritative extras matrix, see
[Installation](../installation.md#optional-extras).

## Inclusion rule

A symbol is supported public API when it meets all of:

1. It is a stable, non-private export (no leading underscore) of an approved
   root or domain package, or a deliberately listed direct-import module.
2. It is part of the package `__all__`, or a class/function defined at module
   level in a whitelisted direct-import module.
3. Root re-exports are preferred when offered; direct imports are listed only
   when explicitly whitelisted here.
4. `fsspeckit.utils` is a legacy facade. It has no generated page and no primary
   reference nav entry. Compatibility mappings are documented in the curated
   deprecation guidance, not here as generated reference.

Symbols not listed here are unsupported, internal, or deprecated and may change
or be removed without notice.

## Root / core

| Canonical import | Generated reference | Optional extra | Status |
| --- | --- | --- | --- |
| `from fsspeckit import filesystem` | [api/fsspeckit.core.md](../api/fsspeckit.core.md) | - | Stable |
| `from fsspeckit import get_filesystem` | [api/fsspeckit.core.md](../api/fsspeckit.core.md) | - | Stable |
| `from fsspeckit import AbstractFileSystem` | [api/fsspeckit.core.md](../api/fsspeckit.core.md) | - | Stable |
| `from fsspeckit import DirFileSystem` | [api/fsspeckit.core.md](../api/fsspeckit.core.md) | - | Stable |
| `from fsspeckit.core import filesystem` | [api/fsspeckit.core.md](../api/fsspeckit.core.md) | - | Stable |
| `from fsspeckit.core import get_filesystem` | [api/fsspeckit.core.md](../api/fsspeckit.core.md) | - | Stable |
| `from fsspeckit.core import AbstractFileSystem` | [api/fsspeckit.core.md](../api/fsspeckit.core.md) | - | Stable |
| `from fsspeckit.core import DirFileSystem` | [api/fsspeckit.core.md](../api/fsspeckit.core.md) | - | Stable |
| `from fsspeckit.core import GitLabFileSystem` | [api/fsspeckit.core.md](../api/fsspeckit.core.md) | - | Stable |
| `from fsspeckit.core import MonitoredSimpleCacheFileSystem` | [api/fsspeckit.core.md](../api/fsspeckit.core.md) | - | Stable |
| `from fsspeckit.core.merge import MergeStrategy` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import MergePlan` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import MergeStats` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import MergeTargetMetadata` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import MergePlanningResults` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import validate_rewrite_mode_compatibility` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import normalize_key_columns` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import validate_merge_inputs` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import check_null_keys` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import calculate_merge_stats` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import validate_strategy_compatibility` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import get_canonical_merge_strategies` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import plan_merge_operation` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import resolve_merge_plan_early_exit` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import validate_merge_inputs_comprehensive` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import plan_source_processing` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import select_rows_by_keys_common` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import execute_merge_by_strategy` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import build_merge_statistics` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.merge import handle_file_io_error_conservative` | [api/fsspeckit.core.merge.md](../api/fsspeckit.core.merge.md) | - | Stable |
| `from fsspeckit.core.maintenance import FileInfo` | [api/fsspeckit.core.maintenance.md](../api/fsspeckit.core.maintenance.md) | - | Stable |
| `from fsspeckit.core.maintenance import MaintenanceStats` | [api/fsspeckit.core.maintenance.md](../api/fsspeckit.core.maintenance.md) | - | Stable |
| `from fsspeckit.core.maintenance import CompactionGroup` | [api/fsspeckit.core.maintenance.md](../api/fsspeckit.core.maintenance.md) | - | Stable |
| `from fsspeckit.core.maintenance import collect_dataset_stats` | [api/fsspeckit.core.maintenance.md](../api/fsspeckit.core.maintenance.md) | - | Stable |
| `from fsspeckit.core.maintenance import plan_compaction_groups` | [api/fsspeckit.core.maintenance.md](../api/fsspeckit.core.maintenance.md) | - | Stable |
| `from fsspeckit.core.maintenance import plan_optimize_groups` | [api/fsspeckit.core.maintenance.md](../api/fsspeckit.core.maintenance.md) | - | Stable |
| `from fsspeckit.core.maintenance import plan_deduplication_groups` | [api/fsspeckit.core.maintenance.md](../api/fsspeckit.core.maintenance.md) | - | Stable |
| `from fsspeckit.core.maintenance import validate_deduplication_inputs` | [api/fsspeckit.core.maintenance.md](../api/fsspeckit.core.maintenance.md) | - | Stable |
| `from fsspeckit.core.maintenance import prepare_deduplication_stats` | [api/fsspeckit.core.maintenance.md](../api/fsspeckit.core.maintenance.md) | - | Stable |
| `from fsspeckit.core.maintenance import execute_deduplication_template` | [api/fsspeckit.core.maintenance.md](../api/fsspeckit.core.maintenance.md) | - | Stable |
| `from fsspeckit.core.maintenance import execute_compaction_template` | [api/fsspeckit.core.maintenance.md](../api/fsspeckit.core.maintenance.md) | - | Stable |

## Datasets / backends

| Canonical import | Generated reference | Optional extra | Status |
| --- | --- | --- | --- |
| `from fsspeckit.datasets import DatasetError` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | - | Stable |
| `from fsspeckit.datasets import DatasetFileError` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | - | Stable |
| `from fsspeckit.datasets import DatasetMergeError` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | - | Stable |
| `from fsspeckit.datasets import DatasetOperationError` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | - | Stable |
| `from fsspeckit.datasets import DatasetPathError` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | - | Stable |
| `from fsspeckit.datasets import DatasetSchemaError` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | - | Stable |
| `from fsspeckit.datasets import DatasetValidationError` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | - | Stable |
| `from fsspeckit.datasets import normalize_path` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | - | Stable |
| `from fsspeckit.datasets import validate_dataset_path` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | - | Stable |
| `from fsspeckit.datasets import PyarrowDatasetIO` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | datasets | Stable |
| `from fsspeckit.datasets import collect_dataset_stats_pyarrow` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | datasets | Stable |
| `from fsspeckit.datasets import compact_parquet_dataset_pyarrow` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | datasets | Stable |
| `from fsspeckit.datasets import optimize_parquet_dataset_pyarrow` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | datasets | Stable |
| `from fsspeckit.datasets import cast_schema` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | datasets | Stable |
| `from fsspeckit.datasets import convert_large_types_to_normal` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | datasets | Stable |
| `from fsspeckit.datasets import opt_dtype_pa` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | datasets | Stable |
| `from fsspeckit.datasets import unify_schemas_pa` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | datasets | Stable |
| `from fsspeckit.datasets import opt_dtype_pl` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | polars | Stable |
| `from fsspeckit.datasets import pl` | [api/fsspeckit.datasets.md](../api/fsspeckit.datasets.md) | polars | Stable |
| `from fsspeckit.datasets.duckdb import DuckDBConnection` | [api/fsspeckit.datasets.duckdb.md](../api/fsspeckit.datasets.duckdb.md) | datasets | Stable |
| `from fsspeckit.datasets.duckdb import create_duckdb_connection` | [api/fsspeckit.datasets.duckdb.md](../api/fsspeckit.datasets.duckdb.md) | datasets | Stable |
| `from fsspeckit.datasets.duckdb import DuckDBDatasetIO` | [api/fsspeckit.datasets.duckdb.md](../api/fsspeckit.datasets.duckdb.md) | datasets | Stable |
| `from fsspeckit.datasets.duckdb import collect_dataset_stats_duckdb` | [api/fsspeckit.datasets.duckdb.md](../api/fsspeckit.datasets.duckdb.md) | datasets | Stable |
| `from fsspeckit.datasets.duckdb import compact_parquet_dataset_duckdb` | [api/fsspeckit.datasets.duckdb.md](../api/fsspeckit.datasets.duckdb.md) | datasets | Stable |
| `from fsspeckit.datasets.pyarrow import PyarrowDatasetIO` | [api/fsspeckit.datasets.pyarrow.md](../api/fsspeckit.datasets.pyarrow.md) | datasets | Stable |
| `from fsspeckit.datasets.pyarrow import collect_dataset_stats_pyarrow` | [api/fsspeckit.datasets.pyarrow.md](../api/fsspeckit.datasets.pyarrow.md) | datasets | Stable |
| `from fsspeckit.datasets.pyarrow import compact_parquet_dataset_pyarrow` | [api/fsspeckit.datasets.pyarrow.md](../api/fsspeckit.datasets.pyarrow.md) | datasets | Stable |
| `from fsspeckit.datasets.pyarrow import optimize_parquet_dataset_pyarrow` | [api/fsspeckit.datasets.pyarrow.md](../api/fsspeckit.datasets.pyarrow.md) | datasets | Stable |
| `from fsspeckit.datasets.pyarrow import pyarrow_dataset` | [api/fsspeckit.datasets.pyarrow.md](../api/fsspeckit.datasets.pyarrow.md) | datasets | Stable |
| `from fsspeckit.datasets.pyarrow import pyarrow_parquet_dataset` | [api/fsspeckit.datasets.pyarrow.md](../api/fsspeckit.datasets.pyarrow.md) | datasets | Stable |
| `from fsspeckit.datasets.pyarrow import MemoryMonitor` | [api/fsspeckit.datasets.pyarrow.md](../api/fsspeckit.datasets.pyarrow.md) | monitoring | Stable |
| `from fsspeckit.datasets.pyarrow import MemoryPressureLevel` | [api/fsspeckit.datasets.pyarrow.md](../api/fsspeckit.datasets.pyarrow.md) | monitoring | Stable |
| `from fsspeckit.datasets.pyarrow import AdaptiveKeyTracker` | [api/fsspeckit.datasets.pyarrow.md](../api/fsspeckit.datasets.pyarrow.md) | datasets | Stable |

## Storage options

| Canonical import | Generated reference | Optional extra | Status |
| --- | --- | --- | --- |
| `from fsspeckit import BaseStorageOptions` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | - | Stable |
| `from fsspeckit import StorageOptions` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | - | Stable |
| `from fsspeckit import LocalStorageOptions` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | - | Stable |
| `from fsspeckit import AwsStorageOptions` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | aws | Stable |
| `from fsspeckit import GcsStorageOptions` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | gcp | Stable |
| `from fsspeckit import AzureStorageOptions` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | azure | Stable |
| `from fsspeckit import GitHubStorageOptions` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | - | Stable |
| `from fsspeckit import GitLabStorageOptions` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | - | Stable |
| `from fsspeckit.storage_options import from_dict` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | - | Stable |
| `from fsspeckit.storage_options import from_env` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | - | Stable |
| `from fsspeckit.storage_options import merge_storage_options` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | - | Stable |
| `from fsspeckit.storage_options import infer_protocol_from_uri` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | - | Stable |
| `from fsspeckit.storage_options import storage_options_from_uri` | [api/fsspeckit.storage_options.md](../api/fsspeckit.storage_options.md) | - | Stable |

## Common utilities

| Canonical import | Generated reference | Optional extra | Status |
| --- | --- | --- | --- |
| `from fsspeckit.common import get_timestamp_column` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import get_timedelta_str` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import timestamp_from_string` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import get_logger` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import setup_logging` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import get_partitions_from_path` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import normalize_partition_value` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import validate_partition_columns` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import build_partition_path` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import extract_partition_filters` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import filter_paths_by_partitions` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import infer_partitioning_scheme` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import get_partition_columns_from_paths` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import create_partition_expression` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import apply_partition_pruning` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import run_parallel` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import sync_dir` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import sync_files` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import validate_path` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import validate_compression_codec` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import scrub_credentials` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import scrub_exception` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import safe_format_error` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import validate_columns` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |
| `from fsspeckit.common import VALID_COMPRESSION_CODECS` | [api/fsspeckit.common.md](../api/fsspeckit.common.md) | - | Stable |

## SQL

| Canonical import | Generated reference | Optional extra | Status |
| --- | --- | --- | --- |
| `from fsspeckit.sql import sql2pyarrow_filter` | [api/fsspeckit.sql.filters.md](../api/fsspeckit.sql.filters.md) | sql | Stable |
| `from fsspeckit.sql import sql2polars_filter` | [api/fsspeckit.sql.filters.md](../api/fsspeckit.sql.filters.md) | sql | Stable |
| `from fsspeckit.sql import get_table_names` | [api/fsspeckit.sql.filters.md](../api/fsspeckit.sql.filters.md) | sql | Stable |

## Legacy facade (compatibility only)

`fsspeckit.utils` is a deprecated backwards-compatibility facade that
re-exports selected helpers from the domain packages. It is not part of the
generated reference and has no primary nav entry. New code must import from the
canonical domain packages listed above. See the curated deprecation guidance for
the mapping of old `fsspeckit.utils.*` imports to their canonical replacements.
