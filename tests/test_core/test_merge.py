"""Tests for backend-neutral merge layer core functionality."""

import pytest
import pyarrow as pa
import pyarrow.compute as pc

from fsspeckit.core.merge import (
    MergeStrategy,
    MergePlan,
    MergeStats,
    normalize_key_columns,
    validate_merge_inputs,
    check_null_keys,
    calculate_merge_stats,
    validate_strategy_compatibility,
    get_canonical_merge_strategies,
)


class TestMergeStrategy:
    """Test MergeStrategy enum and its functionality."""

    def test_strategy_values(self):
        """Test that all expected strategies are defined."""
        expected_strategies = ["upsert", "insert", "update", "full_merge", "deduplicate"]
        actual_strategies = [strategy.value for strategy in MergeStrategy]

        assert sorted(actual_strategies) == sorted(expected_strategies)

    def test_strategy_docstrings(self):
        """Test that each strategy has a helpful docstring."""
        for strategy in MergeStrategy:
            assert strategy.__doc__ is not None
            assert len(strategy.__doc__.strip()) > 0

    def test_get_canonical_merge_strategies(self):
        """Test getting canonical merge strategies."""
        strategies = get_canonical_merge_strategies()
        assert isinstance(strategies, list)
        assert len(strategies) == 5
        assert all(isinstance(s, str) for s in strategies)
        assert "upsert" in strategies
        assert "insert" in strategies
        assert "update" in strategies
        assert "full_merge" in strategies
        assert "deduplicate" in strategies


class TestNormalizeKeyColumns:
    """Test key column normalization functionality."""

    def test_normalize_single_string(self):
        """Test normalizing a single string key column."""
        result = normalize_key_columns("id")
        assert result == ["id"]

    def test_normalize_string_with_spaces(self):
        """Test normalizing string with whitespace."""
        result = normalize_key_columns("  user_id  ")
        assert result == ["user_id"]

    def test_normalize_list_of_strings(self):
        """Test normalizing list of key columns."""
        result = normalize_key_columns(["id", "user_id"])
        assert result == ["id", "user_id"]

    def test_normalize_list_with_spaces(self):
        """Test normalizing list with whitespace."""
        result = normalize_key_columns([" id ", " user_id "])
        assert result == ["id", "user_id"]

    def test_empty_string_error(self):
        """Test error for empty string."""
        with pytest.raises(ValueError, match="key_columns cannot be empty string"):
            normalize_key_columns("")

    def test_empty_list_error(self):
        """Test error for empty list."""
        with pytest.raises(ValueError, match="key_columns cannot be empty"):
            normalize_key_columns([])

    def test_list_with_empty_string_error(self):
        """Test error for list containing empty string."""
        with pytest.raises(ValueError, match="cannot contain empty strings"):
            normalize_key_columns(["id", "", "user_id"])

    def test_non_string_type_error(self):
        """Test error for non-string types."""
        with pytest.raises(ValueError, match="key_columns must be strings"):
            normalize_key_columns([1, 2, 3])

    def test_whitespace_only_string_error(self):
        """Test error for strings with only whitespace."""
        with pytest.raises(ValueError, match="cannot contain empty strings"):
            normalize_key_columns(["   "])


class TestMergePlan:
    """Test MergePlan dataclass functionality."""

    def test_merge_plan_creation(self):
        """Test creating a basic merge plan."""
        plan = MergePlan(
            strategy=MergeStrategy.UPSERT,
            key_columns=["id"],
            source_count=100,
            target_exists=True,
        )

        assert plan.strategy == MergeStrategy.UPSERT
        assert plan.key_columns == ["id"]
        assert plan.source_count == 100
        assert plan.target_exists is True
        assert plan.key_columns_valid is True
        assert plan.schema_compatible is True
        assert plan.null_keys_detected is False

    def test_merge_plan_validation(self):
        """Test merge plan validation in __post_init__."""
        # Test empty key columns
        with pytest.raises(ValueError, match="key_columns must be non-empty"):
            MergePlan(
                strategy=MergeStrategy.UPSERT,
                key_columns=[],
                source_count=100,
                target_exists=True,
            )

        # Test negative source count
        with pytest.raises(ValueError, match="source_count must be >= 0"):
            MergePlan(
                strategy=MergeStrategy.UPSERT,
                key_columns=["id"],
                source_count=-1,
                target_exists=True,
            )

    def test_deduplicate_default_ordering(self):
        """Test that DEDUPLICATE strategy gets default ordering."""
        plan = MergePlan(
            strategy=MergeStrategy.DEDUPLICATE,
            key_columns=["id", "timestamp"],
            source_count=100,
            target_exists=True,
            dedup_order_by=None,  # Should default to key columns
        )

        assert plan.dedup_order_by == ["id", "timestamp"]

    def test_deduplicate_custom_ordering(self):
        """Test that DEDUPLICATE strategy respects custom ordering."""
        plan = MergePlan(
            strategy=MergeStrategy.DEDUPLICATE,
            key_columns=["id"],
            source_count=100,
            target_exists=True,
            dedup_order_by=["updated_at"],
        )

        assert plan.dedup_order_by == ["updated_at"]


class TestMergeStats:
    """Test MergeStats dataclass functionality."""

    def test_merge_stats_creation(self):
        """Test creating basic merge statistics."""
        stats = MergeStats(
            strategy=MergeStrategy.UPSERT,
            source_count=100,
            target_count_before=50,
            target_count_after=75,
            inserted=30,
            updated=20,
            deleted=5,
        )

        assert stats.strategy == MergeStrategy.UPSERT
        assert stats.source_count == 100
        assert stats.target_count_before == 50
        assert stats.target_count_after == 75
        assert stats.inserted == 30
        assert stats.updated == 20
        assert stats.deleted == 5
        # total_processed should be auto-calculated
        assert stats.total_processed == 50  # inserted + updated

    def test_merge_stats_validation(self):
        """Test merge stats validation."""
        with pytest.raises(ValueError, match="source_count must be >= 0"):
            MergeStats(
                strategy=MergeStrategy.UPSERT,
                source_count=-1,
                target_count_before=0,
                target_count_after=0,
                inserted=0,
                updated=0,
                deleted=0,
            )

    def test_merge_stats_to_dict(self):
        """Test converting merge stats to dictionary format."""
        stats = MergeStats(
            strategy=MergeStrategy.UPSERT,
            source_count=100,
            target_count_before=50,
            target_count_after=75,
            inserted=30,
            updated=20,
            deleted=5,
        )

        result = stats.to_dict()

        expected_keys = {
            "inserted", "updated", "deleted", "total",
            "source_count", "target_count_before", "target_count_after",
            "total_processed", "strategy"
        }

        assert set(result.keys()) == expected_keys
        assert result["inserted"] == 30
        assert result["updated"] == 20
        assert result["deleted"] == 5
        assert result["total"] == 75
        assert result["strategy"] == "upsert"


class TestValidateMergeInputs:
    """Test merge input validation functionality."""

    def test_valid_inputs_with_existing_target(self):
        """Test validation with valid inputs and existing target."""
        source_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64()),
        ])

        target_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64()),
        ])

        plan = validate_merge_inputs(
            source_schema=source_schema,
            target_schema=target_schema,
            key_columns=["id"],
            strategy=MergeStrategy.UPSERT,
        )

        assert plan.strategy == MergeStrategy.UPSERT
        assert plan.key_columns == ["id"]
        assert plan.target_exists is True
        assert plan.key_columns_valid is True
        assert plan.schema_compatible is True

    def test_valid_inputs_without_target(self):
        """Test validation with valid inputs and no target."""
        source_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])

        plan = validate_merge_inputs(
            source_schema=source_schema,
            target_schema=None,
            key_columns=["id"],
            strategy=MergeStrategy.INSERT,
        )

        assert plan.strategy == MergeStrategy.INSERT
        assert plan.key_columns == ["id"]
        assert plan.target_exists is False
        assert plan.key_columns_valid is True
        assert plan.schema_compatible is True

    def test_missing_key_in_source(self):
        """Test error when key column missing from source."""
        source_schema = pa.schema([
            pa.field("name", pa.string()),
            pa.field("value", pa.float64()),
        ])

        with pytest.raises(ValueError, match="Key column.*missing from source.*id"):
            validate_merge_inputs(
                source_schema=source_schema,
                target_schema=None,
                key_columns=["id"],
                strategy=MergeStrategy.INSERT,
            )

    def test_missing_key_in_target(self):
        """Test error when key column missing from target."""
        source_schema = pa.schema([pa.field("id", pa.int64())])
        target_schema = pa.schema([pa.field("name", pa.string())])

        with pytest.raises(ValueError, match="Key column.*missing from target.*id"):
            validate_merge_inputs(
                source_schema=source_schema,
                target_schema=target_schema,
                key_columns=["id"],
                strategy=MergeStrategy.UPSERT,
            )

    def test_incompatible_schema_types(self):
        """Test detection of incompatible schema types."""
        source_schema = pa.schema([pa.field("id", pa.int64())])
        target_schema = pa.schema([pa.field("id", pa.string())])

        plan = validate_merge_inputs(
            source_schema=source_schema,
            target_schema=target_schema,
            key_columns=["id"],
            strategy=MergeStrategy.UPSERT,
        )

        assert plan.schema_compatible is False

    def test_schema_column_mismatch(self):
        """Test detection of schema column mismatches."""
        source_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])
        target_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("different_col", pa.string()),
        ])

        plan = validate_merge_inputs(
            source_schema=source_schema,
            target_schema=target_schema,
            key_columns=["id"],
            strategy=MergeStrategy.UPSERT,
        )

        assert plan.schema_compatible is False

    def test_nullable_key_detection(self):
        """Test detection of nullable key columns."""
        source_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=True),
            pa.field("name", pa.string()),
        ])

        plan = validate_merge_inputs(
            source_schema=source_schema,
            target_schema=None,
            key_columns=["id"],
            strategy=MergeStrategy.UPSERT,
        )

        assert plan.null_keys_detected is True


class TestCheckNullKeys:
    """Test NULL key detection functionality."""

    def test_no_null_keys(self):
        """Test when no NULL keys are present."""
        source_table = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })

        # Should not raise any exception
        check_null_keys(source_table, None, ["id"])

    def test_null_keys_in_source(self):
        """Test detection of NULL keys in source."""
        source_table = pa.table({
            "id": [1, None, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })

        with pytest.raises(ValueError, match="Key column 'id' contains.*NULL values in source"):
            check_null_keys(source_table, None, ["id"])

    def test_null_keys_in_target(self):
        """Test detection of NULL keys in target."""
        source_table = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        target_table = pa.table({
            "id": [4, None, 5],
            "name": ["Dave", "Eve", "Frank"],
        })

        with pytest.raises(ValueError, match="Key column 'id' contains.*NULL values in target"):
            check_null_keys(source_table, target_table, ["id"])

    def test_multiple_key_columns(self):
        """Test NULL detection with multiple key columns."""
        source_table = pa.table({
            "id": [1, 2, 3],
            "date": ["2023-01-01", "2023-01-02", None],
            "name": ["Alice", "Bob", "Charlie"],
        })

        with pytest.raises(ValueError, match="Key column 'date' contains.*NULL values in source"):
            check_null_keys(source_table, None, ["id", "date"])

    def test_no_null_keys_when_target_none(self):
        """Test when target is None (doesn't exist)."""
        source_table = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })

        # Should not raise any exception
        check_null_keys(source_table, None, ["id"])


class TestCalculateMergeStats:
    """Test merge statistics calculation functionality."""

    def test_insert_strategy_stats(self):
        """Test INSERT strategy statistics."""
        stats = calculate_merge_stats(
            strategy=MergeStrategy.INSERT,
            source_count=10,
            target_count_before=5,
            target_count_after=8,
        )

        assert stats.strategy == MergeStrategy.INSERT
        assert stats.source_count == 10
        assert stats.target_count_before == 5
        assert stats.target_count_after == 8
        assert stats.inserted == 3  # 8 - 5
        assert stats.updated == 0
        assert stats.deleted == 0
        assert stats.total_processed == 3

    def test_update_strategy_stats(self):
        """Test UPDATE strategy statistics."""
        stats = calculate_merge_stats(
            strategy=MergeStrategy.UPDATE,
            source_count=5,
            target_count_before=10,
            target_count_after=10,  # UPDATE doesn't change row count
        )

        assert stats.strategy == MergeStrategy.UPDATE
        assert stats.inserted == 0
        assert stats.updated == 10  # All existing potentially updated
        assert stats.deleted == 0
        assert stats.total_processed == 10

    def test_update_strategy_empty_target(self):
        """Test UPDATE strategy with empty target."""
        stats = calculate_merge_stats(
            strategy=MergeStrategy.UPDATE,
            source_count=5,
            target_count_before=0,
            target_count_after=0,
        )

        assert stats.updated == 0  # No existing records to update
        assert stats.total_processed == 0

    def test_full_merge_strategy_stats(self):
        """Test FULL_MERGE strategy statistics."""
        stats = calculate_merge_stats(
            strategy=MergeStrategy.FULL_MERGE,
            source_count=8,
            target_count_before=10,
            target_count_after=8,  # Source replaces target
        )

        assert stats.strategy == MergeStrategy.FULL_MERGE
        assert stats.inserted == 8
        assert stats.updated == 0
        assert stats.deleted == 10
        assert stats.total_processed == 8

    def test_upsert_strategy_stats(self):
        """Test UPSERT strategy statistics."""
        stats = calculate_merge_stats(
            strategy=MergeStrategy.UPSERT,
            source_count=12,
            target_count_before=8,
            target_count_after=10,
        )

        assert stats.strategy == MergeStrategy.UPSERT
        # net change = 10 - 8 = 2, so inserted = 2, updated = 12 - 2 = 10
        assert stats.inserted == 2
        assert stats.updated == 10
        assert stats.deleted == 0
        assert stats.total_processed == 12

    def test_deduplicate_strategy_stats(self):
        """Test DEDUPLICATE strategy statistics."""
        stats = calculate_merge_stats(
            strategy=MergeStrategy.DEDUPLICATE,
            source_count=15,
            target_count_before=10,
            target_count_after=12,
        )

        assert stats.strategy == MergeStrategy.DEDUPLICATE
        # Same logic as UPSERT: net change = 12 - 10 = 2, so inserted = 2, updated = 15 - 2 = 13
        assert stats.inserted == 2
        assert stats.updated == 13
        assert stats.deleted == 0
        assert stats.total_processed == 15


class TestValidateStrategyCompatibility:
    """Test strategy compatibility validation."""

    def test_update_with_empty_source_error(self):
        """Test UPDATE strategy with empty source should raise error."""
        with pytest.raises(ValueError, match="UPDATE strategy requires non-empty source data"):
            validate_strategy_compatibility(
                strategy=MergeStrategy.UPDATE,
                source_count=0,
                target_exists=True,
            )

    def test_update_with_non_empty_source_ok(self):
        """Test UPDATE strategy with non-empty source should be OK."""
        # Should not raise any exception
        validate_strategy_compatibility(
            strategy=MergeStrategy.UPDATE,
            source_count=5,
            target_exists=True,
        )

    def test_full_merge_without_target_ok(self):
        """Test FULL_MERGE strategy without existing target should be OK."""
        # Should not raise any exception
        validate_strategy_compatibility(
            strategy=MergeStrategy.FULL_MERGE,
            source_count=5,
            target_exists=False,
        )

    def test_other_strategies_always_compatible(self):
        """Test that other strategies are always compatible."""
        strategies = [MergeStrategy.UPSERT, MergeStrategy.INSERT, MergeStrategy.DEDUPLICATE]

        for strategy in strategies:
            # Should not raise any exception for any combination
            validate_strategy_compatibility(strategy, 0, False)
            validate_strategy_compatibility(strategy, 0, True)
            validate_strategy_compatibility(strategy, 5, False)
            validate_strategy_compatibility(strategy, 5, True)


class TestIntegration:
    """Integration tests for merge core functionality."""

    def test_complete_merge_validation_flow(self):
        """Test a complete merge validation flow."""
        # Create schemas
        source_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64()),
        ])

        target_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64()),
        ])

        # Validate inputs
        plan = validate_merge_inputs(
            source_schema=source_schema,
            target_schema=target_schema,
            key_columns=["id"],
            strategy=MergeStrategy.UPSERT,
        )

        # Validate strategy compatibility
        validate_strategy_compatibility(
            strategy=plan.strategy,
            source_count=50,
            target_exists=plan.target_exists,
        )

        # Check the plan looks correct
        assert plan.key_columns_valid
        assert plan.schema_compatible
        assert not plan.null_keys_detected

    def test_realistic_merge_scenario(self):
        """Test a realistic merge scenario with actual data."""
        # Create test data
        source_table = pa.table({
            "id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "David"],
            "value": [10.5, 20.0, 15.5, 25.0],
        })

        target_table = pa.table({
            "id": [1, 3, 5],
            "name": ["Alice", "Charlie", "Eve"],
            "value": [10.0, 15.0, 30.0],
        })

        # Test null key checking (should pass)
        check_null_keys(source_table, target_table, ["id"])

        # Test stats calculation for realistic scenario
        stats = calculate_merge_stats(
            strategy=MergeStrategy.UPSERT,
            source_count=4,
            target_count_before=3,
            target_count_after=4,  # Assume 1 insert, 2 updates
        )

        assert stats.source_count == 4
        assert stats.target_count_before == 3
        assert stats.target_count_after == 4
        assert stats.inserted + stats.updated == stats.total_processed

        # Convert to dict and verify format
        stats_dict = stats.to_dict()
        required_keys = {"inserted", "updated", "deleted", "total", "strategy"}
        assert required_keys.issubset(set(stats_dict.keys()))