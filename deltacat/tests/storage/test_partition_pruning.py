"""Tests for DeltaCAT partition pruning functionality."""

import pytest
from typing import List, Dict, Any
from datetime import date, datetime
import pyarrow as pa

from deltacat.storage import Partition, Schema, Field
from deltacat.storage.model.partition import (
    PartitionLocator,
    PartitionScheme,
    PartitionKey,
)
from deltacat.types.media import ContentType
from deltacat.storage.model.partition_pruner import (
    PartitionPruner,
    PartitionFilter,
    FilterOperator,
    PartitionStatistics,
    PruningResult,
)


class TestPartitionPruner:
    """Test suite for partition pruning operations."""

    @pytest.fixture
    def sample_partitions(self) -> List[Partition]:
        """Create sample partitions for testing."""
        partitions = []
        
        # Create a simple schema for testing
        schema = Schema.of([
            Field.of(field=pa.field("id", pa.int64()), field_id=1),
            Field.of(field=pa.field("value", pa.string()), field_id=2),
        ])
        
        # Create partitions with different date and region values
        for year in [2023, 2024]:
            for month in [1, 2, 3]:
                for region in ["us-east", "us-west", "eu-west"]:
                    # PartitionValues is just List[Any]
                    partition_values = [year, month, region]
                    
                    locator = PartitionLocator.of(
                        stream_locator=None,  # Not needed for pruning tests
                        partition_values=partition_values,
                        partition_id=f"{year}-{month}-{region}",
                    )
                    
                    partition = Partition.of(
                        locator=locator,
                        schema=schema,
                        content_types=[ContentType.PARQUET],
                    )
                    # Store values as dict for easier access in pruning
                    partition._column_map = {"year": year, "month": month, "region": region}
                    partitions.append(partition)
        
        return partitions

    @pytest.fixture
    def partition_scheme(self) -> PartitionScheme:
        """Create a partition scheme for testing."""
        keys = [
            PartitionKey.of(key=["year"], name="year"),
            PartitionKey.of(key=["month"], name="month"),
            PartitionKey.of(key=["region"], name="region"),
        ]
        scheme = PartitionScheme.of(keys=keys)
        # Add partition_columns for our pruner
        scheme.partition_columns = ["year", "month", "region"]
        return scheme

    def test_prune_partitions_single_filter(self, sample_partitions, partition_scheme):
        """Test pruning with a single filter condition."""
        # Filter for year = 2024
        filters = [
            PartitionFilter(
                column="year",
                operator=FilterOperator.EQUALS,
                value=2024,
            )
        ]
        
        result = PartitionPruner.prune_partitions(
            partitions=sample_partitions,
            filters=filters,
            partition_scheme=partition_scheme,
        )
        
        # Should only return 2024 partitions (9 total: 3 months * 3 regions)
        assert len(result.selected_partitions) == 9
        for partition in result.selected_partitions:
            assert partition._column_map["year"] == 2024
        
        # Check pruning statistics
        assert result.total_partitions == 18
        assert result.pruned_partitions == 9
        assert result.pruning_percentage == 50.0

    def test_prune_partitions_multiple_filters(self, sample_partitions, partition_scheme):
        """Test pruning with multiple filter conditions (AND logic)."""
        # Filter for year = 2024 AND month = 2
        filters = [
            PartitionFilter(
                column="year",
                operator=FilterOperator.EQUALS,
                value=2024,
            ),
            PartitionFilter(
                column="month",
                operator=FilterOperator.EQUALS,
                value=2,
            ),
        ]
        
        result = PartitionPruner.prune_partitions(
            partitions=sample_partitions,
            filters=filters,
            partition_scheme=partition_scheme,
        )
        
        # Should only return 2024-02 partitions (3 regions)
        assert len(result.selected_partitions) == 3
        for partition in result.selected_partitions:
            assert partition._column_map["year"] == 2024
            assert partition._column_map["month"] == 2

    def test_prune_partitions_range_filter(self, sample_partitions, partition_scheme):
        """Test pruning with range filter conditions."""
        # Filter for month >= 2
        filters = [
            PartitionFilter(
                column="month",
                operator=FilterOperator.GREATER_THAN_OR_EQUAL,
                value=2,
            )
        ]
        
        result = PartitionPruner.prune_partitions(
            partitions=sample_partitions,
            filters=filters,
            partition_scheme=partition_scheme,
        )
        
        # Should return partitions with month 2 and 3 (12 total: 2 years * 2 months * 3 regions)
        assert len(result.selected_partitions) == 12
        for partition in result.selected_partitions:
            assert partition._column_map["month"] >= 2

    def test_prune_partitions_in_filter(self, sample_partitions, partition_scheme):
        """Test pruning with IN filter for multiple values."""
        # Filter for region IN ('us-east', 'us-west')
        filters = [
            PartitionFilter(
                column="region",
                operator=FilterOperator.IN,
                value=["us-east", "us-west"],
            )
        ]
        
        result = PartitionPruner.prune_partitions(
            partitions=sample_partitions,
            filters=filters,
            partition_scheme=partition_scheme,
        )
        
        # Should return only us-east and us-west partitions (12 total: 2 years * 3 months * 2 regions)
        assert len(result.selected_partitions) == 12
        for partition in result.selected_partitions:
            assert partition._column_map["region"] in ["us-east", "us-west"]

    def test_prune_partitions_not_equals_filter(self, sample_partitions, partition_scheme):
        """Test pruning with NOT EQUALS filter."""
        # Filter for region != 'eu-west'
        filters = [
            PartitionFilter(
                column="region",
                operator=FilterOperator.NOT_EQUALS,
                value="eu-west",
            )
        ]
        
        result = PartitionPruner.prune_partitions(
            partitions=sample_partitions,
            filters=filters,
            partition_scheme=partition_scheme,
        )
        
        # Should exclude eu-west partitions (12 total: 2 years * 3 months * 2 regions)
        assert len(result.selected_partitions) == 12
        for partition in result.selected_partitions:
            assert partition._column_map["region"] != "eu-west"

    def test_prune_partitions_between_filter(self, sample_partitions, partition_scheme):
        """Test pruning with BETWEEN filter for range."""
        # Filter for month BETWEEN 1 AND 2
        filters = [
            PartitionFilter(
                column="month",
                operator=FilterOperator.BETWEEN,
                value=(1, 2),
            )
        ]
        
        result = PartitionPruner.prune_partitions(
            partitions=sample_partitions,
            filters=filters,
            partition_scheme=partition_scheme,
        )
        
        # Should return partitions with month 1 or 2 (12 total: 2 years * 2 months * 3 regions)
        assert len(result.selected_partitions) == 12
        for partition in result.selected_partitions:
            month = partition._column_map["month"]
            assert 1 <= month <= 2

    def test_prune_partitions_no_filter(self, sample_partitions, partition_scheme):
        """Test that no filters returns all partitions."""
        result = PartitionPruner.prune_partitions(
            partitions=sample_partitions,
            filters=[],
            partition_scheme=partition_scheme,
        )
        
        # Should return all partitions
        assert len(result.selected_partitions) == len(sample_partitions)
        assert result.pruned_partitions == 0
        assert result.pruning_percentage == 0.0

    def test_prune_partitions_filter_non_partition_column(self, sample_partitions, partition_scheme):
        """Test that filtering on non-partition columns returns all partitions."""
        # Filter on a column that's not a partition column
        filters = [
            PartitionFilter(
                column="non_partition_col",
                operator=FilterOperator.EQUALS,
                value="some_value",
            )
        ]
        
        result = PartitionPruner.prune_partitions(
            partitions=sample_partitions,
            filters=filters,
            partition_scheme=partition_scheme,
        )
        
        # Should return all partitions since we can't prune on non-partition columns
        assert len(result.selected_partitions) == len(sample_partitions)
        assert result.pruned_partitions == 0

    def test_analyze_predicates(self):
        """Test predicate analysis to extract partition filters."""
        predicates = {
            "year": 2024,
            "month": {"$gte": 2},
            "region": {"$in": ["us-east", "us-west"]},
            "non_partition_col": "ignored",
        }
        
        keys = [
            PartitionKey.of(key=["year"], name="year"),
            PartitionKey.of(key=["month"], name="month"),
            PartitionKey.of(key=["region"], name="region"),
        ]
        partition_scheme = PartitionScheme.of(keys=keys)
        partition_scheme.partition_columns = ["year", "month", "region"]
        
        filters = PartitionPruner.analyze_predicates(predicates, partition_scheme)
        
        # Should extract 3 partition filters
        assert len(filters) == 3
        
        # Check year filter
        year_filter = next(f for f in filters if f.column == "year")
        assert year_filter.operator == FilterOperator.EQUALS
        assert year_filter.value == 2024
        
        # Check month filter
        month_filter = next(f for f in filters if f.column == "month")
        assert month_filter.operator == FilterOperator.GREATER_THAN_OR_EQUAL
        assert month_filter.value == 2
        
        # Check region filter
        region_filter = next(f for f in filters if f.column == "region")
        assert region_filter.operator == FilterOperator.IN
        assert region_filter.value == ["us-east", "us-west"]

    def test_get_partition_statistics(self, sample_partitions):
        """Test getting statistics about partitions."""
        # Update partitions to use column map for statistics
        for partition in sample_partitions:
            # Convert _column_map to partition_values dict-like access
            partition.locator.partition_values = partition._column_map
            
        stats = PartitionPruner.get_partition_statistics(sample_partitions)
        
        # Check basic counts
        assert stats.total_partitions == 18
        
        # Check value distributions
        assert stats.unique_values_per_column["year"] == {2023, 2024}
        assert stats.unique_values_per_column["month"] == {1, 2, 3}
        assert stats.unique_values_per_column["region"] == {"us-east", "us-west", "eu-west"}
        
        # Check min/max values
        assert stats.min_values["year"] == 2023
        assert stats.max_values["year"] == 2024
        assert stats.min_values["month"] == 1
        assert stats.max_values["month"] == 3

    def test_complex_pruning_scenario(self, sample_partitions, partition_scheme):
        """Test complex pruning with multiple conditions."""
        # Complex filter: year = 2024 AND month IN (1, 3)
        filters = [
            PartitionFilter(
                column="year",
                operator=FilterOperator.EQUALS,
                value=2024,
            ),
            PartitionFilter(
                column="month",
                operator=FilterOperator.IN,
                value=[1, 3],
            ),
        ]
        
        result = PartitionPruner.prune_partitions(
            partitions=sample_partitions,
            filters=filters,
            partition_scheme=partition_scheme,
        )
        
        # Should return 6 partitions (2024 with months 1 and 3, all regions)
        assert len(result.selected_partitions) == 6
        
        for partition in result.selected_partitions:
            assert partition._column_map["year"] == 2024
            assert partition._column_map["month"] in [1, 3]

    def test_empty_partitions_list(self):
        """Test pruning with empty partitions list."""
        keys = [PartitionKey.of(key=["year"], name="year")]
        partition_scheme = PartitionScheme.of(keys=keys)
        partition_scheme.partition_columns = ["year"]
        
        result = PartitionPruner.prune_partitions(
            partitions=[],
            filters=[
                PartitionFilter(
                    column="year",
                    operator=FilterOperator.EQUALS,
                    value=2024,
                )
            ],
            partition_scheme=partition_scheme,
        )
        
        assert len(result.selected_partitions) == 0
        assert result.total_partitions == 0
        assert result.pruned_partitions == 0
        assert result.pruning_percentage == 0.0

    def test_null_value_handling(self):
        """Test handling of null values in partition columns."""
        # Create partitions with some null values
        partitions = []
        
        # Create a simple schema for testing
        schema = Schema.of([
            Field.of(field=pa.field("id", pa.int64()), field_id=1),
            Field.of(field=pa.field("value", pa.string()), field_id=2),
        ])
        
        locator1 = PartitionLocator.of(
            stream_locator=None,
            partition_values=[2024, None, "us-east"],
            partition_id="2024-null-us-east",
        )
        partition1 = Partition.of(
            locator=locator1,
            schema=schema,
            content_types=[ContentType.PARQUET],
        )
        partition1._column_map = {"year": 2024, "month": None, "region": "us-east"}
        partitions.append(partition1)
        
        locator2 = PartitionLocator.of(
            stream_locator=None,
            partition_values=[2024, 1, "us-east"],
            partition_id="2024-1-us-east",
        )
        partition2 = Partition.of(
            locator=locator2,
            schema=schema,
            content_types=[ContentType.PARQUET],
        )
        partition2._column_map = {"year": 2024, "month": 1, "region": "us-east"}
        partitions.append(partition2)
        
        keys = [
            PartitionKey.of(key=["year"], name="year"),
            PartitionKey.of(key=["month"], name="month"),
            PartitionKey.of(key=["region"], name="region"),
        ]
        partition_scheme = PartitionScheme.of(keys=keys)
        partition_scheme.partition_columns = ["year", "month", "region"]
        
        # Filter for month = 1 should exclude null values
        filters = [
            PartitionFilter(
                column="month",
                operator=FilterOperator.EQUALS,
                value=1,
            )
        ]
        
        result = PartitionPruner.prune_partitions(
            partitions=partitions,
            filters=filters,
            partition_scheme=partition_scheme,
        )
        
        assert len(result.selected_partitions) == 1
        assert result.selected_partitions[0]._column_map["month"] == 1

    def test_is_null_filter(self):
        """Test IS_NULL filter operator."""
        # Create partitions with some null values
        partitions = []
        
        # Create a simple schema for testing
        schema = Schema.of([
            Field.of(field=pa.field("id", pa.int64()), field_id=1),
            Field.of(field=pa.field("value", pa.string()), field_id=2),
        ])
        
        locator1 = PartitionLocator.of(
            stream_locator=None,
            partition_values=[2024, None, "us-east"],
            partition_id="null-partition",
        )
        partition1 = Partition.of(
            locator=locator1,
            schema=schema,
            content_types=[ContentType.PARQUET],
        )
        partition1._column_map = {"year": 2024, "month": None, "region": "us-east"}
        partitions.append(partition1)
        
        locator2 = PartitionLocator.of(
            stream_locator=None,
            partition_values=[2024, 1, "us-east"],
            partition_id="non-null-partition",
        )
        partition2 = Partition.of(
            locator=locator2,
            schema=schema,
            content_types=[ContentType.PARQUET],
        )
        partition2._column_map = {"year": 2024, "month": 1, "region": "us-east"}
        partitions.append(partition2)
        
        keys = [
            PartitionKey.of(key=["year"], name="year"),
            PartitionKey.of(key=["month"], name="month"),
            PartitionKey.of(key=["region"], name="region"),
        ]
        partition_scheme = PartitionScheme.of(keys=keys)
        partition_scheme.partition_columns = ["year", "month", "region"]
        
        # Filter for month IS NULL
        filters = [
            PartitionFilter(
                column="month",
                operator=FilterOperator.IS_NULL,
                value=None,
            )
        ]
        
        result = PartitionPruner.prune_partitions(
            partitions=partitions,
            filters=filters,
            partition_scheme=partition_scheme,
        )
        
        assert len(result.selected_partitions) == 1
        assert result.selected_partitions[0]._column_map["month"] is None