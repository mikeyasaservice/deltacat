"""Tests for DeltaCAT partition pruning functionality."""

import pytest
from typing import List, Dict, Any
from datetime import date, datetime
import pyarrow as pa

from deltacat.storage import Partition, PartitionValues
from deltacat.storage.model.partition_pruner import (
    PartitionPruner,
    PartitionFilter,
    FilterOperator,
    PartitionStatistics,
    PruningResult,
)
from deltacat.storage.model.partition import PartitionLocator, PartitionScheme


class TestPartitionPruner:
    """Test suite for partition pruning operations."""

    @pytest.fixture
    def sample_partitions(self) -> List[Partition]:
        """Create sample partitions for testing."""
        partitions = []
        
        # Create partitions with different date and region values
        for year in [2023, 2024]:
            for month in [1, 2, 3]:
                for region in ["us-east", "us-west", "eu-west"]:
                    partition_values = PartitionValues.of({
                        "year": year,
                        "month": month,
                        "region": region,
                    })
                    
                    locator = PartitionLocator.of(
                        stream_locator=None,  # Not needed for pruning tests
                        partition_values=partition_values,
                        partition_id=f"{year}-{month}-{region}",
                    )
                    
                    partition = Partition.of(locator=locator)
                    partitions.append(partition)
        
        return partitions

    @pytest.fixture
    def partition_scheme(self) -> PartitionScheme:
        """Create a partition scheme for testing."""
        return PartitionScheme.of(
            partition_columns=["year", "month", "region"],
            partition_types={
                "year": pa.int32(),
                "month": pa.int32(),
                "region": pa.string(),
            },
        )

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
            assert partition.locator.partition_values["year"] == 2024
        
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
            assert partition.locator.partition_values["year"] == 2024
            assert partition.locator.partition_values["month"] == 2

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
            assert partition.locator.partition_values["month"] >= 2

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
            assert partition.locator.partition_values["region"] in ["us-east", "us-west"]

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
            assert partition.locator.partition_values["region"] != "eu-west"

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
            month = partition.locator.partition_values["month"]
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
        
        partition_scheme = PartitionScheme.of(
            partition_columns=["year", "month", "region"],
        )
        
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
        # Complex filter: (year = 2024 AND month IN (1, 3)) OR (year = 2023 AND region = 'eu-west')
        # Note: This tests the AND logic within filter sets
        
        # First condition set: year = 2024 AND month IN (1, 3)
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
            assert partition.locator.partition_values["year"] == 2024
            assert partition.locator.partition_values["month"] in [1, 3]

    def test_empty_partitions_list(self, partition_scheme):
        """Test pruning with empty partitions list."""
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

    def test_null_value_handling(self, partition_scheme):
        """Test handling of null values in partition columns."""
        # Create partitions with some null values
        partitions = [
            Partition.of(
                locator=PartitionLocator.of(
                    stream_locator=None,
                    partition_values=PartitionValues.of({
                        "year": 2024,
                        "month": None,  # Null value
                        "region": "us-east",
                    }),
                    partition_id="2024-null-us-east",
                )
            ),
            Partition.of(
                locator=PartitionLocator.of(
                    stream_locator=None,
                    partition_values=PartitionValues.of({
                        "year": 2024,
                        "month": 1,
                        "region": "us-east",
                    }),
                    partition_id="2024-1-us-east",
                )
            ),
        ]
        
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
        assert result.selected_partitions[0].locator.partition_values["month"] == 1

    def test_is_null_filter(self, partition_scheme):
        """Test IS_NULL filter operator."""
        # Create partitions with some null values
        partitions = [
            Partition.of(
                locator=PartitionLocator.of(
                    stream_locator=None,
                    partition_values=PartitionValues.of({
                        "year": 2024,
                        "month": None,
                        "region": "us-east",
                    }),
                    partition_id="null-partition",
                )
            ),
            Partition.of(
                locator=PartitionLocator.of(
                    stream_locator=None,
                    partition_values=PartitionValues.of({
                        "year": 2024,
                        "month": 1,
                        "region": "us-east",
                    }),
                    partition_id="non-null-partition",
                )
            ),
        ]
        
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
        assert result.selected_partitions[0].locator.partition_values["month"] is None