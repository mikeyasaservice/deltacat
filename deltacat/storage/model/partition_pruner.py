"""Core partition pruning engine for DeltaCAT.

This module provides the core logic for pruning partitions based on predicates,
enabling efficient data skipping across all DeltaCAT operations.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import logging

from deltacat import logs
from deltacat.storage import Partition, PartitionValues
from deltacat.storage.model.partition import PartitionScheme

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class FilterOperator(Enum):
    """Supported filter operators for partition pruning."""
    EQUALS = "="
    NOT_EQUALS = "!="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    IN = "in"
    NOT_IN = "not_in"
    BETWEEN = "between"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


@dataclass
class PartitionFilter:
    """Represents a single filter condition on a partition column."""
    column: str
    operator: FilterOperator
    value: Any  # Can be single value, list (for IN), or tuple (for BETWEEN)


@dataclass
class PruningResult:
    """Results from partition pruning operation."""
    selected_partitions: List[Partition]
    total_partitions: int
    pruned_partitions: int
    pruning_percentage: float


@dataclass
class PartitionStatistics:
    """Statistics about partition values."""
    total_partitions: int
    unique_values_per_column: Dict[str, Set[Any]]
    min_values: Dict[str, Any]
    max_values: Dict[str, Any]
    null_counts: Dict[str, int]


class PartitionPruner:
    """Core partition pruning logic for all DeltaCAT operations."""

    @classmethod
    def prune_partitions(
        cls,
        partitions: List[Partition],
        filters: List[PartitionFilter],
        partition_scheme: PartitionScheme,
    ) -> PruningResult:
        """Prune partitions based on filter conditions.
        
        Args:
            partitions: List of partitions to prune
            filters: List of filter conditions (combined with AND logic)
            partition_scheme: Partition scheme defining partition columns
            
        Returns:
            PruningResult with selected partitions and statistics
        """
        if not partitions:
            return PruningResult(
                selected_partitions=[],
                total_partitions=0,
                pruned_partitions=0,
                pruning_percentage=0.0,
            )
        
        total_partitions = len(partitions)
        selected_partitions = []
        
        # Get partition columns from scheme
        partition_columns = set(partition_scheme.partition_columns or [])
        
        for partition in partitions:
            if cls._partition_matches_filters(
                partition, filters, partition_columns
            ):
                selected_partitions.append(partition)
        
        pruned_count = total_partitions - len(selected_partitions)
        pruning_percentage = (
            (pruned_count / total_partitions * 100) if total_partitions > 0 else 0.0
        )
        
        logger.debug(
            f"Pruned {pruned_count}/{total_partitions} partitions "
            f"({pruning_percentage:.1f}% reduction)"
        )
        
        return PruningResult(
            selected_partitions=selected_partitions,
            total_partitions=total_partitions,
            pruned_partitions=pruned_count,
            pruning_percentage=pruning_percentage,
        )

    @classmethod
    def _partition_matches_filters(
        cls,
        partition: Partition,
        filters: List[PartitionFilter],
        partition_columns: Set[str],
    ) -> bool:
        """Check if a partition matches all filter conditions.
        
        Args:
            partition: Partition to check
            filters: List of filters (AND logic)
            partition_columns: Set of valid partition column names
            
        Returns:
            True if partition matches all filters
        """
        if not filters:
            return True
        
        # Try to get partition values from _column_map (for testing) or partition_values
        if hasattr(partition, '_column_map'):
            partition_values = partition._column_map
        elif isinstance(partition.locator.partition_values, dict):
            partition_values = partition.locator.partition_values
        else:
            # If partition_values is a list, we need column mapping
            # This would require the partition scheme to map indices to column names
            logger.warning("Partition values are list without column mapping")
            return True
        
        for filter_obj in filters:
            # Skip filters on non-partition columns
            if filter_obj.column not in partition_columns:
                logger.debug(
                    f"Skipping filter on non-partition column: {filter_obj.column}"
                )
                continue
            
            partition_value = partition_values.get(filter_obj.column)
            
            if not cls._value_matches_filter(partition_value, filter_obj):
                return False
        
        return True

    @classmethod
    def _value_matches_filter(
        cls,
        value: Any,
        filter_obj: PartitionFilter,
    ) -> bool:
        """Check if a value matches a filter condition.
        
        Args:
            value: Value to check (can be None)
            filter_obj: Filter condition
            
        Returns:
            True if value matches filter
        """
        operator = filter_obj.operator
        filter_value = filter_obj.value
        
        # Handle NULL checks
        if operator == FilterOperator.IS_NULL:
            return value is None
        elif operator == FilterOperator.IS_NOT_NULL:
            return value is not None
        
        # For other operators, NULL values don't match
        if value is None:
            return False
        
        # Apply operator
        if operator == FilterOperator.EQUALS:
            return value == filter_value
        elif operator == FilterOperator.NOT_EQUALS:
            return value != filter_value
        elif operator == FilterOperator.LESS_THAN:
            return value < filter_value
        elif operator == FilterOperator.LESS_THAN_OR_EQUAL:
            return value <= filter_value
        elif operator == FilterOperator.GREATER_THAN:
            return value > filter_value
        elif operator == FilterOperator.GREATER_THAN_OR_EQUAL:
            return value >= filter_value
        elif operator == FilterOperator.IN:
            return value in filter_value
        elif operator == FilterOperator.NOT_IN:
            return value not in filter_value
        elif operator == FilterOperator.BETWEEN:
            # filter_value should be a tuple (min, max)
            min_val, max_val = filter_value
            return min_val <= value <= max_val
        else:
            logger.warning(f"Unknown filter operator: {operator}")
            return True  # Unknown operators don't filter

    @classmethod
    def analyze_predicates(
        cls,
        predicates: Dict[str, Any],
        partition_scheme: PartitionScheme,
    ) -> List[PartitionFilter]:
        """Analyze predicates to extract partition filters.
        
        Args:
            predicates: Dictionary of column predicates
            partition_scheme: Partition scheme defining partition columns
            
        Returns:
            List of partition filters extracted from predicates
        """
        filters = []
        partition_columns = set(partition_scheme.partition_columns or [])
        
        for column, predicate in predicates.items():
            # Only process partition columns
            if column not in partition_columns:
                continue
            
            if isinstance(predicate, dict):
                # Complex predicate with operator
                for op_str, value in predicate.items():
                    if op_str == "$eq":
                        filters.append(
                            PartitionFilter(
                                column=column,
                                operator=FilterOperator.EQUALS,
                                value=value,
                            )
                        )
                    elif op_str == "$ne":
                        filters.append(
                            PartitionFilter(
                                column=column,
                                operator=FilterOperator.NOT_EQUALS,
                                value=value,
                            )
                        )
                    elif op_str == "$lt":
                        filters.append(
                            PartitionFilter(
                                column=column,
                                operator=FilterOperator.LESS_THAN,
                                value=value,
                            )
                        )
                    elif op_str == "$lte":
                        filters.append(
                            PartitionFilter(
                                column=column,
                                operator=FilterOperator.LESS_THAN_OR_EQUAL,
                                value=value,
                            )
                        )
                    elif op_str == "$gt":
                        filters.append(
                            PartitionFilter(
                                column=column,
                                operator=FilterOperator.GREATER_THAN,
                                value=value,
                            )
                        )
                    elif op_str == "$gte":
                        filters.append(
                            PartitionFilter(
                                column=column,
                                operator=FilterOperator.GREATER_THAN_OR_EQUAL,
                                value=value,
                            )
                        )
                    elif op_str == "$in":
                        filters.append(
                            PartitionFilter(
                                column=column,
                                operator=FilterOperator.IN,
                                value=value,
                            )
                        )
                    elif op_str == "$nin":
                        filters.append(
                            PartitionFilter(
                                column=column,
                                operator=FilterOperator.NOT_IN,
                                value=value,
                            )
                        )
                    elif op_str == "$between":
                        filters.append(
                            PartitionFilter(
                                column=column,
                                operator=FilterOperator.BETWEEN,
                                value=tuple(value),
                            )
                        )
                    elif op_str == "$null":
                        filters.append(
                            PartitionFilter(
                                column=column,
                                operator=FilterOperator.IS_NULL if value else FilterOperator.IS_NOT_NULL,
                                value=None,
                            )
                        )
            else:
                # Simple equality predicate
                filters.append(
                    PartitionFilter(
                        column=column,
                        operator=FilterOperator.EQUALS,
                        value=predicate,
                    )
                )
        
        return filters

    @classmethod
    def get_partition_statistics(
        cls,
        partitions: List[Partition],
    ) -> PartitionStatistics:
        """Get statistics about partition values.
        
        Args:
            partitions: List of partitions to analyze
            
        Returns:
            PartitionStatistics with value distributions and ranges
        """
        if not partitions:
            return PartitionStatistics(
                total_partitions=0,
                unique_values_per_column={},
                min_values={},
                max_values={},
                null_counts={},
            )
        
        unique_values = {}
        min_values = {}
        max_values = {}
        null_counts = {}
        
        # Collect all partition column names
        all_columns = set()
        for partition in partitions:
            if hasattr(partition, '_column_map'):
                all_columns.update(partition._column_map.keys())
            elif isinstance(partition.locator.partition_values, dict):
                all_columns.update(partition.locator.partition_values.keys())
        
        # Initialize collections for each column
        for column in all_columns:
            unique_values[column] = set()
            null_counts[column] = 0
            min_values[column] = None
            max_values[column] = None
        
        # Analyze partition values
        for partition in partitions:
            if hasattr(partition, '_column_map'):
                partition_values = partition._column_map
            elif isinstance(partition.locator.partition_values, dict):
                partition_values = partition.locator.partition_values
            else:
                continue  # Skip if we can't get column mapping
            
            for column in all_columns:
                value = partition_values.get(column)
                
                if value is None:
                    null_counts[column] += 1
                else:
                    unique_values[column].add(value)
                    
                    # Update min/max
                    if min_values[column] is None or (
                        value is not None and value < min_values[column]
                    ):
                        min_values[column] = value
                    
                    if max_values[column] is None or (
                        value is not None and value > max_values[column]
                    ):
                        max_values[column] = value
        
        return PartitionStatistics(
            total_partitions=len(partitions),
            unique_values_per_column=unique_values,
            min_values=min_values,
            max_values=max_values,
            null_counts=null_counts,
        )

    @classmethod
    def optimize_filter_order(
        cls,
        filters: List[PartitionFilter],
        statistics: PartitionStatistics,
    ) -> List[PartitionFilter]:
        """Optimize filter order for better pruning performance.
        
        Filters with higher selectivity (fewer matching values) should be
        evaluated first for better short-circuit evaluation.
        
        Args:
            filters: List of filters to optimize
            statistics: Partition statistics for selectivity estimation
            
        Returns:
            Reordered list of filters
        """
        if len(filters) <= 1:
            return filters
        
        # Estimate selectivity for each filter
        filter_selectivities = []
        
        for filter_obj in filters:
            column = filter_obj.column
            unique_values = statistics.unique_values_per_column.get(column, set())
            
            if not unique_values:
                selectivity = 1.0  # No information, assume low selectivity
            elif filter_obj.operator == FilterOperator.EQUALS:
                # Single value match
                selectivity = 1 / max(len(unique_values), 1)
            elif filter_obj.operator == FilterOperator.IN:
                # Multiple value match
                match_count = len(
                    set(filter_obj.value) & unique_values
                )
                selectivity = match_count / max(len(unique_values), 1)
            elif filter_obj.operator in [
                FilterOperator.LESS_THAN,
                FilterOperator.LESS_THAN_OR_EQUAL,
                FilterOperator.GREATER_THAN,
                FilterOperator.GREATER_THAN_OR_EQUAL,
            ]:
                # Range filter - estimate based on value distribution
                # Simple heuristic: assume 50% selectivity for range filters
                selectivity = 0.5
            else:
                # Default selectivity for other operators
                selectivity = 0.7
            
            filter_selectivities.append((filter_obj, selectivity))
        
        # Sort by selectivity (lower is better)
        filter_selectivities.sort(key=lambda x: x[1])
        
        return [f[0] for f in filter_selectivities]