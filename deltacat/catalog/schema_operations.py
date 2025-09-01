"""Catalog-level schema operations using the core schema evolution engine.

This module provides high-level schema evolution operations at the catalog level,
coordinating between the catalog metadata and the underlying storage layer.
"""

from typing import List, Optional, Dict, Any
import logging

from deltacat import logs
from deltacat.catalog import get_table, alter_table
from deltacat.storage.model.schema_evolution import (
    SchemaEvolution,
    SchemaOperation,
    SchemaOperationType,
    SchemaEvolutionError,
)
from deltacat.storage import Schema, Field

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def evolve_table_schema(
    table: str,
    namespace: str,
    operations: List[SchemaOperation],
    catalog: Optional[str] = None,
    validate_compatibility: bool = True,
    **kwargs,
) -> None:
    """Apply schema evolution operations to a table.
    
    This function coordinates schema evolution at the catalog level, ensuring
    that changes are properly reflected in both catalog metadata and storage.
    
    Args:
        table: Name of the table to evolve
        namespace: Namespace containing the table
        operations: List of schema operations to apply
        catalog: Optional catalog name
        validate_compatibility: Whether to validate backward compatibility
        **kwargs: Additional arguments for alter_table
        
    Raises:
        SchemaEvolutionError: If evolution fails
        ValueError: If table doesn't exist
    """
    # Get current table definition
    table_def = get_table(
        name=table,
        namespace=namespace,
        catalog=catalog,
    )
    
    if not table_def:
        raise ValueError(f"Table {namespace}.{table} does not exist")
    
    current_schema = table_def.table_version.schema
    
    # Apply schema evolution operations
    evolved_schema = SchemaEvolution.apply_operations(current_schema, operations)
    
    # Validate backward compatibility if requested
    if validate_compatibility:
        if not SchemaEvolution.is_backward_compatible(current_schema, evolved_schema):
            logger.warning(
                f"Schema evolution for {namespace}.{table} may break backward compatibility"
            )
    
    # Build schema_updates dictionary for alter_table
    # This will eventually be passed to storage layer's update_table_version
    schema_updates = {
        "schema": evolved_schema,
        "operations": [
            {
                "type": op.type.value,
                "field_name": op.field_name,
                "new_name": op.new_name,
                "field_id": op.field.id if op.field else None,
            }
            for op in operations
        ],
    }
    
    # Apply changes through alter_table
    alter_table(
        table=table,
        namespace=namespace,
        catalog=catalog,
        schema_updates=schema_updates,
        **kwargs,
    )
    
    logger.info(
        f"Successfully evolved schema for {namespace}.{table} with {len(operations)} operations"
    )


def add_table_column(
    table: str,
    namespace: str,
    field: Field,
    catalog: Optional[str] = None,
    **kwargs,
) -> None:
    """Add a column to a table.
    
    Args:
        table: Name of the table
        namespace: Namespace containing the table
        field: Field to add
        catalog: Optional catalog name
        **kwargs: Additional arguments for alter_table
    """
    operation = SchemaOperation(
        type=SchemaOperationType.ADD_COLUMN,
        field=field,
    )
    evolve_table_schema(
        table=table,
        namespace=namespace,
        operations=[operation],
        catalog=catalog,
        **kwargs,
    )


def drop_table_column(
    table: str,
    namespace: str,
    column_name: str,
    force: bool = False,
    catalog: Optional[str] = None,
    **kwargs,
) -> None:
    """Drop a column from a table.
    
    Args:
        table: Name of the table
        namespace: Namespace containing the table
        column_name: Name of column to drop
        force: Force drop even if non-nullable
        catalog: Optional catalog name
        **kwargs: Additional arguments for alter_table
    """
    operation = SchemaOperation(
        type=SchemaOperationType.DROP_COLUMN,
        field_name=column_name,
        force=force,
    )
    evolve_table_schema(
        table=table,
        namespace=namespace,
        operations=[operation],
        catalog=catalog,
        **kwargs,
    )


def rename_table_column(
    table: str,
    namespace: str,
    old_name: str,
    new_name: str,
    catalog: Optional[str] = None,
    **kwargs,
) -> None:
    """Rename a column in a table.
    
    Args:
        table: Name of the table
        namespace: Namespace containing the table
        old_name: Current column name
        new_name: New column name
        catalog: Optional catalog name
        **kwargs: Additional arguments for alter_table
    """
    operation = SchemaOperation(
        type=SchemaOperationType.RENAME_COLUMN,
        field_name=old_name,
        new_name=new_name,
    )
    evolve_table_schema(
        table=table,
        namespace=namespace,
        operations=[operation],
        catalog=catalog,
        **kwargs,
    )


def change_table_column_type(
    table: str,
    namespace: str,
    column_name: str,
    new_type: Any,
    force: bool = False,
    catalog: Optional[str] = None,
    **kwargs,
) -> None:
    """Change the type of a column in a table.
    
    Args:
        table: Name of the table
        namespace: Namespace containing the table
        column_name: Name of column to modify
        new_type: New PyArrow type for the column
        force: Force type change even if incompatible
        catalog: Optional catalog name
        **kwargs: Additional arguments for alter_table
    """
    operation = SchemaOperation(
        type=SchemaOperationType.CHANGE_TYPE,
        field_name=column_name,
        new_type=new_type,
        force=force,
    )
    evolve_table_schema(
        table=table,
        namespace=namespace,
        operations=[operation],
        catalog=catalog,
        **kwargs,
    )


def get_schema_evolution_plan(
    table: str,
    namespace: str,
    target_schema: Schema,
    catalog: Optional[str] = None,
) -> List[SchemaOperation]:
    """Generate a plan to evolve from current schema to target schema.
    
    This function analyzes the difference between current and target schemas
    and generates the necessary operations to transform one into the other.
    
    Args:
        table: Name of the table
        namespace: Namespace containing the table
        target_schema: Desired target schema
        catalog: Optional catalog name
        
    Returns:
        List of schema operations to apply
        
    Raises:
        ValueError: If table doesn't exist
    """
    # Get current table definition
    table_def = get_table(
        name=table,
        namespace=namespace,
        catalog=catalog,
    )
    
    if not table_def:
        raise ValueError(f"Table {namespace}.{table} does not exist")
    
    current_schema = table_def.table_version.schema
    
    # Get schema difference
    diff = SchemaEvolution.get_schema_difference(current_schema, target_schema)
    
    operations = []
    
    # Generate operations for dropped fields
    for field in diff.dropped_fields:
        operations.append(
            SchemaOperation(
                type=SchemaOperationType.DROP_COLUMN,
                field_name=field.arrow.name,
            )
        )
    
    # Generate operations for renamed fields
    for old_name, new_name in diff.renamed_fields:
        operations.append(
            SchemaOperation(
                type=SchemaOperationType.RENAME_COLUMN,
                field_name=old_name,
                new_name=new_name,
            )
        )
    
    # Generate operations for type changes
    for field_name, old_type, new_type in diff.type_changes:
        operations.append(
            SchemaOperation(
                type=SchemaOperationType.CHANGE_TYPE,
                field_name=field_name,
                new_type=new_type,
                force=True,  # Type changes detected in diff should be applied
            )
        )
    
    # Generate operations for added fields
    for field in diff.added_fields:
        operations.append(
            SchemaOperation(
                type=SchemaOperationType.ADD_COLUMN,
                field=field,
            )
        )
    
    return operations