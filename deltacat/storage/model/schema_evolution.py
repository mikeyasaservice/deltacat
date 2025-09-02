"""Core schema evolution engine for DeltaCAT.

This module provides the core logic for evolving table schemas across all
DeltaCAT operations, not just SQL. It handles adding, dropping, renaming,
and modifying columns while maintaining backward compatibility and data integrity.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import pyarrow as pa

from deltacat.storage import Schema, Field


class SchemaEvolutionError(Exception):
    """Base exception for schema evolution errors."""
    pass


class IncompatibleSchemaChangeError(SchemaEvolutionError):
    """Exception raised when a schema change would break compatibility."""
    pass


class SchemaOperationType(Enum):
    """Types of schema evolution operations."""
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    RENAME_COLUMN = "rename_column"
    CHANGE_TYPE = "change_type"
    ADD_NESTED_FIELD = "add_nested_field"


@dataclass
class SchemaOperation:
    """Represents a single schema evolution operation."""
    type: SchemaOperationType
    field: Optional[Field] = None
    field_name: Optional[str] = None
    new_name: Optional[str] = None
    new_type: Optional[pa.DataType] = None
    force: bool = False
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class SchemaDifference:
    """Represents the difference between two schemas."""
    added_fields: List[Field]
    dropped_fields: List[Field]
    renamed_fields: List[Tuple[str, str]]
    type_changes: List[Tuple[str, pa.DataType, pa.DataType]]
    metadata_changes: List[Tuple[str, Dict, Dict]]


class SchemaEvolution:
    """Core schema evolution logic for all DeltaCAT operations."""

    # Type compatibility rules for safe conversions
    COMPATIBLE_TYPE_CONVERSIONS = {
        # Numeric widenings
        (pa.int8(), pa.int16()): True,
        (pa.int8(), pa.int32()): True,
        (pa.int8(), pa.int64()): True,
        (pa.int16(), pa.int32()): True,
        (pa.int16(), pa.int64()): True,
        (pa.int32(), pa.int64()): True,
        (pa.uint8(), pa.uint16()): True,
        (pa.uint8(), pa.uint32()): True,
        (pa.uint8(), pa.uint64()): True,
        (pa.uint16(), pa.uint32()): True,
        (pa.uint16(), pa.uint64()): True,
        (pa.uint32(), pa.uint64()): True,
        (pa.float32(), pa.float64()): True,
        # Int to float conversions
        (pa.int8(), pa.float32()): True,
        (pa.int8(), pa.float64()): True,
        (pa.int16(), pa.float32()): True,
        (pa.int16(), pa.float64()): True,
        (pa.int32(), pa.float64()): True,
        # Date/time conversions
        (pa.date32(), pa.date64()): True,
        (pa.timestamp('s'), pa.timestamp('ms')): True,
        (pa.timestamp('ms'), pa.timestamp('us')): True,
        (pa.timestamp('us'), pa.timestamp('ns')): True,
    }

    @classmethod
    def add_column(
        cls,
        schema: Schema,
        field: Field,
        position: Optional[int] = None
    ) -> Schema:
        """Add a new column to the schema.
        
        Args:
            schema: The existing schema
            field: The new field to add
            position: Optional position to insert the field (default: append)
            
        Returns:
            New schema with the field added
            
        Raises:
            SchemaEvolutionError: If field name or ID already exists
        """
        # Check for duplicate field name
        existing_names = {f.arrow.name for f in schema.fields}
        if field.arrow.name in existing_names:
            raise SchemaEvolutionError(
                f"Field '{field.arrow.name}' already exists in schema"
            )
        
        # Check for duplicate field ID
        existing_ids = {f.id for f in schema.fields}
        if field.id in existing_ids:
            raise SchemaEvolutionError(
                f"Field ID {field.id} already exists in schema"
            )
        
        # Create new field list
        new_fields = list(schema.fields)
        if position is not None:
            new_fields.insert(position, field)
        else:
            new_fields.append(field)
        
        return Schema.of(new_fields)

    @classmethod
    def drop_column(
        cls,
        schema: Schema,
        field_name: str,
        force: bool = False
    ) -> Schema:
        """Drop a column from the schema.
        
        Args:
            schema: The existing schema
            field_name: Name of the field to drop
            force: Force drop even if field is non-nullable
            
        Returns:
            New schema with the field removed
            
        Raises:
            SchemaEvolutionError: If field doesn't exist
            IncompatibleSchemaChangeError: If dropping non-nullable field without force
        """
        # Find the field
        field_to_drop = None
        for field in schema.fields:
            if field.arrow.name == field_name:
                field_to_drop = field
                break
        
        if field_to_drop is None:
            raise SchemaEvolutionError(f"Field '{field_name}' not found in schema")
        
        # Check if field is non-nullable
        if not field_to_drop.arrow.nullable and not force:
            raise IncompatibleSchemaChangeError(
                f"Cannot drop non-nullable field '{field_name}' without force flag"
            )
        
        # Create new field list without the dropped field
        new_fields = [f for f in schema.fields if f.arrow.name != field_name]
        
        return Schema.of(new_fields)

    @classmethod
    def rename_column(
        cls,
        schema: Schema,
        old_name: str,
        new_name: str
    ) -> Schema:
        """Rename a column in the schema.
        
        Args:
            schema: The existing schema
            old_name: Current name of the field
            new_name: New name for the field
            
        Returns:
            New schema with the field renamed
            
        Raises:
            SchemaEvolutionError: If old field doesn't exist or new name already exists
        """
        # Check old field exists
        old_field = None
        for field in schema.fields:
            if field.arrow.name == old_name:
                old_field = field
                break
        
        if old_field is None:
            raise SchemaEvolutionError(f"Field '{old_name}' not found in schema")
        
        # Check new name doesn't exist
        existing_names = {f.arrow.name for f in schema.fields}
        if new_name in existing_names:
            raise SchemaEvolutionError(
                f"Field '{new_name}' already exists in schema"
            )
        
        # Create new field list with renamed field
        new_fields = []
        for field in schema.fields:
            if field.arrow.name == old_name:
                # Create new field with same properties but new name
                renamed_field = Field.of(
                    field=pa.field(new_name, field.arrow.type, field.arrow.nullable),
                    field_id=field.id,
                )
                # Preserve metadata
                if hasattr(field, 'metadata'):
                    renamed_field.metadata = field.metadata
                new_fields.append(renamed_field)
            else:
                new_fields.append(field)
        
        return Schema.of(new_fields)

    @classmethod
    def change_column_type(
        cls,
        schema: Schema,
        field_name: str,
        new_type: pa.DataType,
        force: bool = False
    ) -> Schema:
        """Change the type of a column.
        
        Args:
            schema: The existing schema
            field_name: Name of the field to modify
            new_type: New type for the field
            force: Force type change even if incompatible
            
        Returns:
            New schema with the field type changed
            
        Raises:
            SchemaEvolutionError: If field doesn't exist
            IncompatibleSchemaChangeError: If type change is incompatible without force
        """
        # Find the field
        field_to_change = None
        field_index = None
        for i, field in enumerate(schema.fields):
            if field.arrow.name == field_name:
                field_to_change = field
                field_index = i
                break
        
        if field_to_change is None:
            raise SchemaEvolutionError(f"Field '{field_name}' not found in schema")
        
        # Check type compatibility
        if not force and not cls._is_compatible_type_change(field_to_change.arrow.type, new_type):
            raise IncompatibleSchemaChangeError(
                f"Type change from {field_to_change.arrow.type} to {new_type} is incompatible. "
                f"Use force=True to override."
            )
        
        # Create new field with new type
        new_field = Field.of(
            field=pa.field(field_name, new_type, field_to_change.arrow.nullable),
            field_id=field_to_change.id,
        )
        # Preserve metadata
        if hasattr(field_to_change, 'metadata'):
            new_field.metadata = field_to_change.metadata
        
        # Create new field list with changed field
        new_fields = list(schema.fields)
        new_fields[field_index] = new_field
        
        return Schema.of(new_fields)

    @classmethod
    def merge_schemas(
        cls,
        old_schema: Schema,
        new_schema: Schema,
        resolve_conflicts: str = "error"
    ) -> Schema:
        """Merge two schemas for streaming/incremental updates.
        
        Args:
            old_schema: The existing schema
            new_schema: The new schema to merge
            resolve_conflicts: How to handle conflicts: "error", "keep_old", "keep_new"
            
        Returns:
            Merged schema containing fields from both schemas
            
        Raises:
            IncompatibleSchemaChangeError: If schemas have conflicting fields
        """
        # Build field maps
        old_fields_by_name = {f.arrow.name: f for f in old_schema.fields}
        old_fields_by_id = {f.id: f for f in old_schema.fields}
        new_fields_by_name = {f.arrow.name: f for f in new_schema.fields}
        new_fields_by_id = {f.id: f for f in new_schema.fields}
        
        merged_fields = []
        seen_names = set()
        seen_ids = set()
        
        # First, add all fields from old schema
        for field in old_schema.fields:
            merged_fields.append(field)
            seen_names.add(field.arrow.name)
            seen_ids.add(field.id)
        
        # Then, add new fields from new schema
        for field in new_schema.fields:
            if field.arrow.name in seen_names:
                # Check for type conflict
                old_field = old_fields_by_name[field.arrow.name]
                if old_field.arrow.type != field.arrow.type:
                    if resolve_conflicts == "error":
                        raise IncompatibleSchemaChangeError(
                            f"Type conflict for field '{field.arrow.name}': "
                            f"{old_field.arrow.type} vs {field.arrow.type}"
                        )
                    elif resolve_conflicts == "keep_new":
                        # Replace old field with new one
                        for i, f in enumerate(merged_fields):
                            if f.arrow.name == field.arrow.name:
                                merged_fields[i] = field
                                break
                # else: keep old field (already in merged_fields)
            elif field.id in seen_ids:
                # Field ID exists but name is different - this is an error
                old_field = old_fields_by_id[field.id]
                raise IncompatibleSchemaChangeError(
                    f"Field ID {field.id} exists with different name: "
                    f"'{old_field.arrow.name}' vs '{field.arrow.name}'"
                )
            else:
                # New field, add it
                merged_fields.append(field)
                seen_names.add(field.arrow.name)
                seen_ids.add(field.id)
        
        return Schema.of(merged_fields)

    @classmethod
    def apply_operations(
        cls,
        schema: Schema,
        operations: List[SchemaOperation]
    ) -> Schema:
        """Apply a batch of schema operations.
        
        Args:
            schema: The existing schema
            operations: List of operations to apply in order
            
        Returns:
            New schema with all operations applied
        """
        evolved_schema = schema
        
        for op in operations:
            if op.type == SchemaOperationType.ADD_COLUMN:
                evolved_schema = cls.add_column(evolved_schema, op.field)
            elif op.type == SchemaOperationType.DROP_COLUMN:
                evolved_schema = cls.drop_column(
                    evolved_schema, op.field_name, op.force
                )
            elif op.type == SchemaOperationType.RENAME_COLUMN:
                evolved_schema = cls.rename_column(
                    evolved_schema, op.field_name, op.new_name
                )
            elif op.type == SchemaOperationType.CHANGE_TYPE:
                evolved_schema = cls.change_column_type(
                    evolved_schema, op.field_name, op.new_type, op.force
                )
            elif op.type == SchemaOperationType.ADD_NESTED_FIELD:
                evolved_schema = cls.add_nested_field(
                    evolved_schema, op.field_name, op.field.field
                )
            else:
                raise SchemaEvolutionError(f"Unknown operation type: {op.type}")
        
        return evolved_schema

    @classmethod
    def is_backward_compatible(
        cls,
        old_schema: Schema,
        new_schema: Schema,
        check_renames: bool = False
    ) -> bool:
        """Check if new schema is backward compatible with old schema.
        
        Args:
            old_schema: The existing schema
            new_schema: The new schema to check
            check_renames: Whether to consider renames as compatible
            
        Returns:
            True if new schema is backward compatible
        """
        old_fields_by_name = {f.arrow.name: f for f in old_schema.fields}
        new_fields_by_name = {f.arrow.name: f for f in new_schema.fields}
        new_fields_by_id = {f.id: f for f in new_schema.fields}
        
        # Check all required fields from old schema exist in new schema
        for old_field in old_schema.fields:
            if not old_field.arrow.nullable:  # Required field
                if old_field.arrow.name not in new_fields_by_name:
                    if check_renames:
                        # Check if field was renamed (same ID, different name)
                        if old_field.id not in new_fields_by_id:
                            return False
                    else:
                        return False
                else:
                    # Check type compatibility
                    new_field = new_fields_by_name[old_field.arrow.name]
                    if not cls._is_compatible_type_change(old_field.arrow.type, new_field.arrow.type):
                        return False
        
        return True

    @classmethod
    def add_nested_field(
        cls,
        schema: Schema,
        parent_field_name: str,
        nested_field: pa.Field
    ) -> Schema:
        """Add a field to a nested struct type.
        
        Args:
            schema: The existing schema
            parent_field_name: Name of the parent struct field
            nested_field: The field to add to the struct
            
        Returns:
            New schema with the nested field added
            
        Raises:
            SchemaEvolutionError: If parent field doesn't exist or isn't a struct
        """
        # Find parent field
        parent_field = None
        parent_index = None
        for i, field in enumerate(schema.fields):
            if field.arrow.name == parent_field_name:
                parent_field = field
                parent_index = i
                break
        
        if parent_field is None:
            raise SchemaEvolutionError(
                f"Parent field '{parent_field_name}' not found in schema"
            )
        
        # Check parent is a struct
        if not isinstance(parent_field.arrow.type, pa.StructType):
            raise SchemaEvolutionError(
                f"Field '{parent_field_name}' is not a struct type"
            )
        
        # Add field to struct
        struct_fields = list(parent_field.arrow.type)
        struct_fields.append(nested_field)
        new_struct_type = pa.struct(struct_fields)
        
        # Create new parent field with updated struct
        new_parent_field = Field.of(
            field=pa.field(parent_field_name, new_struct_type, parent_field.arrow.nullable),
            field_id=parent_field.id,
        )
        
        # Create new schema with updated parent field
        new_fields = list(schema.fields)
        new_fields[parent_index] = new_parent_field
        
        return Schema.of(new_fields)

    @classmethod
    def get_schema_difference(
        cls,
        old_schema: Schema,
        new_schema: Schema
    ) -> SchemaDifference:
        """Get the difference between two schemas.
        
        Args:
            old_schema: The original schema
            new_schema: The new schema
            
        Returns:
            SchemaDifference object describing the changes
        """
        old_fields_by_name = {f.arrow.name: f for f in old_schema.fields}
        old_fields_by_id = {f.id: f for f in old_schema.fields}
        new_fields_by_name = {f.arrow.name: f for f in new_schema.fields}
        new_fields_by_id = {f.id: f for f in new_schema.fields}
        
        added_fields = []
        dropped_fields = []
        renamed_fields = []
        type_changes = []
        metadata_changes = []
        
        # Find dropped and changed fields
        for old_field in old_schema.fields:
            if old_field.arrow.name not in new_fields_by_name:
                # Check if renamed (same ID, different name)
                if old_field.id in new_fields_by_id:
                    new_field = new_fields_by_id[old_field.id]
                    renamed_fields.append((old_field.arrow.name, new_field.arrow.name))
                else:
                    dropped_fields.append(old_field)
            else:
                new_field = new_fields_by_name[old_field.arrow.name]
                if old_field.arrow.type != new_field.arrow.type:
                    type_changes.append((old_field.arrow.name, old_field.arrow.type, new_field.arrow.type))
                if hasattr(old_field, 'metadata') and hasattr(new_field, 'metadata'):
                    if old_field.metadata != new_field.metadata:
                        metadata_changes.append(
                            (old_field.arrow.name, old_field.metadata, new_field.metadata)
                        )
        
        # Find added fields
        for new_field in new_schema.fields:
            if new_field.arrow.name not in old_fields_by_name:
                # Check it's not a rename
                if new_field.id not in old_fields_by_id:
                    added_fields.append(new_field)
        
        return SchemaDifference(
            added_fields=added_fields,
            dropped_fields=dropped_fields,
            renamed_fields=renamed_fields,
            type_changes=type_changes,
            metadata_changes=metadata_changes,
        )

    @classmethod
    def _is_compatible_type_change(
        cls,
        old_type: pa.DataType,
        new_type: pa.DataType
    ) -> bool:
        """Check if a type change is compatible (safe widening).
        
        Args:
            old_type: The original type
            new_type: The new type
            
        Returns:
            True if the type change is compatible
        """
        # Same type is always compatible
        if old_type == new_type:
            return True
        
        # Check compatibility rules
        for (from_type, to_type), compatible in cls.COMPATIBLE_TYPE_CONVERSIONS.items():
            if old_type == from_type and new_type == to_type:
                return compatible
        
        return False