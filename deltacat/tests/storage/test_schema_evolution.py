"""Tests for DeltaCAT schema evolution functionality."""

import pytest
from typing import List, Optional
import pyarrow as pa

from deltacat.storage import Schema, Field
from deltacat.storage.model.schema_evolution import (
    SchemaEvolution,
    SchemaOperation,
    SchemaOperationType,
    SchemaEvolutionError,
    IncompatibleSchemaChangeError,
)


class TestSchemaEvolution:
    """Test suite for schema evolution operations."""

    @pytest.fixture
    def base_schema(self) -> Schema:
        """Create a base schema for testing."""
        fields = [
            Field.of(
                field=pa.field("id", pa.int64(), nullable=False),
                field_id=1,
            ),
            Field.of(
                field=pa.field("name", pa.string(), nullable=True),
                field_id=2,
            ),
            Field.of(
                field=pa.field("age", pa.int32(), nullable=True),
                field_id=3,
            ),
            Field.of(
                field=pa.field("email", pa.string(), nullable=True),
                field_id=4,
            ),
        ]
        return Schema.of(fields)

    def test_add_column_to_schema(self, base_schema):
        """Test adding a new column to an existing schema."""
        new_field = Field.of(
            field=pa.field("address", pa.string(), nullable=True),
            field_id=5,
        )
        
        evolved_schema = SchemaEvolution.add_column(base_schema, new_field)
        
        # Verify the new field was added
        assert len(evolved_schema.fields) == len(base_schema.fields) + 1
        assert evolved_schema.fields[-1].name == "address"
        assert evolved_schema.fields[-1].field_id == 5
        
        # Verify existing fields are unchanged
        for i in range(len(base_schema.fields)):
            assert evolved_schema.fields[i].name == base_schema.fields[i].name
            assert evolved_schema.fields[i].field_id == base_schema.fields[i].field_id

    def test_add_column_with_duplicate_name(self, base_schema):
        """Test that adding a column with duplicate name raises error."""
        duplicate_field = Field.of(
            field=pa.field("name", pa.string(), nullable=True),
            field_id=5,
        )
        
        with pytest.raises(SchemaEvolutionError, match="already exists"):
            SchemaEvolution.add_column(base_schema, duplicate_field)

    def test_add_column_with_duplicate_id(self, base_schema):
        """Test that adding a column with duplicate field ID raises error."""
        duplicate_id_field = Field.of(
            field=pa.field("new_field", pa.string(), nullable=True),
            field_id=2,  # ID already exists
        )
        
        with pytest.raises(SchemaEvolutionError, match="Field ID.*already exists"):
            SchemaEvolution.add_column(base_schema, duplicate_id_field)

    def test_drop_column_from_schema(self, base_schema):
        """Test dropping a column from schema."""
        evolved_schema = SchemaEvolution.drop_column(base_schema, "age")
        
        # Verify the field was removed
        assert len(evolved_schema.fields) == len(base_schema.fields) - 1
        field_names = [f.name for f in evolved_schema.fields]
        assert "age" not in field_names
        assert "id" in field_names
        assert "name" in field_names
        assert "email" in field_names

    def test_drop_nonexistent_column(self, base_schema):
        """Test dropping a column that doesn't exist raises error."""
        with pytest.raises(SchemaEvolutionError, match="Field.*not found"):
            SchemaEvolution.drop_column(base_schema, "nonexistent")

    def test_drop_required_column(self, base_schema):
        """Test that dropping a non-nullable column requires force flag."""
        # Should fail without force flag
        with pytest.raises(IncompatibleSchemaChangeError, match="non-nullable"):
            SchemaEvolution.drop_column(base_schema, "id")
        
        # Should succeed with force flag
        evolved_schema = SchemaEvolution.drop_column(base_schema, "id", force=True)
        field_names = [f.name for f in evolved_schema.fields]
        assert "id" not in field_names

    def test_rename_column(self, base_schema):
        """Test renaming a column in schema."""
        evolved_schema = SchemaEvolution.rename_column(base_schema, "age", "years_old")
        
        # Verify the field was renamed
        field_names = [f.name for f in evolved_schema.fields]
        assert "age" not in field_names
        assert "years_old" in field_names
        
        # Verify field ID is preserved
        renamed_field = next(f for f in evolved_schema.fields if f.name == "years_old")
        original_field = next(f for f in base_schema.fields if f.name == "age")
        assert renamed_field.field_id == original_field.field_id
        assert renamed_field.type == original_field.type

    def test_rename_nonexistent_column(self, base_schema):
        """Test renaming a column that doesn't exist raises error."""
        with pytest.raises(SchemaEvolutionError, match="Field.*not found"):
            SchemaEvolution.rename_column(base_schema, "nonexistent", "new_name")

    def test_rename_to_existing_column(self, base_schema):
        """Test renaming to an existing column name raises error."""
        with pytest.raises(SchemaEvolutionError, match="already exists"):
            SchemaEvolution.rename_column(base_schema, "age", "name")

    def test_change_column_type_compatible(self, base_schema):
        """Test changing column type with compatible conversion."""
        # int32 -> int64 is a safe widening conversion
        evolved_schema = SchemaEvolution.change_column_type(
            base_schema, "age", pa.int64()
        )
        
        age_field = next(f for f in evolved_schema.fields if f.name == "age")
        assert age_field.type == pa.int64()
        
        # Field ID should be preserved
        original_field = next(f for f in base_schema.fields if f.name == "age")
        assert age_field.field_id == original_field.field_id

    def test_change_column_type_incompatible(self, base_schema):
        """Test changing column type with incompatible conversion raises error."""
        # string -> int is not a safe conversion without force
        with pytest.raises(IncompatibleSchemaChangeError, match="incompatible"):
            SchemaEvolution.change_column_type(base_schema, "name", pa.int32())
        
        # Should succeed with force flag
        evolved_schema = SchemaEvolution.change_column_type(
            base_schema, "name", pa.int32(), force=True
        )
        name_field = next(f for f in evolved_schema.fields if f.name == "name")
        assert name_field.type == pa.int32()

    def test_change_nonexistent_column_type(self, base_schema):
        """Test changing type of nonexistent column raises error."""
        with pytest.raises(SchemaEvolutionError, match="Field.*not found"):
            SchemaEvolution.change_column_type(base_schema, "nonexistent", pa.string())

    def test_merge_schemas_no_conflicts(self, base_schema):
        """Test merging schemas with no conflicts."""
        # Create a schema with additional fields
        new_fields = [
            Field.of(
                field=pa.field("id", pa.int64(), nullable=False),
                field_id=1,
            ),
            Field.of(
                field=pa.field("name", pa.string(), nullable=True),
                field_id=2,
            ),
            Field.of(
                field=pa.field("address", pa.string(), nullable=True),
                field_id=5,
            ),
            Field.of(
                field=pa.field("phone", pa.string(), nullable=True),
                field_id=6,
            ),
        ]
        new_schema = Schema.of(new_fields)
        
        merged_schema = SchemaEvolution.merge_schemas(base_schema, new_schema)
        
        # Should have all unique fields
        field_names = [f.name for f in merged_schema.fields]
        assert "id" in field_names
        assert "name" in field_names
        assert "age" in field_names  # From base schema
        assert "email" in field_names  # From base schema
        assert "address" in field_names  # From new schema
        assert "phone" in field_names  # From new schema
        assert len(merged_schema.fields) == 6

    def test_merge_schemas_with_type_conflicts(self, base_schema):
        """Test merging schemas with type conflicts."""
        # Create a schema with conflicting type for 'age'
        conflicting_fields = [
            Field.of(
                field=pa.field("age", pa.string(), nullable=True),  # Different type
                field_id=3,
            ),
        ]
        conflicting_schema = Schema.of(conflicting_fields)
        
        with pytest.raises(IncompatibleSchemaChangeError, match="Type conflict"):
            SchemaEvolution.merge_schemas(base_schema, conflicting_schema)

    def test_apply_operations_batch(self, base_schema):
        """Test applying multiple schema operations in batch."""
        operations = [
            SchemaOperation(
                type=SchemaOperationType.ADD_COLUMN,
                field=Field.of(
                    field=pa.field("address", pa.string(), nullable=True),
                    field_id=5,
                ),
            ),
            SchemaOperation(
                type=SchemaOperationType.DROP_COLUMN,
                field_name="age",
            ),
            SchemaOperation(
                type=SchemaOperationType.RENAME_COLUMN,
                field_name="email",
                new_name="email_address",
            ),
            SchemaOperation(
                type=SchemaOperationType.CHANGE_TYPE,
                field_name="id",
                new_type=pa.int32(),
                force=True,  # Narrowing conversion requires force
            ),
        ]
        
        evolved_schema = SchemaEvolution.apply_operations(base_schema, operations)
        
        # Verify all operations were applied
        field_names = [f.name for f in evolved_schema.fields]
        assert "address" in field_names
        assert "age" not in field_names
        assert "email" not in field_names
        assert "email_address" in field_names
        
        id_field = next(f for f in evolved_schema.fields if f.name == "id")
        assert id_field.type == pa.int32()

    def test_validate_backward_compatibility(self, base_schema):
        """Test backward compatibility validation."""
        # Adding optional field is backward compatible
        new_field = Field.of(
            field=pa.field("address", pa.string(), nullable=True),
            field_id=5,
        )
        evolved_schema = SchemaEvolution.add_column(base_schema, new_field)
        assert SchemaEvolution.is_backward_compatible(base_schema, evolved_schema)
        
        # Dropping required field is not backward compatible
        evolved_schema = SchemaEvolution.drop_column(base_schema, "id", force=True)
        assert not SchemaEvolution.is_backward_compatible(base_schema, evolved_schema)
        
        # Renaming is considered backward compatible if tracked properly
        evolved_schema = SchemaEvolution.rename_column(base_schema, "age", "years_old")
        assert SchemaEvolution.is_backward_compatible(
            base_schema, evolved_schema, check_renames=True
        )

    def test_schema_evolution_with_nested_types(self):
        """Test schema evolution with nested struct types."""
        # Create schema with nested struct
        fields = [
            Field.of(
                field=pa.field("id", pa.int64(), nullable=False),
                field_id=1,
            ),
            Field.of(
                field=pa.field(
                    "person",
                    pa.struct([
                        pa.field("first_name", pa.string()),
                        pa.field("last_name", pa.string()),
                        pa.field("age", pa.int32()),
                    ]),
                    nullable=True,
                ),
                field_id=2,
            ),
        ]
        schema = Schema.of(fields)
        
        # Add field to nested struct
        evolved_schema = SchemaEvolution.add_nested_field(
            schema,
            "person",
            pa.field("middle_name", pa.string(), nullable=True),
        )
        
        person_field = next(f for f in evolved_schema.fields if f.name == "person")
        struct_type = person_field.type
        field_names = [f.name for f in struct_type]
        assert "middle_name" in field_names

    def test_get_schema_difference(self, base_schema):
        """Test getting the difference between two schemas."""
        # Create evolved schema
        evolved_schema = SchemaEvolution.add_column(
            base_schema,
            Field.of(
                field=pa.field("address", pa.string(), nullable=True),
                field_id=5,
            ),
        )
        evolved_schema = SchemaEvolution.drop_column(evolved_schema, "age")
        evolved_schema = SchemaEvolution.rename_column(evolved_schema, "email", "email_address")
        
        diff = SchemaEvolution.get_schema_difference(base_schema, evolved_schema)
        
        assert len(diff.added_fields) == 1
        assert diff.added_fields[0].name == "address"
        
        assert len(diff.dropped_fields) == 1
        assert diff.dropped_fields[0].name == "age"
        
        assert len(diff.renamed_fields) == 1
        assert diff.renamed_fields[0] == ("email", "email_address")

    def test_schema_evolution_preserves_metadata(self, base_schema):
        """Test that schema evolution preserves field metadata."""
        # Add metadata to a field
        base_schema.fields[0].metadata = {"description": "Unique identifier"}
        
        # Perform evolution
        evolved_schema = SchemaEvolution.add_column(
            base_schema,
            Field.of(
                field=pa.field("new_field", pa.string()),
                field_id=5,
            ),
        )
        
        # Verify metadata is preserved
        id_field = next(f for f in evolved_schema.fields if f.name == "id")
        assert id_field.metadata == {"description": "Unique identifier"}