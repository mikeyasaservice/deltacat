"""Tests for catalog-level schema operations."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pyarrow as pa

from deltacat.catalog.schema_operations import (
    evolve_table_schema,
    add_table_column,
    drop_table_column,
    rename_table_column,
    change_table_column_type,
    get_schema_evolution_plan,
)
from deltacat.storage import Schema, Field
from deltacat.storage.model.schema_evolution import (
    SchemaOperation,
    SchemaOperationType,
    SchemaEvolutionError,
)
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.table_version import TableVersion


class TestCatalogSchemaOperations:
    """Test suite for catalog-level schema operations."""

    @pytest.fixture
    def mock_table_def(self):
        """Create a mock table definition with schema."""
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
        ]
        schema = Schema.of(fields)
        
        table_version = Mock(spec=TableVersion)
        table_version.schema = schema
        
        table_def = Mock(spec=TableDefinition)
        table_def.table_version = table_version
        
        return table_def

    @patch("deltacat.catalog.schema_operations.alter_table")
    @patch("deltacat.catalog.schema_operations.get_table")
    def test_evolve_table_schema(self, mock_get_table, mock_alter_table, mock_table_def):
        """Test evolving a table schema with multiple operations."""
        mock_get_table.return_value = mock_table_def
        
        operations = [
            SchemaOperation(
                type=SchemaOperationType.ADD_COLUMN,
                field=Field.of(
                    field=pa.field("email", pa.string(), nullable=True),
                    field_id=4,
                ),
            ),
            SchemaOperation(
                type=SchemaOperationType.DROP_COLUMN,
                field_name="age",
            ),
        ]
        
        evolve_table_schema(
            table="test_table",
            namespace="test_namespace",
            operations=operations,
            catalog="test_catalog",
        )
        
        # Verify get_table was called
        mock_get_table.assert_called_once_with(
            name="test_table",
            namespace="test_namespace",
            catalog="test_catalog",
        )
        
        # Verify alter_table was called with schema updates
        mock_alter_table.assert_called_once()
        call_args = mock_alter_table.call_args
        assert call_args[1]["table"] == "test_table"
        assert call_args[1]["namespace"] == "test_namespace"
        assert call_args[1]["catalog"] == "test_catalog"
        assert "schema_updates" in call_args[1]
        
        # Verify the evolved schema has correct fields
        schema_updates = call_args[1]["schema_updates"]
        evolved_schema = schema_updates["schema"]
        field_names = [f.arrow.name for f in evolved_schema.fields]
        assert "email" in field_names
        assert "age" not in field_names
        assert "id" in field_names
        assert "name" in field_names

    @patch("deltacat.catalog.schema_operations.alter_table")
    @patch("deltacat.catalog.schema_operations.get_table")
    def test_evolve_table_schema_nonexistent_table(self, mock_get_table, mock_alter_table):
        """Test that evolving schema for nonexistent table raises error."""
        mock_get_table.return_value = None
        
        operations = [
            SchemaOperation(
                type=SchemaOperationType.ADD_COLUMN,
                field=Field.of(
                    field=pa.field("new_col", pa.string()),
                    field_id=5,
                ),
            ),
        ]
        
        with pytest.raises(ValueError, match="does not exist"):
            evolve_table_schema(
                table="nonexistent",
                namespace="test_namespace",
                operations=operations,
            )
        
        mock_alter_table.assert_not_called()

    @patch("deltacat.catalog.schema_operations.evolve_table_schema")
    def test_add_table_column(self, mock_evolve):
        """Test adding a column to a table."""
        field = Field.of(
            field=pa.field("new_column", pa.string(), nullable=True),
            field_id=10,
        )
        
        add_table_column(
            table="test_table",
            namespace="test_namespace",
            field=field,
            catalog="test_catalog",
        )
        
        mock_evolve.assert_called_once()
        call_args = mock_evolve.call_args
        assert call_args[1]["table"] == "test_table"
        assert call_args[1]["namespace"] == "test_namespace"
        assert call_args[1]["catalog"] == "test_catalog"
        
        operations = call_args[1]["operations"]
        assert len(operations) == 1
        assert operations[0].type == SchemaOperationType.ADD_COLUMN
        assert operations[0].field == field

    @patch("deltacat.catalog.schema_operations.evolve_table_schema")
    def test_drop_table_column(self, mock_evolve):
        """Test dropping a column from a table."""
        drop_table_column(
            table="test_table",
            namespace="test_namespace",
            column_name="old_column",
            force=True,
            catalog="test_catalog",
        )
        
        mock_evolve.assert_called_once()
        call_args = mock_evolve.call_args
        assert call_args[1]["table"] == "test_table"
        assert call_args[1]["namespace"] == "test_namespace"
        assert call_args[1]["catalog"] == "test_catalog"
        
        operations = call_args[1]["operations"]
        assert len(operations) == 1
        assert operations[0].type == SchemaOperationType.DROP_COLUMN
        assert operations[0].field_name == "old_column"
        assert operations[0].force is True

    @patch("deltacat.catalog.schema_operations.evolve_table_schema")
    def test_rename_table_column(self, mock_evolve):
        """Test renaming a column in a table."""
        rename_table_column(
            table="test_table",
            namespace="test_namespace",
            old_name="old_name",
            new_name="new_name",
            catalog="test_catalog",
        )
        
        mock_evolve.assert_called_once()
        call_args = mock_evolve.call_args
        assert call_args[1]["table"] == "test_table"
        assert call_args[1]["namespace"] == "test_namespace"
        assert call_args[1]["catalog"] == "test_catalog"
        
        operations = call_args[1]["operations"]
        assert len(operations) == 1
        assert operations[0].type == SchemaOperationType.RENAME_COLUMN
        assert operations[0].field_name == "old_name"
        assert operations[0].new_name == "new_name"

    @patch("deltacat.catalog.schema_operations.evolve_table_schema")
    def test_change_table_column_type(self, mock_evolve):
        """Test changing column type in a table."""
        change_table_column_type(
            table="test_table",
            namespace="test_namespace",
            column_name="age",
            new_type=pa.int64(),
            force=False,
            catalog="test_catalog",
        )
        
        mock_evolve.assert_called_once()
        call_args = mock_evolve.call_args
        assert call_args[1]["table"] == "test_table"
        assert call_args[1]["namespace"] == "test_namespace"
        assert call_args[1]["catalog"] == "test_catalog"
        
        operations = call_args[1]["operations"]
        assert len(operations) == 1
        assert operations[0].type == SchemaOperationType.CHANGE_TYPE
        assert operations[0].field_name == "age"
        assert operations[0].new_type == pa.int64()
        assert operations[0].force is False

    @patch("deltacat.catalog.schema_operations.get_table")
    def test_get_schema_evolution_plan(self, mock_get_table, mock_table_def):
        """Test generating schema evolution plan."""
        mock_get_table.return_value = mock_table_def
        
        # Create target schema with changes
        target_fields = [
            Field.of(
                field=pa.field("id", pa.int64(), nullable=False),
                field_id=1,
            ),
            Field.of(
                field=pa.field("full_name", pa.string(), nullable=True),  # Renamed from 'name'
                field_id=2,
            ),
            Field.of(
                field=pa.field("email", pa.string(), nullable=True),  # Added
                field_id=4,
            ),
            # 'age' field dropped
        ]
        target_schema = Schema.of(target_fields)
        
        operations = get_schema_evolution_plan(
            table="test_table",
            namespace="test_namespace",
            target_schema=target_schema,
            catalog="test_catalog",
        )
        
        # Verify operations were generated
        operation_types = [op.type for op in operations]
        
        # Should have operations for: drop age, rename name->full_name, add email
        assert SchemaOperationType.DROP_COLUMN in operation_types
        assert SchemaOperationType.RENAME_COLUMN in operation_types
        assert SchemaOperationType.ADD_COLUMN in operation_types
        
        # Find specific operations
        drop_op = next(op for op in operations if op.type == SchemaOperationType.DROP_COLUMN)
        assert drop_op.field_name == "age"
        
        rename_op = next(op for op in operations if op.type == SchemaOperationType.RENAME_COLUMN)
        assert rename_op.field_name == "name"
        assert rename_op.new_name == "full_name"
        
        add_op = next(op for op in operations if op.type == SchemaOperationType.ADD_COLUMN)
        assert add_op.field.arrow.name == "email"

    @patch("deltacat.catalog.schema_operations.get_table")
    def test_get_schema_evolution_plan_nonexistent_table(self, mock_get_table):
        """Test that evolution plan for nonexistent table raises error."""
        mock_get_table.return_value = None
        
        # Create a simple target schema (can't be empty in DeltaCAT)
        target_schema = Schema.of([
            Field.of(
                field=pa.field("id", pa.int64()),
                field_id=1,
            )
        ])
        
        with pytest.raises(ValueError, match="does not exist"):
            get_schema_evolution_plan(
                table="nonexistent",
                namespace="test_namespace",
                target_schema=target_schema,
            )

    @patch("deltacat.catalog.schema_operations.alter_table")
    @patch("deltacat.catalog.schema_operations.get_table")
    def test_backward_compatibility_warning(self, mock_get_table, mock_alter_table, mock_table_def):
        """Test that backward incompatible changes produce warning."""
        mock_get_table.return_value = mock_table_def
        
        # Drop a required field - not backward compatible
        operations = [
            SchemaOperation(
                type=SchemaOperationType.DROP_COLUMN,
                field_name="id",
                force=True,
            ),
        ]
        
        with patch("deltacat.catalog.schema_operations.logger") as mock_logger:
            evolve_table_schema(
                table="test_table",
                namespace="test_namespace",
                operations=operations,
                validate_compatibility=True,
            )
            
            # Should log a warning about backward compatibility
            mock_logger.warning.assert_called()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "backward compatibility" in warning_msg