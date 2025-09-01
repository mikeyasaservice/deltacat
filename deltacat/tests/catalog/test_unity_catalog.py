"""Tests for Unity Catalog implementation in DeltaCAT.

This test suite follows TDD principles - tests are written first to define
the expected behavior of the Unity Catalog integration.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock, call
from typing import Dict, Any, List
import pyarrow as pa
import pandas as pd

from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.namespace import Namespace
from deltacat.storage.model.table import Table
from deltacat.storage.model.schema import Schema, Field
from deltacat.storage.model.list_result import ListResult
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode


class TestUnityCatalogConfig(unittest.TestCase):
    """Test Unity Catalog configuration."""
    
    def test_config_initialization(self):
        """Test that UnityCatalogConfig can be initialized with required fields."""
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main",
            warehouse_id="warehouse123"
        )
        
        self.assertEqual(config.workspace_url, "https://workspace.databricks.com")
        self.assertEqual(config.token, "dapi123456789")
        self.assertEqual(config.catalog_name, "main")
        self.assertEqual(config.warehouse_id, "warehouse123")
    
    def test_config_with_optional_fields(self):
        """Test UnityCatalogConfig with optional fields."""
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        
        self.assertIsNone(config.warehouse_id)
        self.assertIsNone(config.cluster_id)
        self.assertEqual(config.additional_properties, {})


class TestUnityCatalogClient(unittest.TestCase):
    """Test Unity Catalog client wrapper."""
    
    @patch('deltacat.catalog.unity.client.WorkspaceClient')
    def test_client_initialization(self, mock_workspace_client):
        """Test Unity client initialization with workspace URL and token."""
        from deltacat.catalog.unity.client import UnityClient
        
        client = UnityClient(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789"
        )
        
        mock_workspace_client.assert_called_once_with(
            host="https://workspace.databricks.com",
            token="dapi123456789"
        )
        self.assertIsNotNone(client.workspace)
    
    @patch('deltacat.catalog.unity.client.WorkspaceClient')
    def test_list_catalogs(self, mock_workspace_client):
        """Test listing Unity catalogs."""
        from deltacat.catalog.unity.client import UnityClient
        
        # Mock catalog list response
        mock_catalog1 = Mock()
        mock_catalog1.name = "main"
        mock_catalog2 = Mock()
        mock_catalog2.name = "bronze"
        
        mock_workspace = Mock()
        mock_workspace.catalogs.list.return_value = [mock_catalog1, mock_catalog2]
        mock_workspace_client.return_value = mock_workspace
        
        client = UnityClient(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789"
        )
        
        catalogs = client.list_catalogs()
        self.assertEqual(len(catalogs), 2)
        self.assertEqual(catalogs[0].name, "main")
        self.assertEqual(catalogs[1].name, "bronze")


class TestUnityCatalogImpl(unittest.TestCase):
    """Test Unity Catalog interface implementation."""
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_initialize(self, mock_client_class):
        """Test Unity Catalog initialization."""
        from deltacat.catalog.unity.impl import initialize
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        
        result = initialize(config=config)
        
        mock_client_class.assert_called_once_with(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789"
        )
        self.assertIsNotNone(result)
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_create_namespace(self, mock_client_class):
        """Test creating a namespace (schema) in Unity Catalog."""
        from deltacat.catalog.unity.impl import create_namespace
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock successful schema creation
        mock_schema = Mock()
        mock_schema.name = "test_schema"
        mock_schema.catalog_name = "main"
        mock_client.workspace.schemas.create.return_value = mock_schema
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        namespace = create_namespace(
            namespace="test_schema",
            inner=inner,
            config=config
        )
        
        self.assertIsNotNone(namespace)
        mock_client.workspace.schemas.create.assert_called_once()
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_list_namespaces(self, mock_client_class):
        """Test listing namespaces (schemas) in Unity Catalog."""
        from deltacat.catalog.unity.impl import list_namespaces
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock schema list response
        mock_schema1 = Mock()
        mock_schema1.name = "bronze"
        mock_schema1.catalog_name = "main"
        mock_schema2 = Mock()
        mock_schema2.name = "silver"
        mock_schema2.catalog_name = "main"
        
        mock_client.workspace.schemas.list.return_value = [mock_schema1, mock_schema2]
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        result = list_namespaces(inner=inner, config=config)
        
        self.assertIsInstance(result, ListResult)
        namespaces = list(result.all_items())
        self.assertEqual(len(namespaces), 2)
        self.assertEqual(namespaces[0].namespace, "bronze")
        self.assertEqual(namespaces[1].namespace, "silver")
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_create_delta_table(self, mock_client_class):
        """Test creating a Delta table in Unity Catalog."""
        from deltacat.catalog.unity.impl import create_table
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock table creation
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "test_schema"
        mock_table.table_type = "MANAGED"
        mock_table.data_source_format = "DELTA"
        mock_client.workspace.tables.create.return_value = mock_table
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        # Create schema for table
        schema = Schema.of([
            Field.of(field=pa.field("id", pa.int64()), field_id=1),
            Field.of(field=pa.field("name", pa.string()), field_id=2),
        ])
        
        table_def = create_table(
            table="test_table",
            namespace="test_schema",
            schema=schema,
            format="DELTA",
            inner=inner,
            config=config
        )
        
        self.assertIsNotNone(table_def)
        mock_client.workspace.tables.create.assert_called_once()
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_create_iceberg_table_with_uniform(self, mock_client_class):
        """Test creating an Iceberg-compatible table using Delta UniForm."""
        from deltacat.catalog.unity.impl import create_table
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock table creation with UniForm
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "test_schema"
        mock_table.table_type = "MANAGED"
        mock_table.data_source_format = "DELTA"
        mock_table.properties = {
            "delta.universalFormat.enabledFormats": "iceberg"
        }
        mock_client.workspace.tables.create.return_value = mock_table
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        # Create schema for table
        schema = Schema.of([
            Field.of(field=pa.field("id", pa.int64()), field_id=1),
            Field.of(field=pa.field("name", pa.string()), field_id=2),
        ])
        
        table_def = create_table(
            table="test_table",
            namespace="test_schema",
            schema=schema,
            format="ICEBERG",  # Request Iceberg format
            inner=inner,
            config=config
        )
        
        self.assertIsNotNone(table_def)
        # Verify UniForm was enabled
        call_args = mock_client.workspace.tables.create.call_args
        self.assertIn("properties", call_args[1])
        self.assertEqual(
            call_args[1]["properties"]["delta.universalFormat.enabledFormats"],
            "iceberg"
        )
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_read_delta_table(self, mock_client_class):
        """Test reading a Delta table from Unity Catalog."""
        from deltacat.catalog.unity.impl import read_table
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock table metadata
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "test_schema"
        mock_table.data_source_format = "DELTA"
        mock_table.storage_location = "s3://bucket/path/to/table"
        mock_client.workspace.tables.get.return_value = mock_table
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        # Mock Delta table read
        with patch('deltacat.catalog.unity.impl.DeltaTable') as mock_delta_table:
            mock_dt = Mock()
            mock_arrow_table = pa.table({
                'id': [1, 2, 3],
                'name': ['Alice', 'Bob', 'Charlie']
            })
            mock_dt.to_pyarrow_table.return_value = mock_arrow_table
            mock_delta_table.return_value = mock_dt
            
            dataset = read_table(
                table="test_table",
                namespace="test_schema",
                inner=inner,
                config=config
            )
            
            self.assertIsNotNone(dataset)
            mock_client.workspace.tables.get.assert_called_once_with(
                full_name="main.test_schema.test_table"
            )
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_read_iceberg_table(self, mock_client_class):
        """Test reading an Iceberg table from Unity Catalog."""
        from deltacat.catalog.unity.impl import read_table
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock Iceberg table metadata
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "test_schema"
        mock_table.data_source_format = "ICEBERG"
        mock_table.storage_location = "s3://bucket/path/to/iceberg/table"
        mock_client.workspace.tables.get.return_value = mock_table
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        # Mock Iceberg table read
        with patch('deltacat.catalog.unity.impl.load_table') as mock_load_table:
            mock_iceberg_table = Mock()
            mock_arrow_table = pa.table({
                'id': [1, 2, 3],
                'name': ['Alice', 'Bob', 'Charlie']
            })
            mock_iceberg_table.scan().to_arrow.return_value = mock_arrow_table
            mock_load_table.return_value = mock_iceberg_table
            
            dataset = read_table(
                table="test_table",
                namespace="test_schema",
                inner=inner,
                config=config
            )
            
            self.assertIsNotNone(dataset)
            mock_client.workspace.tables.get.assert_called_once()
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_write_to_delta_table(self, mock_client_class):
        """Test writing data to a Delta table in Unity Catalog."""
        from deltacat.catalog.unity.impl import write_to_table
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock table metadata
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "test_schema"
        mock_table.data_source_format = "DELTA"
        mock_table.storage_location = "s3://bucket/path/to/table"
        mock_client.workspace.tables.get.return_value = mock_table
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        # Create test data
        test_data = pa.table({
            'id': [4, 5, 6],
            'name': ['David', 'Eve', 'Frank']
        })
        
        # Mock Delta table write
        with patch('deltalake.write_deltalake') as mock_write:
            write_to_table(
                data=test_data,
                table="test_table",
                namespace="test_schema",
                mode=TableWriteMode.APPEND,
                inner=inner,
                config=config
            )
            
            mock_write.assert_called_once()
            call_args = mock_write.call_args
            self.assertEqual(call_args[0][0], "s3://bucket/path/to/table")
            self.assertEqual(call_args[1]["mode"], "append")
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_list_tables(self, mock_client_class):
        """Test listing tables in a Unity Catalog namespace."""
        from deltacat.catalog.unity.impl import list_tables
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock table list
        mock_table1 = Mock()
        mock_table1.name = "customers"
        mock_table1.catalog_name = "main"
        mock_table1.schema_name = "sales"
        mock_table1.data_source_format = "DELTA"
        
        mock_table2 = Mock()
        mock_table2.name = "orders"
        mock_table2.catalog_name = "main"
        mock_table2.schema_name = "sales"
        mock_table2.data_source_format = "ICEBERG"
        
        mock_client.workspace.tables.list.return_value = [mock_table1, mock_table2]
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        result = list_tables(
            namespace="sales",
            inner=inner,
            config=config
        )
        
        self.assertIsInstance(result, ListResult)
        tables = list(result.all_items())
        self.assertEqual(len(tables), 2)
        self.assertEqual(tables[0].table.table_name, "customers")
        self.assertEqual(tables[1].table.table_name, "orders")
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_alter_table_schema(self, mock_client_class):
        """Test altering table schema in Unity Catalog."""
        from deltacat.catalog.unity.impl import alter_table
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock table metadata
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "test_schema"
        mock_client.workspace.tables.get.return_value = mock_table
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        # Define schema updates
        schema_updates = {
            "schema": Schema.of([
                Field.of(field=pa.field("id", pa.int64()), field_id=1),
                Field.of(field=pa.field("name", pa.string()), field_id=2),
                Field.of(field=pa.field("email", pa.string()), field_id=3),  # New column
            ])
        }
        
        alter_table(
            table="test_table",
            namespace="test_schema",
            schema_updates=schema_updates,
            inner=inner,
            config=config
        )
        
        mock_client.workspace.tables.update.assert_called_once()
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_rename_table(self, mock_client_class):
        """Test renaming a table in Unity Catalog."""
        from deltacat.catalog.unity.impl import rename_table
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        # Mock table metadata retrieval
        mock_table = Mock()
        mock_table.name = "old_table"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "test_schema"
        mock_table.full_name = "main.test_schema.old_table"
        mock_client.workspace.tables.get.return_value = mock_table
        
        # Mock successful rename operation
        mock_client.workspace.tables.update.return_value = Mock()
        
        # Test renaming within same namespace
        rename_table(
            table="old_table",
            new_table="new_table",
            namespace="test_schema",
            inner=inner,
            config=config
        )
        
        # Verify the update was called with correct parameters
        mock_client.workspace.tables.update.assert_called_once_with(
            full_name="main.test_schema.old_table",
            new_name="new_table"
        )
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_rename_table_across_namespaces(self, mock_client_class):
        """Test renaming a table across different namespaces."""
        from deltacat.catalog.unity.impl import rename_table
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        # Mock table metadata
        mock_table = Mock()
        mock_table.name = "old_table"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "old_schema"
        mock_table.full_name = "main.old_schema.old_table"
        mock_client.workspace.tables.get.return_value = mock_table
        
        # Test renaming across namespaces
        rename_table(
            table="old_table",
            new_table="new_table",
            namespace="old_schema",
            new_namespace="new_schema",
            inner=inner,
            config=config
        )
        
        # Verify update with new catalog.schema.table path
        mock_client.workspace.tables.update.assert_called_once_with(
            full_name="main.old_schema.old_table",
            new_catalog_name="main",
            new_schema_name="new_schema",
            new_name="new_table"
        )
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_truncate_table(self, mock_client_class):
        """Test truncating a table in Unity Catalog."""
        from deltacat.catalog.unity.impl import truncate_table
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        # Mock table metadata
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "test_schema"
        mock_table.full_name = "main.test_schema.test_table"
        mock_table.data_source_format = "DELTA"
        mock_table.storage_location = "s3://bucket/path/to/table"
        mock_client.workspace.tables.get.return_value = mock_table
        
        # Mock SQL execution for TRUNCATE
        mock_client.workspace.statement_execution.execute_statement.return_value = Mock(
            status="SUCCEEDED"
        )
        
        # Test truncating Delta table
        truncate_table(
            table="test_table",
            namespace="test_schema",
            inner=inner,
            config=config
        )
        
        # Verify TRUNCATE TABLE was executed
        mock_client.workspace.statement_execution.execute_statement.assert_called_once()
        call_args = mock_client.workspace.statement_execution.execute_statement.call_args
        self.assertIn("TRUNCATE TABLE", call_args[1]["statement"])
        self.assertIn("main.test_schema.test_table", call_args[1]["statement"])
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_truncate_table_with_partitions(self, mock_client_class):
        """Test truncating a partitioned table preserves partition structure."""
        from deltacat.catalog.unity.impl import truncate_table
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        # Mock partitioned table metadata
        mock_table = Mock()
        mock_table.name = "partitioned_table"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "test_schema"
        mock_table.full_name = "main.test_schema.partitioned_table"
        mock_table.data_source_format = "DELTA"
        mock_table.storage_location = "s3://bucket/path/to/partitioned_table"
        mock_table.partition_columns = ["year", "month"]
        mock_client.workspace.tables.get.return_value = mock_table
        
        # Mock SQL execution
        mock_client.workspace.statement_execution.execute_statement.return_value = Mock(
            status="SUCCEEDED"
        )
        
        # Test truncating partitioned table
        truncate_table(
            table="partitioned_table",
            namespace="test_schema",
            inner=inner,
            config=config
        )
        
        # Verify TRUNCATE was executed (partitions preserved automatically)
        mock_client.workspace.statement_execution.execute_statement.assert_called_once()
        call_args = mock_client.workspace.statement_execution.execute_statement.call_args
        self.assertIn("TRUNCATE TABLE", call_args[1]["statement"])
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_truncate_iceberg_table(self, mock_client_class):
        """Test truncating an Iceberg table via UniForm."""
        from deltacat.catalog.unity.impl import truncate_table
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        inner = mock_client
        
        # Mock Iceberg table metadata (with UniForm)
        mock_table = Mock()
        mock_table.name = "iceberg_table"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "test_schema"
        mock_table.full_name = "main.test_schema.iceberg_table"
        mock_table.data_source_format = "DELTA"  # Actually Delta with UniForm
        mock_table.storage_location = "s3://bucket/path/to/iceberg_table"
        mock_table.properties = {
            "delta.universalFormat.enabledFormats": "iceberg"
        }
        mock_client.workspace.tables.get.return_value = mock_table
        
        # Mock SQL execution
        mock_client.workspace.statement_execution.execute_statement.return_value = Mock(
            status="SUCCEEDED"
        )
        
        # Test truncating Iceberg-compatible table
        truncate_table(
            table="iceberg_table",
            namespace="test_schema",
            inner=inner,
            config=config
        )
        
        # Verify TRUNCATE was executed
        mock_client.workspace.statement_execution.execute_statement.assert_called_once()


class TestFormatHandler(unittest.TestCase):
    """Test format handling for Delta, Iceberg, and Hudi."""
    
    def test_detect_format_from_unity_table(self):
        """Test detecting table format from Unity metadata."""
        from deltacat.catalog.unity.format_handler import detect_format
        
        # Test Delta detection
        mock_table = Mock()
        mock_table.data_source_format = "DELTA"
        self.assertEqual(detect_format(mock_table), "DELTA")
        
        # Test Iceberg detection
        mock_table.data_source_format = "ICEBERG"
        self.assertEqual(detect_format(mock_table), "ICEBERG")
        
        # Test Hudi detection
        mock_table.data_source_format = "HUDI"
        self.assertEqual(detect_format(mock_table), "HUDI")
    
    def test_uniform_properties_for_iceberg(self):
        """Test generating UniForm properties for Iceberg compatibility."""
        from deltacat.catalog.unity.format_handler import get_uniform_properties
        
        props = get_uniform_properties("ICEBERG")
        self.assertEqual(props["delta.universalFormat.enabledFormats"], "iceberg")
    
    def test_uniform_properties_for_hudi(self):
        """Test generating UniForm properties for Hudi compatibility."""
        from deltacat.catalog.unity.format_handler import get_uniform_properties
        
        props = get_uniform_properties("HUDI")
        self.assertEqual(props["delta.universalFormat.enabledFormats"], "hudi")
    
    def test_uniform_properties_for_both(self):
        """Test generating UniForm properties for both Iceberg and Hudi."""
        from deltacat.catalog.unity.format_handler import get_uniform_properties
        
        props = get_uniform_properties(["ICEBERG", "HUDI"])
        self.assertEqual(props["delta.universalFormat.enabledFormats"], "iceberg,hudi")


class TestUnityCatalogIntegration(unittest.TestCase):
    """Integration tests for Unity Catalog with DeltaCAT."""
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_end_to_end_table_lifecycle(self, mock_client_class):
        """Test complete table lifecycle: create, write, read, alter, drop."""
        from deltacat.catalog.unity import impl as unity_impl
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        
        # Initialize catalog
        inner = unity_impl.initialize(config=config)
        
        # 1. Create namespace
        mock_schema = Mock()
        mock_schema.name = "test_ns"
        mock_client.workspace.schemas.create.return_value = mock_schema
        
        namespace = unity_impl.create_namespace(
            namespace="test_ns",
            inner=inner,
            config=config
        )
        self.assertIsNotNone(namespace)
        
        # 2. Create table
        schema = Schema.of([
            Field.of(field=pa.field("id", pa.int64()), field_id=1),
            Field.of(field=pa.field("value", pa.float64()), field_id=2),
        ])
        
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.catalog_name = "main"
        mock_table.schema_name = "test_ns"
        mock_table.data_source_format = "DELTA"
        mock_table.storage_location = "s3://bucket/table"
        mock_client.workspace.tables.create.return_value = mock_table
        mock_client.workspace.tables.get.return_value = mock_table
        
        table_def = unity_impl.create_table(
            table="test_table",
            namespace="test_ns",
            schema=schema,
            inner=inner,
            config=config
        )
        self.assertIsNotNone(table_def)
        
        # 3. Write data
        test_data = pa.table({
            'id': [1, 2, 3],
            'value': [10.5, 20.5, 30.5]
        })
        
        with patch('deltalake.write_deltalake'):
            unity_impl.write_to_table(
                data=test_data,
                table="test_table",
                namespace="test_ns",
                inner=inner,
                config=config
            )
        
        # 4. Read data
        with patch('deltacat.catalog.unity.impl.DeltaTable') as mock_delta_table:
            mock_dt = Mock()
            mock_dt.to_pyarrow_table.return_value = test_data
            mock_delta_table.return_value = mock_dt
            
            dataset = unity_impl.read_table(
                table="test_table",
                namespace="test_ns",
                inner=inner,
                config=config
            )
            self.assertIsNotNone(dataset)
        
        # 5. Drop table
        unity_impl.drop_table(
            table="test_table",
            namespace="test_ns",
            inner=inner,
            config=config
        )
        mock_client.workspace.tables.delete.assert_called_once()
    
    @patch('deltacat.catalog.unity.impl.UnityClient')
    def test_multi_format_support(self, mock_client_class):
        """Test working with Delta, Iceberg, and Hudi tables."""
        from deltacat.catalog.unity import impl as unity_impl
        from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig
        
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        config = UnityCatalogConfig(
            workspace_url="https://workspace.databricks.com",
            token="dapi123456789",
            catalog_name="main"
        )
        
        inner = unity_impl.initialize(config=config)
        
        # Test data
        test_data = pa.table({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C']
        })
        
        # Create and read Delta table
        mock_delta_table = Mock()
        mock_delta_table.data_source_format = "DELTA"
        mock_delta_table.storage_location = "s3://bucket/delta"
        mock_client.workspace.tables.get.return_value = mock_delta_table
        
        with patch('deltacat.catalog.unity.impl.DeltaTable') as mock_dt:
            mock_dt_instance = Mock()
            mock_dt_instance.to_pyarrow_table.return_value = test_data
            mock_dt.return_value = mock_dt_instance
            
            dataset = unity_impl.read_table(
                table="delta_table",
                namespace="test",
                inner=inner,
                config=config
            )
            self.assertIsNotNone(dataset)
        
        # Create and read Iceberg table
        mock_iceberg_table = Mock()
        mock_iceberg_table.data_source_format = "ICEBERG"
        mock_iceberg_table.storage_location = "s3://bucket/iceberg"
        mock_client.workspace.tables.get.return_value = mock_iceberg_table
        
        with patch('deltacat.catalog.unity.impl.load_table') as mock_load:
            mock_iceberg = Mock()
            mock_iceberg.scan().to_arrow.return_value = test_data
            mock_load.return_value = mock_iceberg
            
            dataset = unity_impl.read_table(
                table="iceberg_table",
                namespace="test",
                inner=inner,
                config=config
            )
            self.assertIsNotNone(dataset)


if __name__ == '__main__':
    unittest.main()