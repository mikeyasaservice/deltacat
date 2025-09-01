"""Tests for unified table format abstraction.

This test suite follows TDD principles - tests are written first to define
the expected behavior of the table format abstraction layer.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock, call
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pyarrow as pa
import pandas as pd
import tempfile
import os
import shutil

from deltacat.storage.model.schema import Schema, Field
from deltacat.types.media import ContentType


class TestTableFormatBase(unittest.TestCase):
    """Test the base TableFormat abstract class."""
    
    def test_table_format_is_abstract(self):
        """Test that TableFormat cannot be instantiated directly."""
        from deltacat.storage.formats.base import TableFormat
        
        with self.assertRaises(TypeError):
            # Should not be able to instantiate abstract class
            TableFormat("dummy_path")
    
    def test_table_format_defines_interface(self):
        """Test that TableFormat defines all required abstract methods."""
        from deltacat.storage.formats.base import TableFormat
        
        # Check that all expected methods are defined as abstract
        abstract_methods = TableFormat.__abstractmethods__
        expected_methods = {
            'read', 'write', 'append', 'overwrite', 'delete',
            'time_travel', 'get_history', 'optimize', 'vacuum',
            'get_schema', 'get_metadata', 'exists', 'get_format_type'
        }
        
        for method in expected_methods:
            self.assertIn(method, abstract_methods)


@patch('deltacat.storage.formats.delta.HAS_DELTA', True)
class TestDeltaFormat(unittest.TestCase):
    """Test Delta Lake format implementation."""
    
    def setUp(self):
        """Create temporary directory for test tables."""
        self.test_dir = tempfile.mkdtemp()
        self.table_path = os.path.join(self.test_dir, "test_delta_table")
    
    def tearDown(self):
        """Clean up test directory."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_delta_format_initialization(self):
        """Test DeltaFormat can be initialized with a path."""
        from deltacat.storage.formats.delta import DeltaFormat
        
        delta_table = DeltaFormat(self.table_path)
        self.assertEqual(delta_table.path, self.table_path)
        self.assertEqual(delta_table.get_format_type(), "delta")
    
    @patch('deltacat.storage.formats.delta.DeltaTable')
    def test_delta_read(self, mock_delta_table_class):
        """Test reading from a Delta table."""
        from deltacat.storage.formats.delta import DeltaFormat
        
        # Mock Delta table read
        mock_delta_table = Mock()
        mock_arrow_table = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        mock_delta_table.to_pyarrow_table.return_value = mock_arrow_table
        mock_delta_table_class.return_value = mock_delta_table
        
        # Read table
        delta_format = DeltaFormat(self.table_path)
        result = delta_format.read()
        
        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result.num_rows, 3)
        mock_delta_table_class.assert_called_once_with(self.table_path)
    
    @patch('deltacat.storage.formats.delta.write_deltalake')
    def test_delta_write(self, mock_write_deltalake):
        """Test writing to a Delta table."""
        from deltacat.storage.formats.delta import DeltaFormat
        
        # Create test data
        test_data = pa.table({
            'id': [1, 2, 3],
            'value': [10.5, 20.5, 30.5]
        })
        
        # Write table
        delta_format = DeltaFormat(self.table_path)
        delta_format.write(test_data)
        
        mock_write_deltalake.assert_called_once_with(
            self.table_path,
            test_data,
            mode='append'
        )
    
    @patch('deltacat.storage.formats.delta.write_deltalake')
    def test_delta_append(self, mock_write_deltalake):
        """Test appending to a Delta table."""
        from deltacat.storage.formats.delta import DeltaFormat
        
        test_data = pa.table({
            'id': [4, 5],
            'value': [40.5, 50.5]
        })
        
        delta_format = DeltaFormat(self.table_path)
        delta_format.append(test_data)
        
        mock_write_deltalake.assert_called_once_with(
            self.table_path,
            test_data,
            mode='append'
        )
    
    @patch('deltacat.storage.formats.delta.write_deltalake')
    def test_delta_overwrite(self, mock_write_deltalake):
        """Test overwriting a Delta table."""
        from deltacat.storage.formats.delta import DeltaFormat
        
        test_data = pa.table({
            'id': [10, 20],
            'value': [100.0, 200.0]
        })
        
        delta_format = DeltaFormat(self.table_path)
        delta_format.overwrite(test_data)
        
        mock_write_deltalake.assert_called_once_with(
            self.table_path,
            test_data,
            mode='overwrite'
        )
    
    @patch('deltacat.storage.formats.delta.DeltaTable')
    def test_delta_time_travel_by_version(self, mock_delta_table_class):
        """Test time travel by version for Delta tables."""
        from deltacat.storage.formats.delta import DeltaFormat
        
        # Mock versioned read
        mock_delta_table = Mock()
        mock_arrow_table = pa.table({'id': [1], 'name': ['Version1']})
        mock_delta_table.to_pyarrow_table.return_value = mock_arrow_table
        mock_delta_table_class.return_value = mock_delta_table
        
        delta_format = DeltaFormat(self.table_path)
        result = delta_format.time_travel(version=1)
        
        self.assertIsInstance(result, pa.Table)
        mock_delta_table.load_version.assert_called_once_with(1)
    
    @patch('deltacat.storage.formats.delta.DeltaTable')
    def test_delta_time_travel_by_timestamp(self, mock_delta_table_class):
        """Test time travel by timestamp for Delta tables."""
        from deltacat.storage.formats.delta import DeltaFormat
        
        # Mock timestamped read
        mock_delta_table = Mock()
        mock_arrow_table = pa.table({'id': [2], 'name': ['Timestamp']})
        mock_delta_table.to_pyarrow_table.return_value = mock_arrow_table
        mock_delta_table_class.return_value = mock_delta_table
        
        timestamp = datetime.now() - timedelta(days=1)
        delta_format = DeltaFormat(self.table_path)
        result = delta_format.time_travel(timestamp=timestamp)
        
        self.assertIsInstance(result, pa.Table)
        mock_delta_table.load_as_of_timestamp.assert_called_once_with(timestamp)
    
    @patch('deltacat.storage.formats.delta.DeltaTable')
    def test_delta_get_history(self, mock_delta_table_class):
        """Test getting history of Delta table versions."""
        from deltacat.storage.formats.delta import DeltaFormat
        
        # Mock history
        mock_delta_table = Mock()
        mock_history = [
            {'version': 0, 'timestamp': '2024-01-01', 'operation': 'CREATE'},
            {'version': 1, 'timestamp': '2024-01-02', 'operation': 'APPEND'},
        ]
        mock_delta_table.history.return_value = mock_history
        mock_delta_table_class.return_value = mock_delta_table
        
        delta_format = DeltaFormat(self.table_path)
        history = delta_format.get_history()
        
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]['version'], 0)
    
    @patch('deltacat.storage.formats.delta.DeltaTable')
    def test_delta_optimize(self, mock_delta_table_class):
        """Test optimizing a Delta table."""
        from deltacat.storage.formats.delta import DeltaFormat
        
        # Mock optimize operation
        mock_delta_table = Mock()
        mock_optimize = Mock()
        mock_optimize.files_added = 1
        mock_optimize.files_removed = 5
        mock_optimize.bytes_added = 1000
        mock_optimize.bytes_removed = 5000
        mock_optimize.partitions_optimized = 2
        mock_delta_table.optimize.return_value = mock_optimize
        mock_delta_table_class.return_value = mock_delta_table
        
        delta_format = DeltaFormat(self.table_path)
        result = delta_format.optimize()
        
        self.assertIn('files_added', result)
        self.assertEqual(result['files_added'], 1)
        mock_delta_table.optimize.assert_called_once()
    
    @patch('deltacat.storage.formats.delta.DeltaTable')
    def test_delta_vacuum(self, mock_delta_table_class):
        """Test vacuum operation on Delta table."""
        from deltacat.storage.formats.delta import DeltaFormat
        
        # Mock vacuum operation
        mock_delta_table = Mock()
        mock_vacuum_result = ['file1.parquet', 'file2.parquet']  # List of deleted files
        mock_delta_table.vacuum.return_value = mock_vacuum_result
        mock_delta_table_class.return_value = mock_delta_table
        
        delta_format = DeltaFormat(self.table_path)
        result = delta_format.vacuum(retention_hours=168)
        
        self.assertIn('files_deleted', result)
        self.assertEqual(result['files_deleted'], 2)
        mock_delta_table.vacuum.assert_called_once_with(
            retention_hours=168,
            dry_run=False
        )


@patch('deltacat.storage.formats.iceberg.HAS_ICEBERG', True)
class TestIcebergFormat(unittest.TestCase):
    """Test Apache Iceberg format implementation."""
    
    def setUp(self):
        """Create temporary directory for test tables."""
        self.test_dir = tempfile.mkdtemp()
        self.table_path = os.path.join(self.test_dir, "test_iceberg_table")
        self.catalog_name = "test_catalog"
        self.namespace = "test_namespace"
        self.table_name = "test_table"
    
    def tearDown(self):
        """Clean up test directory."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_iceberg_format_initialization(self):
        """Test IcebergFormat can be initialized."""
        from deltacat.storage.formats.iceberg import IcebergFormat
        
        iceberg_table = IcebergFormat(
            path=self.table_path,
            catalog_name=self.catalog_name,
            namespace=self.namespace,
            table_name=self.table_name
        )
        
        self.assertEqual(iceberg_table.path, self.table_path)
        self.assertEqual(iceberg_table.get_format_type(), "iceberg")
    
    @patch('deltacat.storage.formats.iceberg.load_table')
    def test_iceberg_read(self, mock_load_table):
        """Test reading from an Iceberg table."""
        from deltacat.storage.formats.iceberg import IcebergFormat
        
        # Mock Iceberg table read
        mock_table = Mock()
        mock_arrow_table = pa.table({
            'id': [1, 2, 3],
            'product': ['A', 'B', 'C']
        })
        mock_scan = Mock()
        mock_scan.to_arrow.return_value = mock_arrow_table
        mock_table.scan.return_value = mock_scan
        mock_load_table.return_value = mock_table
        
        # Read table
        iceberg_format = IcebergFormat(
            path=self.table_path,
            catalog_name=self.catalog_name,
            namespace=self.namespace,
            table_name=self.table_name
        )
        result = iceberg_format.read()
        
        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result.num_rows, 3)
    
    @patch('deltacat.storage.formats.iceberg.load_table')
    def test_iceberg_append(self, mock_load_table):
        """Test appending to an Iceberg table."""
        from deltacat.storage.formats.iceberg import IcebergFormat
        
        # Mock Iceberg table
        mock_table = Mock()
        mock_load_table.return_value = mock_table
        
        test_data = pa.table({
            'id': [4, 5],
            'product': ['D', 'E']
        })
        
        iceberg_format = IcebergFormat(
            path=self.table_path,
            catalog_name=self.catalog_name,
            namespace=self.namespace,
            table_name=self.table_name
        )
        iceberg_format.append(test_data)
        
        # Verify append was called
        mock_table.append.assert_called_once()
    
    @patch('deltacat.storage.formats.iceberg.load_table')
    def test_iceberg_time_travel(self, mock_load_table):
        """Test time travel for Iceberg tables."""
        from deltacat.storage.formats.iceberg import IcebergFormat
        
        # Mock Iceberg time travel
        mock_table = Mock()
        mock_arrow_table = pa.table({'id': [1], 'snapshot': ['old']})
        mock_scan = Mock()
        mock_scan.use_snapshot.return_value = mock_scan
        mock_scan.to_arrow.return_value = mock_arrow_table
        mock_table.scan.return_value = mock_scan
        mock_load_table.return_value = mock_table
        
        iceberg_format = IcebergFormat(
            path=self.table_path,
            catalog_name=self.catalog_name,
            namespace=self.namespace,
            table_name=self.table_name
        )
        
        # Time travel by snapshot ID
        result = iceberg_format.time_travel(version=12345)
        self.assertIsInstance(result, pa.Table)
        mock_scan.use_snapshot.assert_called_with(12345)
    
    @patch('deltacat.storage.formats.iceberg.load_table')
    def test_iceberg_get_history(self, mock_load_table):
        """Test getting history of Iceberg table."""
        from deltacat.storage.formats.iceberg import IcebergFormat
        
        # Mock Iceberg history
        mock_table = Mock()
        mock_snapshot1 = Mock()
        mock_snapshot1.snapshot_id = 1
        mock_snapshot1.timestamp_ms = 1704067200000
        mock_snapshot2 = Mock()
        mock_snapshot2.snapshot_id = 2
        mock_snapshot2.timestamp_ms = 1704153600000
        
        mock_table.history.return_value = [mock_snapshot1, mock_snapshot2]
        mock_load_table.return_value = mock_table
        
        iceberg_format = IcebergFormat(
            path=self.table_path,
            catalog_name=self.catalog_name,
            namespace=self.namespace,
            table_name=self.table_name
        )
        history = iceberg_format.get_history()
        
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]['snapshot_id'], 1)


class TestParquetFormat(unittest.TestCase):
    """Test Parquet format implementation (simple format without versioning)."""
    
    def setUp(self):
        """Create temporary directory for test files."""
        self.test_dir = tempfile.mkdtemp()
        self.file_path = os.path.join(self.test_dir, "test_data.parquet")
    
    def tearDown(self):
        """Clean up test directory."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_parquet_format_initialization(self):
        """Test ParquetFormat can be initialized."""
        from deltacat.storage.formats.parquet import ParquetFormat
        
        parquet_file = ParquetFormat(self.file_path)
        self.assertEqual(parquet_file.path, self.file_path)
        self.assertEqual(parquet_file.get_format_type(), "parquet")
    
    @patch('os.path.exists')
    @patch('deltacat.storage.formats.parquet.pq.read_table')
    def test_parquet_read(self, mock_read_table, mock_exists):
        """Test reading a Parquet file."""
        from deltacat.storage.formats.parquet import ParquetFormat
        
        # Mock file exists
        mock_exists.return_value = True
        
        # Mock Parquet read
        mock_arrow_table = pa.table({
            'id': [1, 2, 3],
            'value': [100, 200, 300]
        })
        mock_read_table.return_value = mock_arrow_table
        
        parquet_format = ParquetFormat(self.file_path)
        result = parquet_format.read()
        
        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result.num_rows, 3)
        mock_read_table.assert_called_once_with(self.file_path, columns=None)
    
    @patch('deltacat.storage.formats.parquet.pq.write_table')
    def test_parquet_write(self, mock_write_table):
        """Test writing a Parquet file."""
        from deltacat.storage.formats.parquet import ParquetFormat
        
        test_data = pa.table({
            'id': [1, 2, 3],
            'value': [100, 200, 300]
        })
        
        parquet_format = ParquetFormat(self.file_path)
        parquet_format.write(test_data)
        
        mock_write_table.assert_called_once_with(test_data, self.file_path)
    
    def test_parquet_no_time_travel(self):
        """Test that Parquet format doesn't support time travel."""
        from deltacat.storage.formats.parquet import ParquetFormat
        
        parquet_format = ParquetFormat(self.file_path)
        
        with self.assertRaises(NotImplementedError):
            parquet_format.time_travel(version=1)
    
    def test_parquet_no_history(self):
        """Test that Parquet format doesn't support history."""
        from deltacat.storage.formats.parquet import ParquetFormat
        
        parquet_format = ParquetFormat(self.file_path)
        
        with self.assertRaises(NotImplementedError):
            parquet_format.get_history()


@patch('deltacat.storage.formats.delta.HAS_DELTA', True)
@patch('deltacat.storage.formats.iceberg.HAS_ICEBERG', True)
class TestUnifiedFormatInterface(unittest.TestCase):
    """Test the unified format interface that auto-detects formats."""
    
    @patch('deltacat.storage.formats.unified.Path')
    @patch('os.path.exists')
    def test_detect_delta_format(self, mock_exists, mock_path_class):
        """Test auto-detection of Delta format."""
        from deltacat.storage.formats.unified import detect_format
        
        # Mock for Delta detection
        mock_path_instance = Mock()
        mock_delta_log = Mock()
        mock_delta_log.exists.return_value = True
        mock_delta_log.is_dir.return_value = True
        mock_path_instance.__truediv__ = Mock(return_value=mock_delta_log)
        mock_path_class.return_value = mock_path_instance
        mock_exists.return_value = True
        
        # Delta tables are directories with _delta_log
        format_type = detect_format("/path/to/delta/table")
        self.assertEqual(format_type, "delta")
    
    def test_detect_iceberg_format(self):
        """Test auto-detection of Iceberg format."""
        from deltacat.storage.formats.unified import detect_format
        
        # Iceberg tables have metadata.json
        format_type = detect_format("/path/to/iceberg/metadata/v1.metadata.json")
        self.assertEqual(format_type, "iceberg")
        
        # Also check .iceberg extension
        format_type = detect_format("/path/to/table.iceberg")
        self.assertEqual(format_type, "iceberg")
    
    def test_detect_parquet_format(self):
        """Test auto-detection of Parquet format."""
        from deltacat.storage.formats.unified import detect_format
        
        format_type = detect_format("/path/to/data.parquet")
        self.assertEqual(format_type, "parquet")
        
        format_type = detect_format("/path/to/data.pq")
        self.assertEqual(format_type, "parquet")
    
    def test_get_table_format_delta(self):
        """Test getting appropriate format handler for Delta."""
        from deltacat.storage.formats.unified import get_table_format
        from deltacat.storage.formats.delta import DeltaFormat
        
        table = get_table_format("/path/to/table.delta")
        self.assertIsInstance(table, DeltaFormat)
        self.assertEqual(table.get_format_type(), "delta")
    
    def test_get_table_format_iceberg(self):
        """Test getting appropriate format handler for Iceberg."""
        from deltacat.storage.formats.unified import get_table_format
        from deltacat.storage.formats.iceberg import IcebergFormat
        
        table = get_table_format(
            "/path/to/table.iceberg",
            catalog_name="test",
            namespace="ns",
            table_name="table"
        )
        self.assertIsInstance(table, IcebergFormat)
        self.assertEqual(table.get_format_type(), "iceberg")
    
    def test_get_table_format_parquet(self):
        """Test getting appropriate format handler for Parquet."""
        from deltacat.storage.formats.unified import get_table_format
        from deltacat.storage.formats.parquet import ParquetFormat
        
        table = get_table_format("/path/to/data.parquet")
        self.assertIsInstance(table, ParquetFormat)
        self.assertEqual(table.get_format_type(), "parquet")
    
    @patch('deltacat.storage.formats.unified.DeltaFormat')
    def test_unified_read_operation(self, mock_delta_format):
        """Test unified read operation across formats."""
        from deltacat.storage.formats.unified import read_table
        
        # Mock Delta format
        mock_table = Mock()
        mock_arrow_data = pa.table({'id': [1, 2], 'name': ['A', 'B']})
        mock_table.read.return_value = mock_arrow_data
        mock_delta_format.return_value = mock_table
        
        # Read using unified interface
        result = read_table("/path/to/table.delta")
        
        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result.num_rows, 2)
        mock_table.read.assert_called_once()
    
    @patch('deltacat.storage.formats.unified.DeltaFormat')
    def test_unified_write_operation(self, mock_delta_format):
        """Test unified write operation across formats."""
        from deltacat.storage.formats.unified import write_table
        
        # Mock Delta format
        mock_table = Mock()
        mock_delta_format.return_value = mock_table
        
        test_data = pa.table({'id': [1, 2], 'value': [10, 20]})
        
        # Write using unified interface
        write_table("/path/to/table.delta", test_data, mode='append')
        
        mock_table.append.assert_called_once_with(test_data)
    
    def test_format_conversion(self):
        """Test converting between formats."""
        from deltacat.storage.formats.unified import convert_format
        
        # Mock source and target formats
        with patch('deltacat.storage.formats.unified.get_table_format') as mock_get_format:
            # Mock source (Delta)
            mock_source = Mock()
            mock_data = pa.table({'id': [1, 2], 'value': [10, 20]})
            mock_source.read.return_value = mock_data
            mock_source.get_format_type.return_value = 'delta'
            
            # Mock target (Iceberg)
            mock_target = Mock()
            mock_target.get_format_type.return_value = 'iceberg'
            
            mock_get_format.side_effect = [mock_source, mock_target]
            
            # Convert Delta to Iceberg
            convert_format(
                source_path="/path/to/table.delta",
                target_path="/path/to/table.iceberg",
                target_format="iceberg"
            )
            
            # Verify data was read from source and written to target
            mock_source.read.assert_called_once()
            mock_target.write.assert_called_once_with(mock_data, mode='overwrite', partition_by=None)


class TestFormatIntegration(unittest.TestCase):
    """Integration tests for format abstraction with DeltaCAT."""
    
    @patch('deltacat.catalog.get_table')
    def test_format_with_catalog(self, mock_get_table):
        """Test format abstraction works with DeltaCAT catalog."""
        from deltacat.storage.formats.unified import get_table_format_from_catalog
        
        # Mock table definition
        mock_table_def = Mock()
        mock_table_def.table.locator = "s3://bucket/path/to/table.delta"
        mock_table_def.namespace = "test_namespace"
        mock_get_table.return_value = mock_table_def
        
        # Get format from catalog  
        mock_catalog = Mock()
        mock_catalog.get_table = mock_get_table
        table_format = get_table_format_from_catalog(
            catalog=mock_catalog,
            table_name="my_table",
            namespace="test_namespace"
        )
        
        self.assertIsNotNone(table_format)
        self.assertEqual(table_format.get_format_type(), "delta")
    
    def test_format_schema_conversion(self):
        """Test converting between DeltaCAT Schema and Arrow schema."""
        from deltacat.storage.formats.unified import arrow_to_deltacat_schema
        
        # Create Arrow schema
        arrow_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("amount", pa.float64())
        ])
        
        # Convert to DeltaCAT schema
        deltacat_schema = arrow_to_deltacat_schema(arrow_schema)
        
        self.assertEqual(len(deltacat_schema.fields), 3)
        self.assertEqual(deltacat_schema.fields[0].arrow.name, "id")
        self.assertEqual(deltacat_schema.fields[1].arrow.name, "name")
        self.assertEqual(deltacat_schema.fields[2].arrow.name, "amount")


if __name__ == '__main__':
    unittest.main()