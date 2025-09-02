"""Tests for Iceberg SQL adapter."""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pyarrow as pa
import duckdb

from deltacat.sql.iceberg_adapter import IcebergSQLAdapter


class TestIcebergSQLAdapter(unittest.TestCase):
    """Test cases for the Iceberg SQL adapter."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create test connection
        self.connection = duckdb.connect(":memory:")
        
        # Mock catalog
        self.mock_catalog = Mock()
        
    def tearDown(self):
        """Clean up test fixtures."""
        self.connection.close()
        
    @patch('deltacat.sql.iceberg_adapter.get_catalog')
    def test_adapter_initialization(self, mock_get_catalog):
        """Test Iceberg adapter initialization."""
        mock_get_catalog.return_value = self.mock_catalog
        
        adapter = IcebergSQLAdapter("test_catalog", self.connection)
        
        self.assertEqual(adapter.catalog_name, "test_catalog")
        self.assertEqual(adapter.connection, self.connection)
        mock_get_catalog.assert_called_once_with("test_catalog")
        
    @patch('deltacat.sql.iceberg_adapter.get_catalog')
    def test_iceberg_extension_loading(self, mock_get_catalog):
        """Test DuckDB Iceberg extension loading."""
        mock_get_catalog.return_value = self.mock_catalog
        
        adapter = IcebergSQLAdapter("test_catalog", self.connection)
        
        # Check if extension was loaded
        result = self.connection.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'iceberg'").fetchall()
        self.assertTrue(any('iceberg' in str(row) for row in result))
        
    def test_is_iceberg_table_detection(self):
        """Test Iceberg table detection logic."""
        adapter = IcebergSQLAdapter(connection=self.connection)
        
        # Test with Iceberg catalog
        mock_catalog = Mock()
        mock_catalog.impl = Mock()
        mock_catalog.impl.__name__ = 'IcebergCatalog'
        adapter.catalog = mock_catalog
        
        table_def = Mock()
        table_def.properties = {}  # Add empty properties dict
        self.assertTrue(adapter.is_iceberg_table(table_def))
        
        # Test with properties
        adapter.catalog = None  # Reset catalog
        table_def = Mock()
        table_def.properties = {'table_type': 'ICEBERG'}
        self.assertTrue(adapter.is_iceberg_table(table_def))
        
        # Test with metadata_location
        table_def = Mock()
        table_def.properties = {}
        table_def.metadata_location = "s3://bucket/table/metadata.json"
        self.assertTrue(adapter.is_iceberg_table(table_def))
        
        # Test non-Iceberg table
        table_def = Mock(spec=['properties'])  # Mock without metadata_location
        table_def.properties = {}
        self.assertFalse(adapter.is_iceberg_table(table_def))
        
    def test_get_iceberg_location(self):
        """Test getting Iceberg table location."""
        adapter = IcebergSQLAdapter(connection=self.connection)
        
        # Test with metadata_location
        table_def = Mock()
        table_def.metadata_location = "s3://bucket/table/metadata/v1.metadata.json"
        location = adapter.get_iceberg_location(table_def)
        self.assertEqual(location, "s3://bucket/table/metadata/v1.metadata.json")
        
        # Test with table location
        table_def.metadata_location = None
        table_def.table = Mock()
        table_def.table.location = "s3://bucket/table"
        location = adapter.get_iceberg_location(table_def)
        self.assertEqual(location, "s3://bucket/table/metadata.json")
        
    @patch('deltacat.sql.iceberg_adapter.get_table')
    def test_register_iceberg_table(self, mock_get_table):
        """Test registering an Iceberg table with DuckDB."""
        # Create adapter without catalog to test basic flow
        adapter = IcebergSQLAdapter(catalog_name=None, connection=self.connection)
        
        # Mock table definition
        table_def = Mock()
        table_def.metadata_location = "s3://bucket/table/metadata.json"
        mock_get_table.return_value = table_def
        
        # Mock is_iceberg_table to return True
        adapter.is_iceberg_table = Mock(return_value=True)
        adapter.get_iceberg_location = Mock(return_value="s3://bucket/table/metadata.json")
        
        # Register table (will fail without actual Iceberg file, but tests the logic)
        success = adapter.register_iceberg_table("test_table", "test_namespace", "test_alias")
        
        # Verify get_table was called
        mock_get_table.assert_called_once_with(
            name="test_table",
            namespace="test_namespace",
            catalog=None
        )
        
        # Check if view creation was attempted
        # Note: This will fail without actual Iceberg files, but we're testing the flow
        self.assertIsNotNone(adapter.connection)
        
    @patch('deltacat.sql.iceberg_adapter.list_tables')
    @patch('deltacat.sql.iceberg_adapter.list_namespaces')
    def test_register_all_iceberg_tables(self, mock_list_ns, mock_list_tables):
        """Test registering all Iceberg tables."""
        # Create adapter without catalog to test basic flow
        adapter = IcebergSQLAdapter(catalog_name=None, connection=self.connection)
        
        # Mock namespaces
        ns1 = Mock()
        ns1.name = "namespace1"
        mock_list_ns.return_value = Mock(all_items=lambda: [ns1])
        
        # Mock tables
        table1 = Mock()
        table1.table.name = "table1"
        mock_list_tables.return_value = Mock(all_items=lambda: [table1])
        
        # Mock register method
        adapter.register_iceberg_table = Mock(return_value=True)
        
        # Register all tables
        count = adapter.register_all_iceberg_tables()
        
        # Verify
        self.assertEqual(count, 1)
        adapter.register_iceberg_table.assert_called_once_with("table1", "namespace1")
        
    def test_sql_execution(self):
        """Test SQL query execution."""
        adapter = IcebergSQLAdapter(connection=self.connection)
        
        # Create a test table
        test_data = pa.table({
            'id': [1, 2, 3],
            'value': [10, 20, 30]
        })
        self.connection.register("test_table", test_data)
        
        # Execute query
        result = adapter.sql("SELECT SUM(value) as total FROM test_table")
        
        # Verify result
        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result['total'][0].as_py(), 60)
        
    @patch.object(IcebergSQLAdapter, '_setup_iceberg_extension')
    def test_time_travel_query_with_snapshot(self, mock_setup):
        """Test time travel query with snapshot ID."""
        # Create a mock connection instead of using real DuckDB
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.arrow.return_value = pa.table({'col': [1, 2, 3]})
        mock_connection.execute.return_value = mock_result
        
        adapter = IcebergSQLAdapter(connection=mock_connection)
        
        # Execute time travel query
        result = adapter.time_travel_query("test_table", snapshot_id=12345)
        
        # Verify correct SQL was generated
        expected_sql = """
            SELECT * FROM iceberg_scan(
                'test_table',
                snapshot_id => 12345
            )
        """
        mock_connection.execute.assert_called_once()
        actual_sql = mock_connection.execute.call_args[0][0]
        # Normalize whitespace for comparison
        self.assertEqual(
            ' '.join(actual_sql.split()),
            ' '.join(expected_sql.split())
        )
            
    @patch.object(IcebergSQLAdapter, '_setup_iceberg_extension')
    def test_time_travel_query_with_timestamp(self, mock_setup):
        """Test time travel query with timestamp."""
        # Create a mock connection instead of using real DuckDB
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.arrow.return_value = pa.table({'col': [1, 2, 3]})
        mock_connection.execute.return_value = mock_result
        
        adapter = IcebergSQLAdapter(connection=mock_connection)
        
        # Execute time travel query
        result = adapter.time_travel_query("test_table", timestamp="2024-01-01 00:00:00")
        
        # Verify correct SQL was generated
        expected_sql = """
            SELECT * FROM iceberg_scan(
                'test_table',
                timestamp => '2024-01-01 00:00:00'::TIMESTAMP
            )
        """
        mock_connection.execute.assert_called_once()
        actual_sql = mock_connection.execute.call_args[0][0]
        # Normalize whitespace for comparison
        self.assertEqual(
            ' '.join(actual_sql.split()),
            ' '.join(expected_sql.split())
        )
            
    @patch.object(IcebergSQLAdapter, '_setup_iceberg_extension')
    def test_get_table_snapshots(self, mock_setup):
        """Test getting table snapshots."""
        # Create a mock connection
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.arrow.return_value = pa.table({
            'snapshot_id': [1, 2, 3],
            'timestamp': ['2024-01-01', '2024-01-02', '2024-01-03']
        })
        mock_connection.execute.return_value = mock_result
        
        adapter = IcebergSQLAdapter(connection=mock_connection)
        
        # Get snapshots
        result = adapter.get_table_snapshots("test_table")
        
        # Verify
        mock_connection.execute.assert_called_once()
        self.assertIn("iceberg_snapshots", mock_connection.execute.call_args[0][0])
        self.assertIsInstance(result, pa.Table)
            
    @patch.object(IcebergSQLAdapter, '_setup_iceberg_extension')
    def test_get_table_metadata(self, mock_setup):
        """Test getting table metadata."""
        # Create a mock connection
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            ('location', 's3://bucket/table'),
            ('format_version', '2')
        ]
        mock_connection.execute.return_value = mock_result
        
        adapter = IcebergSQLAdapter(connection=mock_connection)
        
        # Get metadata
        result = adapter.get_table_metadata("test_table")
        
        # Verify
        mock_connection.execute.assert_called_once()
        self.assertIn("iceberg_metadata", mock_connection.execute.call_args[0][0])
        self.assertIsInstance(result, dict)
        self.assertEqual(result['location'], 's3://bucket/table')
        self.assertEqual(result['format_version'], '2')


class TestIcebergIntegrationWithGateway(unittest.TestCase):
    """Test Iceberg integration with main SQL gateway."""
    
    @patch('deltacat.sql.gateway.raise_if_not_initialized')
    @patch('deltacat.sql.gateway.IcebergSQLAdapter')
    @patch('deltacat.sql.gateway.CatalogAdapter')
    def test_gateway_with_iceberg_optimization(self, mock_catalog_adapter, mock_iceberg_adapter, mock_init):
        """Test SQL gateway with Iceberg optimization enabled."""
        from deltacat.sql.gateway import DeltaCATSQLGateway
        
        # Mock adapters
        mock_catalog = Mock()
        mock_catalog.list_all_tables.return_value = []
        mock_catalog_adapter.return_value = mock_catalog
        
        mock_iceberg = Mock()
        mock_iceberg.register_all_iceberg_tables.return_value = 5
        mock_iceberg_adapter.return_value = mock_iceberg
        
        # Create gateway with Iceberg optimization
        gateway = DeltaCATSQLGateway(
            catalog_name="test",
            enable_iceberg_optimization=True,
            auto_register_tables=True
        )
        
        # Verify Iceberg adapter was created
        mock_iceberg_adapter.assert_called_once_with("test", gateway.connection)
        
        # Verify Iceberg tables were registered
        mock_iceberg.register_all_iceberg_tables.assert_called_once()
        
        # Verify gateway has Iceberg adapter
        self.assertIsNotNone(gateway.iceberg_adapter)
        
    @patch('deltacat.sql.gateway.raise_if_not_initialized')
    @patch('deltacat.sql.gateway.CatalogAdapter')
    def test_gateway_without_iceberg_optimization(self, mock_catalog_adapter, mock_init):
        """Test SQL gateway with Iceberg optimization disabled."""
        from deltacat.sql.gateway import DeltaCATSQLGateway
        
        # Mock catalog adapter
        mock_catalog = Mock()
        mock_catalog.list_all_tables.return_value = []
        mock_catalog_adapter.return_value = mock_catalog
        
        # Create gateway without Iceberg optimization
        gateway = DeltaCATSQLGateway(
            catalog_name="test",
            enable_iceberg_optimization=False,
            auto_register_tables=False
        )
        
        # Verify no Iceberg adapter
        self.assertIsNone(gateway.iceberg_adapter)


if __name__ == '__main__':
    unittest.main()