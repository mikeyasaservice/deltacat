"""Tests for DeltaCAT SQL Gateway."""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pyarrow as pa
import pyarrow.dataset as ds
import duckdb

from deltacat.sql.gateway import DeltaCATSQLGateway
from deltacat.sql.catalog_adapter import CatalogAdapter


class TestDeltaCATSQLGateway(unittest.TestCase):
    """Test cases for the SQL Gateway."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock catalog initialization
        self.mock_catalog_init = patch('deltacat.sql.gateway.raise_if_not_initialized')
        self.mock_catalog_init.start()
        
        # Create test Arrow table
        self.test_data = pa.table({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': [100, 200, 150, 300, 250],
        })
        
        # Create test Arrow dataset
        self.test_dataset = ds.dataset(self.test_data)
        
    def tearDown(self):
        """Clean up test fixtures."""
        self.mock_catalog_init.stop()
        
    @patch('deltacat.sql.gateway.CatalogAdapter')
    def test_gateway_initialization(self, mock_adapter_class):
        """Test SQL gateway initialization."""
        # Setup mock
        mock_adapter = Mock(spec=CatalogAdapter)
        mock_adapter.list_all_tables.return_value = []
        mock_adapter_class.return_value = mock_adapter
        
        # Create gateway
        gateway = DeltaCATSQLGateway(catalog_name="test_catalog", auto_register_tables=False)
        
        # Verify initialization
        self.assertIsNotNone(gateway.connection)
        self.assertEqual(gateway.catalog_name, "test_catalog")
        mock_adapter_class.assert_called_once_with("test_catalog")
        
    @patch('deltacat.sql.gateway.CatalogAdapter')
    def test_register_table(self, mock_adapter_class):
        """Test registering a table with DuckDB."""
        # Setup mock
        mock_adapter = Mock(spec=CatalogAdapter)
        mock_adapter.list_all_tables.return_value = []
        mock_adapter.get_table_as_arrow_dataset.return_value = self.test_dataset
        mock_adapter_class.return_value = mock_adapter
        
        # Create gateway
        gateway = DeltaCATSQLGateway(auto_register_tables=False)
        
        # Register table
        success = gateway.register_table("test_table", "test_namespace")
        
        # Verify registration
        self.assertTrue(success)
        mock_adapter.get_table_as_arrow_dataset.assert_called_once_with(
            "test_table", "test_namespace"
        )
        
        # Verify we can query the table
        result = gateway.sql("SELECT COUNT(*) as cnt FROM test_namespace_test_table")
        self.assertEqual(result['cnt'][0].as_py(), 5)
        
    @patch('deltacat.sql.gateway.CatalogAdapter')
    def test_sql_query(self, mock_adapter_class):
        """Test executing SQL queries."""
        # Setup mock
        mock_adapter = Mock(spec=CatalogAdapter)
        mock_adapter.list_all_tables.return_value = [("test", "users")]
        mock_adapter.get_table_as_arrow_dataset.return_value = self.test_dataset
        mock_adapter_class.return_value = mock_adapter
        
        # Create gateway and register table
        gateway = DeltaCATSQLGateway(auto_register_tables=False)
        gateway.register_table("users", "test", alias="users")
        
        # Test SELECT query
        result = gateway.sql("SELECT name, value FROM users WHERE value > 150")
        
        # Verify results
        self.assertIsInstance(result, pa.Table)
        self.assertEqual(len(result), 3)  # Bob, David, Eve
        self.assertIn('Bob', result['name'].to_pylist())
        
    @patch('deltacat.sql.gateway.CatalogAdapter')
    def test_sql_aggregation(self, mock_adapter_class):
        """Test SQL aggregation queries."""
        # Setup mock
        mock_adapter = Mock(spec=CatalogAdapter)
        mock_adapter.list_all_tables.return_value = []
        mock_adapter.get_table_as_arrow_dataset.return_value = self.test_dataset
        mock_adapter_class.return_value = mock_adapter
        
        # Create gateway
        gateway = DeltaCATSQLGateway(auto_register_tables=False)
        gateway.register_table("data", alias="data")
        
        # Test aggregation
        result = gateway.sql("""
            SELECT 
                COUNT(*) as count,
                SUM(value) as total,
                AVG(value) as average,
                MAX(value) as maximum,
                MIN(value) as minimum
            FROM data
        """)
        
        # Verify results
        self.assertEqual(result['count'][0].as_py(), 5)
        self.assertEqual(result['total'][0].as_py(), 1000)
        self.assertEqual(result['average'][0].as_py(), 200.0)
        self.assertEqual(result['maximum'][0].as_py(), 300)
        self.assertEqual(result['minimum'][0].as_py(), 100)
        
    @patch('deltacat.sql.gateway.CatalogAdapter')
    def test_explain_query(self, mock_adapter_class):
        """Test query explanation."""
        # Setup mock
        mock_adapter = Mock(spec=CatalogAdapter)
        mock_adapter.list_all_tables.return_value = []
        mock_adapter.get_table_as_arrow_dataset.return_value = self.test_dataset
        mock_adapter_class.return_value = mock_adapter
        
        # Create gateway
        gateway = DeltaCATSQLGateway(auto_register_tables=False)
        gateway.register_table("test", alias="test")
        
        # Get query plan
        plan = gateway.explain("SELECT * FROM test WHERE value > 200")
        
        # Verify we got a plan
        self.assertIsInstance(plan, str)
        # Note: Actual plan content depends on DuckDB version
        
    @patch('deltacat.sql.gateway.catalog_create_table')
    @patch('deltacat.sql.gateway.CatalogAdapter')
    def test_create_table_ddl(self, mock_adapter_class, mock_create):
        """Test CREATE TABLE DDL operation."""
        # Setup mocks
        mock_adapter = Mock(spec=CatalogAdapter)
        mock_adapter.list_all_tables.return_value = []
        mock_adapter_class.return_value = mock_adapter
        mock_create.return_value = Mock()
        
        # Create gateway
        gateway = DeltaCATSQLGateway(auto_register_tables=False)
        
        # Create table
        schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
        ])
        
        success = gateway.create_table(
            "new_table",
            schema,
            namespace="test"
        )
        
        # Verify creation
        self.assertTrue(success)
        mock_create.assert_called_once()
        
    @patch('deltacat.sql.gateway.table_exists')
    @patch('deltacat.sql.gateway.catalog_drop_table')
    @patch('deltacat.sql.gateway.CatalogAdapter')
    def test_drop_table_ddl(self, mock_adapter_class, mock_drop, mock_exists):
        """Test DROP TABLE DDL operation."""
        # Setup mocks
        mock_adapter = Mock(spec=CatalogAdapter)
        mock_adapter.list_all_tables.return_value = []
        mock_adapter.clear_cache = Mock()
        mock_adapter_class.return_value = mock_adapter
        mock_exists.return_value = True
        
        # Create gateway
        gateway = DeltaCATSQLGateway(auto_register_tables=False)
        
        # Drop table
        success = gateway.drop_table("old_table", namespace="test")
        
        # Verify drop
        self.assertTrue(success)
        mock_drop.assert_called_once_with(
            name="old_table",
            namespace="test",
            catalog=None
        )
        mock_adapter.clear_cache.assert_called_once()


class TestCatalogAdapter(unittest.TestCase):
    """Test cases for the Catalog Adapter."""
    
    @patch('deltacat.sql.catalog_adapter.get_table')
    @patch('deltacat.sql.catalog_adapter.list_tables')
    @patch('deltacat.sql.catalog_adapter.list_namespaces')
    def test_list_all_tables(self, mock_list_ns, mock_list_tables, mock_get_table):
        """Test listing all tables from catalog."""
        # Setup mocks
        ns1 = Mock()
        ns1.name = "namespace1"
        ns2 = Mock()
        ns2.name = "namespace2"
        
        mock_list_ns.return_value = Mock(all_items=lambda: [ns1, ns2])
        
        table1 = Mock()
        table1.table.name = "table1"
        table2 = Mock()
        table2.table.name = "table2"
        
        mock_list_tables.return_value = Mock(all_items=lambda: [table1, table2])
        
        # Create adapter
        adapter = CatalogAdapter("test_catalog")
        
        # List tables
        tables = adapter.list_all_tables()
        
        # Verify results
        self.assertEqual(len(tables), 4)  # 2 namespaces × 2 tables
        self.assertIn(("namespace1", "table1"), tables)
        self.assertIn(("namespace2", "table2"), tables)
        
    def test_arrow_dataset_caching(self):
        """Test that Arrow datasets are cached."""
        adapter = CatalogAdapter()
        
        # Create mock dataset
        mock_dataset = Mock(spec=ds.Dataset)
        
        # Add to cache
        adapter._table_cache["test.table"] = mock_dataset
        
        # Verify cached
        cached = adapter._table_cache.get("test.table")
        self.assertEqual(cached, mock_dataset)
        
        # Clear cache
        adapter.clear_cache()
        self.assertEqual(len(adapter._table_cache), 0)


if __name__ == '__main__':
    unittest.main()