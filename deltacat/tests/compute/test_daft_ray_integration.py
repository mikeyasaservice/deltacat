"""Integration tests for Daft and Ray SQL execution implementations.

These tests verify the actual implementations of _execute_daft() and _execute_ray()
methods, as well as the deltacat_table_to_daft_dataframe() conversion function.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
import pyarrow as pa
import pandas as pd

from deltacat.compute.engine import UnifiedComputeEngine
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage import Table, TableVersion, Stream


class TestDaftExecution:
    """Test the Daft SQL execution implementation."""
    
    @patch('deltacat.compute.engine.get_table')
    @patch('deltacat.utils.daft.DataFrame.from_scan_operator')
    @patch('daft.sql')
    def test_execute_daft_single_table(self, mock_sql, mock_from_scan, mock_get_table):
        """Test _execute_daft with a single table query."""
        # Setup mocks
        mock_table_def = Mock(spec=TableDefinition)
        mock_table_def.table.namespace = "test_namespace"
        mock_table_def.table.table_name = "test_table"
        mock_table_def.table_version = Mock()
        mock_get_table.return_value = mock_table_def
        
        mock_df = Mock()
        mock_from_scan.return_value = mock_df
        
        expected_result = pa.table({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_result_df = Mock()
        mock_result_df.to_arrow.return_value = expected_result
        mock_sql.return_value = mock_result_df
        
        # Execute
        engine = UnifiedComputeEngine(catalog_name="test_catalog")
        result = engine._execute_daft("SELECT * FROM test_table WHERE col1 > 0")
        
        # Verify
        assert mock_get_table.called
        assert mock_from_scan.called
        mock_sql.assert_called_once()
        
        # Check SQL was called with proper catalog
        sql_call_args = mock_sql.call_args
        assert "test_table" in sql_call_args[1]["catalog"]
        
        assert isinstance(result, pa.Table)
        assert result == expected_result
    
    @patch('deltacat.compute.engine.get_table')
    @patch('deltacat.utils.daft.DataFrame.from_scan_operator')
    @patch('daft.sql')
    def test_execute_daft_join_query(self, mock_sql, mock_from_scan, mock_get_table):
        """Test _execute_daft with a JOIN query involving multiple tables."""
        # Setup mocks for two tables
        def get_table_side_effect(name, namespace, catalog):
            mock_table_def = Mock(spec=TableDefinition)
            mock_table_def.table.namespace = namespace or "default"
            mock_table_def.table.table_name = name
            mock_table_def.table_version = Mock()
            return mock_table_def
        
        mock_get_table.side_effect = get_table_side_effect
        
        mock_df1 = Mock()
        mock_df2 = Mock()
        mock_from_scan.side_effect = [mock_df1, mock_df2]
        
        expected_result = pa.table({"order_id": [1, 2], "customer_name": ["Alice", "Bob"]})
        mock_result_df = Mock()
        mock_result_df.to_arrow.return_value = expected_result
        mock_sql.return_value = mock_result_df
        
        # Execute
        engine = UnifiedComputeEngine(catalog_name="test_catalog")
        result = engine._execute_daft(
            "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"
        )
        
        # Verify both tables were registered
        assert mock_get_table.call_count == 2
        assert mock_from_scan.call_count == 2
        
        # Check SQL was called with both tables in catalog
        sql_call_args = mock_sql.call_args
        catalog = sql_call_args[1]["catalog"]
        assert "orders" in catalog
        assert "customers" in catalog
        
        assert isinstance(result, pa.Table)
    
    @patch('deltacat.compute.engine.get_table')
    def test_execute_daft_missing_table(self, mock_get_table):
        """Test _execute_daft handles missing tables gracefully."""
        mock_get_table.return_value = None
        
        engine = UnifiedComputeEngine(catalog_name="test_catalog")
        
        with patch('daft.sql') as mock_sql:
            mock_result_df = Mock()
            mock_result_df.to_arrow.return_value = pa.table({"col": []})
            mock_sql.return_value = mock_result_df
            
            # Should still execute but with warning
            result = engine._execute_daft("SELECT * FROM nonexistent_table")
            
            # SQL called with empty catalog
            sql_call_args = mock_sql.call_args
            catalog = sql_call_args[1]["catalog"]
            assert len(catalog) == 0


class TestRayExecution:
    """Test the Ray SQL execution implementation."""
    
    @patch('ray.is_initialized')
    @patch('ray.init')
    @patch('ray.get')
    @patch('deltacat.compute.engine.get_table')
    def test_execute_ray_ml_operations(self, mock_get_table, mock_ray_get, 
                                       mock_ray_init, mock_ray_initialized):
        """Test _execute_ray routes ML operations correctly."""
        # Setup
        mock_ray_initialized.return_value = False
        
        mock_table_def = Mock(spec=TableDefinition)
        mock_table_def.table.namespace = "ml"
        mock_table_def.table.table_name = "features"
        mock_table_def.table_version = Mock()
        
        # Mock scan plan with data files
        mock_scan_plan = Mock()
        mock_scan_task = Mock()
        mock_data_file = Mock()
        mock_data_file.file_path = "s3://bucket/data.parquet"
        mock_scan_task.data_files.return_value = [mock_data_file]
        mock_scan_plan.scan_tasks = [mock_scan_task]
        mock_table_def.create_scan_plan.return_value = mock_scan_plan
        
        mock_get_table.return_value = mock_table_def
        
        # Mock Ray execution result
        expected_result = pa.table({"prediction": [0.8, 0.9, 0.7]})
        mock_ray_get.return_value = pa.serialize(expected_result).to_buffer().to_pybytes()
        
        # Execute
        engine = UnifiedComputeEngine(catalog_name="test_catalog")
        
        with patch('ray.data.read_parquet') as mock_read_parquet:
            mock_dataset = Mock()
            mock_dataset.count.return_value = 1000
            mock_dataset.to_pandas.return_value = pd.DataFrame({"features": [1, 2, 3]})
            mock_read_parquet.return_value = mock_dataset
            
            result = engine._execute_ray(
                "SELECT *, PREDICT(model, features) FROM features"
            )
        
        # Verify
        assert mock_ray_init.called
        assert isinstance(result, pa.Table)
    
    @patch('ray.is_initialized')
    @patch('ray.get')
    @patch('deltacat.compute.engine.get_table')
    def test_execute_ray_standard_query(self, mock_get_table, mock_ray_get, 
                                        mock_ray_initialized):
        """Test _execute_ray falls back to Daft for standard SQL."""
        # Setup
        mock_ray_initialized.return_value = True
        
        mock_table_def = Mock(spec=TableDefinition)
        mock_table_def.table.namespace = "default"
        mock_table_def.table.table_name = "sales"
        mock_table_def.table_version = Mock()
        
        # Mock scan plan
        mock_scan_plan = Mock()
        mock_scan_plan.scan_tasks = []
        mock_table_def.create_scan_plan.return_value = mock_scan_plan
        
        mock_get_table.return_value = mock_table_def
        
        # Mock Ray execution result
        expected_result = pa.table({"total": [100, 200, 300]})
        mock_ray_get.return_value = pa.serialize(expected_result).to_buffer().to_pybytes()
        
        # Execute
        engine = UnifiedComputeEngine(catalog_name="test_catalog")
        result = engine._execute_ray("SELECT SUM(amount) as total FROM sales GROUP BY region")
        
        # Verify it used the Daft-within-Ray approach
        assert mock_ray_get.called
        assert isinstance(result, pa.Table)
    
    @patch('ray.is_initialized')
    @patch('ray.cluster_resources')
    @patch('deltacat.compute.engine.get_table')
    def test_execute_ray_with_multiple_tables(self, mock_get_table, 
                                              mock_cluster_resources,
                                              mock_ray_initialized):
        """Test _execute_ray handles multiple tables."""
        # Setup
        mock_ray_initialized.return_value = True
        mock_cluster_resources.return_value = {"CPU": 16}
        
        def get_table_side_effect(name, namespace, catalog):
            mock_table_def = Mock(spec=TableDefinition)
            mock_table_def.table.namespace = namespace or "default"
            mock_table_def.table.table_name = name
            mock_table_def.table_version = Mock()
            
            mock_scan_plan = Mock()
            mock_scan_plan.scan_tasks = []
            mock_table_def.create_scan_plan.return_value = mock_scan_plan
            
            return mock_table_def
        
        mock_get_table.side_effect = get_table_side_effect
        
        # Execute
        engine = UnifiedComputeEngine(catalog_name="test_catalog")
        
        with patch('ray.get') as mock_ray_get:
            expected_result = pa.table({"result": [1, 2, 3]})
            mock_ray_get.return_value = pa.serialize(expected_result).to_buffer().to_pybytes()
            
            result = engine._execute_ray(
                "SELECT * FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id"
            )
        
        # Verify
        assert mock_get_table.call_count >= 2  # Called for each table
        assert isinstance(result, pa.Table)


class TestDeltaCATToDaftConversion:
    """Test the deltacat_table_to_daft_dataframe conversion function."""
    
    @patch('ray.is_initialized')
    @patch('ray.init')
    @patch('daft.context.set_runner_ray')
    @patch('deltacat.utils.daft.DataFrame.from_scan_operator')
    def test_deltacat_to_daft_conversion(self, mock_from_scan, mock_set_runner,
                                         mock_ray_init, mock_ray_initialized):
        """Test successful conversion of DeltaCAT table to Daft DataFrame."""
        from deltacat.utils.daft import deltacat_table_to_daft_dataframe
        from daft.io import IOConfig, S3Config
        
        # Setup
        mock_ray_initialized.return_value = False
        
        # Create mock table definition
        mock_table = Mock(spec=Table)
        mock_table.namespace = "test_ns"
        mock_table.table_name = "test_table"
        
        mock_table_version = Mock(spec=TableVersion)
        mock_stream = Mock(spec=Stream)
        
        table_def = TableDefinition.of(
            table=mock_table,
            table_version=mock_table_version,
            stream=mock_stream
        )
        
        # Mock DataFrame
        mock_df = Mock()
        mock_from_scan.return_value = mock_df
        
        # Execute
        io_config = IOConfig(
            s3=S3Config(
                region_name="us-east-1",
                retry_mode="adaptive",
            )
        )
        
        result = deltacat_table_to_daft_dataframe(table_def, io_config)
        
        # Verify
        assert mock_ray_init.called
        assert mock_set_runner.called
        assert mock_from_scan.called
        
        # Check scan operator was created
        scan_op_call = mock_from_scan.call_args[0][0]
        assert scan_op_call.table == table_def
        
        assert result == mock_df
    
    def test_deltacat_to_daft_missing_table_version(self):
        """Test conversion fails gracefully with missing table version."""
        from deltacat.utils.daft import deltacat_table_to_daft_dataframe
        
        # Create table def without version
        mock_table = Mock(spec=Table)
        mock_table.namespace = "test_ns"
        mock_table.table_name = "test_table"
        
        table_def = TableDefinition.of(
            table=mock_table,
            table_version=None,  # Missing version
            stream=Mock()
        )
        
        # Should raise RuntimeError
        with pytest.raises(RuntimeError) as exc_info:
            deltacat_table_to_daft_dataframe(table_def)
        
        assert "TableVersion is missing" in str(exc_info.value)
    
    def test_deltacat_to_daft_none_table_definition(self):
        """Test conversion handles None table definition."""
        from deltacat.utils.daft import deltacat_table_to_daft_dataframe
        
        with pytest.raises(RuntimeError) as exc_info:
            deltacat_table_to_daft_dataframe(None)
        
        assert "TableDefinition is required" in str(exc_info.value)


class TestEndToEndIntegration:
    """End-to-end integration tests for the compute engine."""
    
    @patch('deltacat.compute.engine.get_table')
    @patch('deltacat.utils.daft.DataFrame.from_scan_operator')
    @patch('daft.sql')
    def test_auto_routing_to_daft(self, mock_sql, mock_from_scan, mock_get_table):
        """Test that large queries automatically route to Daft."""
        # Setup large table mock
        mock_table_def = Mock(spec=TableDefinition)
        mock_table_def.table.namespace = "big_data"
        mock_table_def.table.table_name = "huge_table"
        mock_table_def.table_version = Mock()
        mock_get_table.return_value = mock_table_def
        
        mock_df = Mock()
        mock_from_scan.return_value = mock_df
        
        mock_result_df = Mock()
        mock_result_df.to_arrow.return_value = pa.table({"count": [1000000]})
        mock_sql.return_value = mock_result_df
        
        # Execute with AUTO engine
        engine = UnifiedComputeEngine(catalog_name="test_catalog")
        
        # Patch the router to simulate large data detection
        with patch.object(engine.router, '_estimate_data_size', return_value=200_000_000_000):
            result = engine.execute(
                "SELECT COUNT(*) FROM huge_table",
                engine="auto"
            )
        
        # Verify Daft was used
        assert mock_sql.called
        assert isinstance(result, pa.Table)
    
    @patch('ray.is_initialized')
    @patch('ray.get')
    @patch('deltacat.compute.engine.get_table')
    def test_auto_routing_to_ray_for_ml(self, mock_get_table, mock_ray_get,
                                        mock_ray_initialized):
        """Test that ML queries automatically route to Ray."""
        # Setup
        mock_ray_initialized.return_value = True
        
        mock_table_def = Mock(spec=TableDefinition)
        mock_table_def.table.namespace = "ml"
        mock_table_def.table.table_name = "features"
        mock_table_def.table_version = Mock()
        
        mock_scan_plan = Mock()
        mock_scan_plan.scan_tasks = []
        mock_table_def.create_scan_plan.return_value = mock_scan_plan
        
        mock_get_table.return_value = mock_table_def
        
        expected_result = pa.table({"embedding": [[0.1, 0.2], [0.3, 0.4]]})
        mock_ray_get.return_value = pa.serialize(expected_result).to_buffer().to_pybytes()
        
        # Execute with AUTO engine and ML operation
        engine = UnifiedComputeEngine(catalog_name="test_catalog")
        result = engine.execute(
            "SELECT *, EMBEDDING(text) as embedding FROM features",
            engine="auto"
        )
        
        # Verify Ray was used
        assert mock_ray_get.called
        assert isinstance(result, pa.Table)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])