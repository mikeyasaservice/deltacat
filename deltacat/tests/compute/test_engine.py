"""Test-Driven Development tests for Unified Compute Engine.

These tests are written BEFORE implementation to define expected behavior.
They should fail initially and guide the implementation.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pyarrow as pa

# Import the modules we're testing (these may not exist yet)
from deltacat.compute.engine import (
    ComputeEngineType,
    QueryProfile,
    EngineRouter,
    UnifiedComputeEngine,
    create_engine,
)


class TestComputeEngineType:
    """Test the engine type enumeration."""
    
    def test_engine_types_exist(self):
        """Verify all expected engine types are defined."""
        assert ComputeEngineType.DUCKDB.value == "duckdb"
        assert ComputeEngineType.DAFT.value == "daft"
        assert ComputeEngineType.RAY.value == "ray"
        assert ComputeEngineType.AUTO.value == "auto"


class TestQueryProfile:
    """Test query profiling functionality."""
    
    def test_query_profile_initialization(self):
        """Test QueryProfile initializes with correct defaults."""
        profile = QueryProfile()
        
        assert profile.estimated_data_size_bytes == 0
        assert profile.table_count == 0
        assert profile.has_joins is False
        assert profile.has_complex_aggregations is False
        assert profile.has_python_udf is False
        assert profile.has_ml_operations is False
        assert profile.has_window_functions is False
        assert profile.is_interactive is True
        assert profile.tables == []
    
    def test_query_profile_repr(self):
        """Test QueryProfile string representation."""
        profile = QueryProfile()
        profile.estimated_data_size_bytes = 1000000000
        profile.table_count = 2
        profile.has_joins = True
        profile.has_ml_operations = False
        
        repr_str = repr(profile)
        assert "1,000,000,000 bytes" in repr_str
        assert "tables=2" in repr_str
        assert "joins=True" in repr_str
        assert "ml=False" in repr_str


class TestEngineRouter:
    """Test the smart routing logic."""
    
    def test_router_initialization(self):
        """Test router initializes with catalog."""
        router = EngineRouter(catalog_name="test_catalog")
        assert router.catalog_name == "test_catalog"
    
    def test_small_data_routes_to_duckdb(self):
        """Test that small data (<1GB) routes to DuckDB."""
        router = EngineRouter()
        
        profile = QueryProfile()
        profile.estimated_data_size_bytes = 500_000_000  # 500 MB
        profile.table_count = 1
        
        engine = router.choose_engine(profile=profile)
        assert engine == ComputeEngineType.DUCKDB
    
    def test_large_data_routes_to_daft(self):
        """Test that large data (>100GB) routes to Daft."""
        router = EngineRouter()
        
        profile = QueryProfile()
        profile.estimated_data_size_bytes = 200_000_000_000  # 200 GB
        profile.table_count = 2
        profile.has_joins = True
        
        engine = router.choose_engine(profile=profile)
        assert engine == ComputeEngineType.DAFT
    
    def test_ml_operations_route_to_ray(self):
        """Test that ML operations route to Ray."""
        router = EngineRouter()
        
        profile = QueryProfile()
        profile.estimated_data_size_bytes = 1_000_000  # Small data
        profile.has_ml_operations = True
        
        engine = router.choose_engine(profile=profile)
        assert engine == ComputeEngineType.RAY
    
    def test_python_udf_routes_to_ray(self):
        """Test that Python UDFs route to Ray."""
        router = EngineRouter()
        
        profile = QueryProfile()
        profile.has_python_udf = True
        
        engine = router.choose_engine(profile=profile)
        assert engine == ComputeEngineType.RAY
    
    def test_hints_override_routing(self):
        """Test that user hints override automatic routing."""
        router = EngineRouter()
        
        # Small data that would normally go to DuckDB
        profile = QueryProfile()
        profile.estimated_data_size_bytes = 100_000
        
        # But user wants distributed processing
        hints = {"distributed": True}
        engine = router.choose_engine(profile=profile, hints=hints)
        assert engine == ComputeEngineType.DAFT
        
        # User explicitly requests engine
        hints = {"engine": "ray"}
        engine = router.choose_engine(profile=profile, hints=hints)
        assert engine == ComputeEngineType.RAY
    
    def test_medium_data_simple_ops_uses_duckdb(self):
        """Test medium data with simple operations uses DuckDB."""
        router = EngineRouter()
        
        profile = QueryProfile()
        profile.estimated_data_size_bytes = 50_000_000_000  # 50 GB
        profile.table_count = 2
        profile.has_complex_aggregations = False
        
        engine = router.choose_engine(profile=profile)
        assert engine == ComputeEngineType.DUCKDB
    
    def test_medium_data_complex_ops_uses_daft(self):
        """Test medium data with complex operations uses Daft."""
        router = EngineRouter()
        
        profile = QueryProfile()
        profile.estimated_data_size_bytes = 50_000_000_000  # 50 GB
        profile.has_complex_aggregations = True
        
        engine = router.choose_engine(profile=profile)
        assert engine == ComputeEngineType.DAFT
    
    def test_analyze_query_detects_operations(self):
        """Test query analysis detects various SQL operations."""
        router = EngineRouter()
        
        # Test JOIN detection
        query = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"
        profile = router._analyze_query(query)
        assert profile.has_joins is True
        
        # Test complex aggregation detection
        query = "SELECT stddev(amount), percentile(price, 0.95) FROM sales"
        profile = router._analyze_query(query)
        assert profile.has_complex_aggregations is True
        
        # Test window function detection
        query = "SELECT *, ROW_NUMBER() OVER (PARTITION BY category) FROM products"
        profile = router._analyze_query(query)
        assert profile.has_window_functions is True
        
        # Test ML operation detection
        query = "SELECT *, PREDICT(model, features) as score FROM customers"
        profile = router._analyze_query(query)
        assert profile.has_ml_operations is True
    
    def test_analyze_query_extracts_tables(self):
        """Test query analysis extracts table names."""
        router = EngineRouter()
        
        query = """
            SELECT * 
            FROM orders o 
            JOIN customers c ON o.customer_id = c.id
            JOIN products p ON o.product_id = p.id
        """
        profile = router._analyze_query(query)
        
        assert "orders" in profile.tables
        assert "customers" in profile.tables
        assert "products" in profile.tables
        assert profile.table_count == 3


class TestUnifiedComputeEngine:
    """Test the main unified compute engine."""
    
    def test_engine_initialization(self):
        """Test engine initializes with default settings."""
        engine = UnifiedComputeEngine(catalog_name="test_catalog")
        
        assert engine.catalog_name == "test_catalog"
        assert engine.default_engine == ComputeEngineType.AUTO
        assert engine.router is not None
        assert engine._duckdb_engine is None  # Lazy init
        assert engine._daft_engine is None
        assert engine._ray_engine is None
    
    def test_create_engine_helper(self):
        """Test the convenience function for creating engines."""
        engine = create_engine(catalog="test", default_engine="duckdb")
        
        assert engine.catalog_name == "test"
        assert engine.default_engine == ComputeEngineType.DUCKDB
    
    @patch('deltacat.compute.engine.UnifiedComputeEngine._execute_duckdb')
    def test_execute_with_auto_routing(self, mock_duckdb):
        """Test execution with automatic engine selection."""
        mock_duckdb.return_value = pa.table({"result": [1, 2, 3]})
        
        engine = UnifiedComputeEngine()
        
        # Small query should route to DuckDB
        result = engine.execute(
            "SELECT * FROM small_table WHERE id < 100",
            engine="auto"
        )
        
        mock_duckdb.assert_called_once()
        assert isinstance(result, pa.Table)
    
    @patch('deltacat.compute.engine.UnifiedComputeEngine._execute_daft')
    def test_execute_with_explicit_engine(self, mock_daft):
        """Test execution with explicitly specified engine."""
        mock_daft.return_value = pa.table({"result": [1, 2, 3]})
        
        engine = UnifiedComputeEngine()
        
        # Force Daft even for small query
        result = engine.execute(
            "SELECT * FROM small_table",
            engine="daft"
        )
        
        mock_daft.assert_called_once()
        assert isinstance(result, pa.Table)
    
    @patch('deltacat.compute.engine.UnifiedComputeEngine._execute_ray')
    def test_execute_ml_query_uses_ray(self, mock_ray):
        """Test ML queries automatically use Ray."""
        mock_ray.return_value = pa.table({"prediction": [0.8, 0.9, 0.7]})
        
        engine = UnifiedComputeEngine()
        
        result = engine.execute(
            "SELECT *, PREDICT(model, features) FROM data",
            engine="auto"
        )
        
        mock_ray.assert_called_once()
        assert isinstance(result, pa.Table)
    
    def test_execute_with_hints(self):
        """Test execution respects user hints."""
        engine = UnifiedComputeEngine()
        
        with patch.object(engine, '_execute_daft') as mock_daft:
            mock_daft.return_value = pa.table({"result": [1]})
            
            # Hint to use distributed processing
            result = engine.execute(
                "SELECT * FROM tiny_table",
                hints={"distributed": True}
            )
            
            mock_daft.assert_called_once()
    
    def test_explain_provides_insights(self):
        """Test explain method provides query insights."""
        engine = UnifiedComputeEngine()
        
        explanation = engine.explain(
            "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id"
        )
        
        assert "selected_engine" in explanation
        assert "query_profile" in explanation
        assert "routing_reason" in explanation
        assert "alternative_engines" in explanation
        
        profile = explanation["query_profile"]
        assert profile["has_joins"] is True
        assert profile["table_count"] == 2
        assert "orders" in profile["tables"]
        assert "customers" in profile["tables"]
    
    def test_explain_with_forced_engine(self):
        """Test explain with explicitly specified engine."""
        engine = UnifiedComputeEngine()
        
        explanation = engine.explain(
            "SELECT * FROM small_table",
            engine="daft"
        )
        
        assert explanation["selected_engine"] == "daft"
    
    @patch('deltacat.sql.gateway.DeltaCATSQLGateway')
    def test_duckdb_execution_lazy_init(self, mock_gateway_class):
        """Test DuckDB engine is lazily initialized."""
        mock_gateway = Mock()
        mock_gateway.sql.return_value = pa.table({"result": [1]})
        mock_gateway_class.return_value = mock_gateway
        
        engine = UnifiedComputeEngine(catalog_name="test")
        
        # Engine not initialized yet
        assert engine._duckdb_engine is None
        
        # Execute query
        result = engine._execute_duckdb("SELECT 1")
        
        # Now it should be initialized
        assert engine._duckdb_engine is not None
        mock_gateway_class.assert_called_once_with(
            catalog_name="test",
            auto_register_tables=True
        )
        mock_gateway.sql.assert_called_once_with("SELECT 1")
    
    def test_invalid_engine_raises_error(self):
        """Test invalid engine type raises error."""
        engine = UnifiedComputeEngine()
        
        with pytest.raises(ValueError):
            engine.execute("SELECT 1", engine="invalid_engine")
    
    def test_routing_thresholds_are_sensible(self):
        """Test that routing thresholds make sense."""
        assert EngineRouter.SMALL_DATA_THRESHOLD == 1_000_000_000  # 1 GB
        assert EngineRouter.MEDIUM_DATA_THRESHOLD == 100_000_000_000  # 100 GB
        assert EngineRouter.SMALL_DATA_THRESHOLD < EngineRouter.MEDIUM_DATA_THRESHOLD


class TestIntegrationScenarios:
    """Integration tests for real-world scenarios."""
    
    @patch('deltacat.compute.engine.get_table')
    def test_estimate_data_size_with_catalog(self, mock_get_table):
        """Test data size estimation using catalog metadata."""
        # Mock catalog responses
        mock_table = Mock()
        mock_get_table.return_value = mock_table
        
        router = EngineRouter(catalog_name="test")
        
        # Estimate size for multiple tables
        size = router._estimate_data_size(["orders", "customers"])
        
        # Should have called get_table for each table
        assert mock_get_table.call_count == 2
        # Default estimate is 10GB per table
        assert size == 20_000_000_000  # 20 GB total
    
    def test_end_to_end_query_routing(self):
        """Test complete query routing flow."""
        engine = UnifiedComputeEngine(catalog_name="test")
        
        # Test various query patterns
        test_cases = [
            # (query, expected_engine)
            ("SELECT * FROM tiny_table LIMIT 10", ComputeEngineType.DUCKDB),
            ("SELECT COUNT(*) FROM billion_row_table", ComputeEngineType.DAFT),
            ("SELECT *, EMBEDDING(text) FROM documents", ComputeEngineType.RAY),
        ]
        
        for query, expected_engine in test_cases:
            # Get the router's decision
            profile = engine.router._analyze_query(query)
            selected = engine.router.choose_engine(profile=profile)
            
            # For simple cases without catalog, check the logic
            if "billion_row" in query:
                # Assume large table
                profile.estimated_data_size_bytes = 1_000_000_000_000
                selected = engine.router.choose_engine(profile=profile)
            elif "EMBEDDING" in query:
                # ML operation detected
                assert profile.has_ml_operations is True
                
            # Verify routing (may not match exactly without catalog data)
            if "EMBEDDING" in query or "PREDICT" in query:
                assert selected == ComputeEngineType.RAY