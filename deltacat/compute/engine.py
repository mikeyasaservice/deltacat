"""Unified Compute Engine for DeltaCAT - Smart routing between DuckDB, Daft, and Ray.

This module provides intelligent query routing to optimize performance and cost
by automatically selecting the best compute engine based on query characteristics.
"""

import logging
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union
import pyarrow as pa

from deltacat import logs
from deltacat.catalog import get_table, list_tables
from deltacat.storage import Dataset

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class ComputeEngineType(Enum):
    """Available compute engines."""
    DUCKDB = "duckdb"  # In-process, single-node SQL
    DAFT = "daft"      # Distributed dataframe compute
    RAY = "ray"        # General distributed compute
    AUTO = "auto"      # Let the router decide


class QueryProfile:
    """Profile of a query to help with routing decisions."""
    
    def __init__(self):
        self.estimated_data_size_bytes: int = 0
        self.table_count: int = 0
        self.has_joins: bool = False
        self.has_complex_aggregations: bool = False
        self.has_python_udf: bool = False
        self.has_ml_operations: bool = False
        self.has_window_functions: bool = False
        self.is_interactive: bool = True
        self.tables: List[str] = []
        
    def __repr__(self) -> str:
        return (
            f"QueryProfile(size={self.estimated_data_size_bytes:,} bytes, "
            f"tables={self.table_count}, joins={self.has_joins}, "
            f"ml={self.has_ml_operations})"
        )


class EngineRouter:
    """Smart router that selects the optimal engine for a query."""
    
    # Thresholds for engine selection (configurable)
    SMALL_DATA_THRESHOLD = 1_000_000_000      # 1 GB - use DuckDB
    MEDIUM_DATA_THRESHOLD = 100_000_000_000   # 100 GB - consider Daft
    
    def __init__(self, catalog_name: Optional[str] = None):
        """Initialize the router with catalog access.
        
        Args:
            catalog_name: Name of the catalog to use for metadata
        """
        self.catalog_name = catalog_name
        
    def choose_engine(
        self,
        query: Optional[str] = None,
        profile: Optional[QueryProfile] = None,
        hints: Optional[Dict[str, Any]] = None,
    ) -> ComputeEngineType:
        """Choose the optimal engine based on query characteristics.
        
        Args:
            query: SQL query string (optional if profile provided)
            profile: Pre-computed query profile
            hints: User hints to influence engine selection
            
        Returns:
            Selected compute engine type
        """
        # 1. Check user hints first - user knows best
        if hints:
            if hints.get("engine"):
                engine = hints["engine"].lower()
                if engine == "duckdb":
                    return ComputeEngineType.DUCKDB
                elif engine == "daft":
                    return ComputeEngineType.DAFT
                elif engine == "ray":
                    return ComputeEngineType.RAY
            
            # Specific hint patterns
            if hints.get("distributed", False):
                logger.info("Hint: distributed=True, using Daft")
                return ComputeEngineType.DAFT
            
            if hints.get("ml_workload", False):
                logger.info("Hint: ml_workload=True, using Ray")
                return ComputeEngineType.RAY
        
        # 2. Analyze query profile
        if not profile and query:
            profile = self._analyze_query(query)
        
        if not profile:
            logger.warning("No profile available, defaulting to DuckDB")
            return ComputeEngineType.DUCKDB
        
        # 3. Apply routing rules
        return self._apply_routing_rules(profile)
    
    def _analyze_query(self, query: str) -> QueryProfile:
        """Analyze a SQL query to build its profile.
        
        Args:
            query: SQL query string
            
        Returns:
            Query profile with estimated characteristics
        """
        profile = QueryProfile()
        
        # Simple heuristics for now - can enhance with sqlglot later
        query_lower = query.lower()
        
        # Detect operations
        profile.has_joins = " join " in query_lower
        profile.has_complex_aggregations = any(
            func in query_lower 
            for func in ["stddev", "variance", "percentile", "median"]
        )
        profile.has_window_functions = " over " in query_lower
        profile.has_python_udf = "udf" in query_lower or "predict" in query_lower
        profile.has_ml_operations = any(
            ml_op in query_lower
            for ml_op in ["predict", "score", "embedding", "vectorize"]
        )
        
        # Extract table references (simple pattern matching)
        # TODO: Use sqlglot for proper parsing
        import re
        table_pattern = r'(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)'
        matches = re.findall(table_pattern, query, re.IGNORECASE)
        profile.tables = list(set(matches))
        profile.table_count = len(profile.tables)
        
        # Estimate data size if we have catalog access
        if self.catalog_name and profile.tables:
            profile.estimated_data_size_bytes = self._estimate_data_size(
                profile.tables
            )
        
        logger.debug(f"Query profile: {profile}")
        return profile
    
    def _estimate_data_size(self, tables: List[str]) -> int:
        """Estimate total data size for tables.
        
        Args:
            tables: List of table names
            
        Returns:
            Estimated size in bytes
        """
        total_size = 0
        
        for table_ref in tables:
            try:
                # Handle namespace.table format
                parts = table_ref.split(".")
                if len(parts) == 2:
                    namespace, table_name = parts
                else:
                    namespace = None
                    table_name = table_ref
                
                # Get table metadata
                table_def = get_table(
                    name=table_name,
                    namespace=namespace,
                    catalog=self.catalog_name
                )
                
                if table_def:
                    # Estimate based on table properties if available
                    # TODO: Get actual statistics from table metadata
                    # For now, use a default estimate
                    total_size += 10_000_000_000  # 10GB default per table
                    
            except Exception as e:
                logger.debug(f"Could not estimate size for table {table_ref}: {e}")
                # Use a conservative estimate
                total_size += 10_000_000_000
        
        return total_size
    
    def _apply_routing_rules(self, profile: QueryProfile) -> ComputeEngineType:
        """Apply routing rules based on query profile.
        
        Args:
            profile: Query profile
            
        Returns:
            Selected compute engine
        """
        # Rule 1: ML/Python UDF workloads → Ray
        if profile.has_ml_operations or profile.has_python_udf:
            logger.info(
                f"Routing to Ray: ML operations={profile.has_ml_operations}, "
                f"Python UDF={profile.has_python_udf}"
            )
            return ComputeEngineType.RAY
        
        # Rule 2: Small data → DuckDB (fastest for small queries)
        if profile.estimated_data_size_bytes < self.SMALL_DATA_THRESHOLD:
            logger.info(
                f"Routing to DuckDB: data size "
                f"({profile.estimated_data_size_bytes:,} bytes) "
                f"< threshold ({self.SMALL_DATA_THRESHOLD:,} bytes)"
            )
            return ComputeEngineType.DUCKDB
        
        # Rule 3: Medium data with simple operations → DuckDB
        if profile.estimated_data_size_bytes < self.MEDIUM_DATA_THRESHOLD:
            if not profile.has_complex_aggregations and profile.table_count <= 2:
                logger.info(
                    "Routing to DuckDB: medium data with simple operations"
                )
                return ComputeEngineType.DUCKDB
        
        # Rule 4: Large data or complex operations → Daft
        logger.info(
            f"Routing to Daft: data size "
            f"({profile.estimated_data_size_bytes:,} bytes) "
            f"or complex operations"
        )
        return ComputeEngineType.DAFT


class UnifiedComputeEngine:
    """Unified compute engine that routes queries to optimal backend.
    
    This is the main user-facing API for executing queries across different
    compute engines with automatic optimization.
    """
    
    def __init__(
        self,
        catalog_name: Optional[str] = None,
        default_engine: ComputeEngineType = ComputeEngineType.AUTO,
    ):
        """Initialize the unified compute engine.
        
        Args:
            catalog_name: Catalog to use for metadata
            default_engine: Default engine type when auto-routing fails
        """
        self.catalog_name = catalog_name
        self.default_engine = default_engine
        self.router = EngineRouter(catalog_name)
        
        # Lazy initialization of engines
        self._duckdb_engine = None
        self._daft_engine = None
        self._ray_engine = None
        
        logger.info(
            f"Initialized UnifiedComputeEngine with catalog={catalog_name}, "
            f"default_engine={default_engine.value}"
        )
    
    def execute(
        self,
        query: str,
        engine: Union[str, ComputeEngineType] = ComputeEngineType.AUTO,
        hints: Optional[Dict[str, Any]] = None,
    ) -> pa.Table:
        """Execute a query using the optimal or specified engine.
        
        Args:
            query: SQL query to execute
            engine: Specific engine to use, or AUTO for smart routing
            hints: Optional hints to influence engine selection
            
        Returns:
            Query results as an Arrow Table
        """
        # Convert string to enum if needed
        if isinstance(engine, str):
            engine = ComputeEngineType(engine.lower())
        
        # Choose engine if auto
        if engine == ComputeEngineType.AUTO:
            engine = self.router.choose_engine(query=query, hints=hints)
            logger.info(f"Auto-selected engine: {engine.value}")
        else:
            logger.info(f"Using specified engine: {engine.value}")
        
        # Route to appropriate engine
        if engine == ComputeEngineType.DUCKDB:
            return self._execute_duckdb(query)
        elif engine == ComputeEngineType.DAFT:
            return self._execute_daft(query)
        elif engine == ComputeEngineType.RAY:
            return self._execute_ray(query)
        else:
            raise ValueError(f"Unknown engine type: {engine}")
    
    def _execute_duckdb(self, query: str) -> pa.Table:
        """Execute query using DuckDB.
        
        Args:
            query: SQL query
            
        Returns:
            Query results as Arrow Table
        """
        if not self._duckdb_engine:
            from deltacat.sql.gateway import DeltaCATSQLGateway
            self._duckdb_engine = DeltaCATSQLGateway(
                catalog_name=self.catalog_name,
                auto_register_tables=True
            )
        
        logger.debug(f"Executing on DuckDB: {query[:100]}...")
        return self._duckdb_engine.sql(query)
    
    def _execute_daft(self, query: str) -> pa.Table:
        """Execute query using Daft.
        
        Args:
            query: SQL query
            
        Returns:
            Query results as Arrow Table
        """
        # Import here to avoid circular dependencies
        import daft
        from deltacat.utils.daft import deltacat_table_to_daft_dataframe
        
        logger.debug(f"Executing on Daft: {query[:100]}...")
        
        # Parse tables from query and register with Daft
        profile = self.router._analyze_query(query)
        
        for table_ref in profile.tables:
            # Get table from catalog
            parts = table_ref.split(".")
            namespace = parts[0] if len(parts) == 2 else None
            table_name = parts[-1]
            
            table_def = get_table(
                name=table_name,
                namespace=namespace,
                catalog=self.catalog_name
            )
            
            if table_def:
                # Convert to Daft DataFrame
                # TODO: Implement proper conversion
                logger.info(f"Registering table {table_ref} with Daft")
        
        # Execute SQL with Daft
        result = daft.sql(query)
        return result.to_arrow()
    
    def _execute_ray(self, query: str) -> pa.Table:
        """Execute query using Ray.
        
        Args:
            query: SQL query
            
        Returns:
            Query results as Arrow Table
        """
        import ray
        
        logger.debug(f"Executing on Ray: {query[:100]}...")
        
        @ray.remote
        def distributed_query():
            # TODO: Implement Ray-based SQL execution
            # For now, delegate to Daft within Ray
            return self._execute_daft(query)
        
        return ray.get(distributed_query.remote())
    
    def explain(
        self,
        query: str,
        engine: Union[str, ComputeEngineType] = ComputeEngineType.AUTO,
    ) -> Dict[str, Any]:
        """Explain query execution plan and engine selection.
        
        Args:
            query: SQL query to explain
            engine: Engine to use (or AUTO)
            
        Returns:
            Explanation dictionary with engine choice and reasoning
        """
        if isinstance(engine, str):
            engine = ComputeEngineType(engine.lower())
        
        profile = self.router._analyze_query(query)
        
        if engine == ComputeEngineType.AUTO:
            selected_engine = self.router.choose_engine(query=query)
        else:
            selected_engine = engine
        
        explanation = {
            "selected_engine": selected_engine.value,
            "query_profile": {
                "estimated_data_size_gb": profile.estimated_data_size_bytes / 1e9,
                "table_count": profile.table_count,
                "has_joins": profile.has_joins,
                "has_ml_operations": profile.has_ml_operations,
                "has_python_udf": profile.has_python_udf,
                "tables": profile.tables,
            },
            "routing_reason": self._get_routing_reason(profile, selected_engine),
            "alternative_engines": self._get_alternatives(profile),
        }
        
        return explanation
    
    def _get_routing_reason(
        self,
        profile: QueryProfile,
        engine: ComputeEngineType
    ) -> str:
        """Get human-readable reason for engine selection.
        
        Args:
            profile: Query profile
            engine: Selected engine
            
        Returns:
            Explanation string
        """
        if engine == ComputeEngineType.RAY:
            return "Query contains ML operations or Python UDFs requiring Ray"
        elif engine == ComputeEngineType.DUCKDB:
            if profile.estimated_data_size_bytes < EngineRouter.SMALL_DATA_THRESHOLD:
                return f"Small data size ({profile.estimated_data_size_bytes / 1e9:.2f} GB) - DuckDB is fastest"
            else:
                return "Medium data size with simple operations - DuckDB can handle efficiently"
        elif engine == ComputeEngineType.DAFT:
            return f"Large data size ({profile.estimated_data_size_bytes / 1e9:.2f} GB) or complex operations requiring distribution"
        return "Unknown routing reason"
    
    def _get_alternatives(
        self,
        profile: QueryProfile
    ) -> List[Dict[str, str]]:
        """Get alternative engine options.
        
        Args:
            profile: Query profile
            
        Returns:
            List of alternative engines with trade-offs
        """
        alternatives = []
        
        if profile.estimated_data_size_bytes < EngineRouter.MEDIUM_DATA_THRESHOLD:
            alternatives.append({
                "engine": "duckdb",
                "pros": "Fastest for small-medium data, lowest cost",
                "cons": "Single-node memory limits"
            })
        
        alternatives.append({
            "engine": "daft",
            "pros": "Scales to any data size, handles complex operations",
            "cons": "Higher overhead for small queries"
        })
        
        if profile.has_python_udf or profile.has_ml_operations:
            alternatives.append({
                "engine": "ray",
                "pros": "Best for ML workloads, Python UDF support",
                "cons": "Highest overhead, requires Ray cluster"
            })
        
        return alternatives


# Convenience function for quick access
def create_engine(
    catalog: Optional[str] = None,
    default_engine: str = "auto"
) -> UnifiedComputeEngine:
    """Create a unified compute engine instance.
    
    Args:
        catalog: Catalog name to use
        default_engine: Default engine when auto-routing fails
        
    Returns:
        Configured UnifiedComputeEngine instance
    """
    return UnifiedComputeEngine(
        catalog_name=catalog,
        default_engine=ComputeEngineType(default_engine.lower())
    )