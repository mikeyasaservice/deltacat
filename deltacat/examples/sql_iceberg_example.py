"""Example: SQL queries on DeltaCAT Iceberg tables with optimized DuckDB integration."""

import deltacat as dc
from deltacat.sql import DeltaCATSQLGateway
from deltacat.experimental.catalog.iceberg import IcebergCatalogConfig

# Example 1: Basic setup with Iceberg catalog
def setup_iceberg_sql():
    """Setup DeltaCAT with Iceberg catalog and SQL gateway."""
    
    # Initialize DeltaCAT
    dc.init()
    
    # Configure Iceberg catalog (example with AWS Glue)
    iceberg_config = IcebergCatalogConfig(
        type="glue",
        properties={
            "region": "us-west-2",
            "database": "my_lakehouse",
            "warehouse": "s3://my-bucket/warehouse"
        }
    )
    
    # Register the Iceberg catalog
    from deltacat.experimental.catalog.iceberg import IcebergCatalog
    catalog = IcebergCatalog.from_config(iceberg_config)
    dc.put_catalog("iceberg", catalog)
    
    # Create SQL gateway with Iceberg optimization
    sql_gateway = DeltaCATSQLGateway(
        catalog_name="iceberg",
        enable_iceberg_optimization=True  # Enable native Iceberg support
    )
    
    return sql_gateway


# Example 2: Query Iceberg tables with native support
def query_iceberg_tables():
    """Demonstrate SQL queries on Iceberg tables."""
    
    sql = setup_iceberg_sql()
    
    # Simple query - DuckDB uses iceberg_scan() internally
    result = sql.sql("""
        SELECT 
            customer_id,
            SUM(amount) as total_spent,
            COUNT(*) as order_count
        FROM sales_orders
        GROUP BY customer_id
        ORDER BY total_spent DESC
        LIMIT 10
    """)
    
    print("Top 10 customers by spending:")
    print(result.to_pandas())
    
    # Join across Iceberg tables
    result = sql.sql("""
        SELECT 
            c.customer_name,
            c.region,
            o.product_category,
            SUM(o.amount) as category_spend
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        WHERE o.order_date >= '2024-01-01'
        GROUP BY c.customer_name, c.region, o.product_category
    """)
    
    print("\nCustomer spending by category:")
    print(result.to_pandas())


# Example 3: Time travel queries on Iceberg tables
def time_travel_queries():
    """Demonstrate Iceberg time travel via SQL."""
    
    sql = setup_iceberg_sql()
    
    # Get table snapshots
    if sql.iceberg_adapter:
        snapshots = sql.iceberg_adapter.get_table_snapshots("sales_orders")
        print("Available snapshots:")
        print(snapshots.to_pandas())
        
        # Query specific snapshot
        snapshot_id = snapshots['snapshot_id'][0].as_py()
        historical_data = sql.iceberg_adapter.time_travel_query(
            "sales_orders",
            snapshot_id=snapshot_id
        )
        print(f"\nData from snapshot {snapshot_id}:")
        print(historical_data.to_pandas().head())
        
        # Query by timestamp
        historical_data = sql.iceberg_adapter.time_travel_query(
            "sales_orders",
            timestamp="2024-01-01 00:00:00"
        )
        print("\nData as of 2024-01-01:")
        print(historical_data.to_pandas().head())


# Example 4: Mixed catalog types (Iceberg + native DeltaCAT)
def mixed_catalog_queries():
    """Query across Iceberg and native DeltaCAT tables."""
    
    dc.init()
    
    # Register both Iceberg and native catalogs
    # ... setup code ...
    
    # SQL gateway handles both transparently
    sql = DeltaCATSQLGateway(
        enable_iceberg_optimization=True
    )
    
    # Query will use:
    # - iceberg_scan() for Iceberg tables
    # - Arrow Datasets for native DeltaCAT tables
    result = sql.sql("""
        SELECT 
            i.product_id,
            i.inventory_count,  -- From Iceberg table
            d.forecast_demand   -- From DeltaCAT table
        FROM iceberg.inventory i
        JOIN deltacat.demand_forecast d ON i.product_id = d.product_id
        WHERE i.inventory_count < d.forecast_demand * 1.5
    """)
    
    print("Products needing restock:")
    print(result.to_pandas())


# Example 5: Performance comparison
def performance_comparison():
    """Compare native Iceberg vs generic Arrow approach."""
    
    import time
    
    # Setup with Iceberg optimization
    sql_optimized = DeltaCATSQLGateway(
        catalog_name="iceberg",
        enable_iceberg_optimization=True
    )
    
    # Setup without optimization (Arrow Dataset approach)
    sql_generic = DeltaCATSQLGateway(
        catalog_name="iceberg",
        enable_iceberg_optimization=False
    )
    
    query = """
        SELECT 
            year(order_date) as year,
            month(order_date) as month,
            SUM(amount) as revenue
        FROM large_orders_table
        WHERE order_date >= '2020-01-01'
        GROUP BY year, month
        ORDER BY year, month
    """
    
    # Time optimized query
    start = time.time()
    result_optimized = sql_optimized.sql(query)
    time_optimized = time.time() - start
    
    # Time generic query
    start = time.time()
    result_generic = sql_generic.sql(query)
    time_generic = time.time() - start
    
    print(f"Native Iceberg: {time_optimized:.2f}s")
    print(f"Arrow Dataset: {time_generic:.2f}s")
    print(f"Speedup: {time_generic/time_optimized:.1f}x")
    
    # The native approach should be faster because:
    # 1. DuckDB can push filters directly to Iceberg
    # 2. Iceberg metadata enables better partition pruning
    # 3. Column statistics enable better query planning


if __name__ == "__main__":
    # Run examples
    print("=" * 60)
    print("DeltaCAT SQL with Native Iceberg Support")
    print("=" * 60)
    
    # Note: These examples assume you have:
    # 1. An Iceberg catalog configured (Glue, Hive, etc.)
    # 2. Some Iceberg tables created
    # 3. Appropriate AWS/cloud credentials
    
    try:
        query_iceberg_tables()
        time_travel_queries()
        mixed_catalog_queries()
        performance_comparison()
    except Exception as e:
        print(f"Example requires Iceberg catalog setup: {e}")
        print("\nTo set up Iceberg tables:")
        print("1. Install: pip install deltacat[iceberg]")
        print("2. Configure AWS credentials")
        print("3. Create Iceberg tables in Glue or Hive")