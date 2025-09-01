"""Resource cleanup fixtures for tests to prevent hanging."""
import pytest
import ray
import gc
import daft

@pytest.fixture(autouse=True, scope="function")
def cleanup_resources():
    """Cleanup resources after each test to prevent hanging."""
    yield
    
    # Cleanup Ray if initialized
    if ray.is_initialized():
        ray.shutdown()
    
    # Force garbage collection
    gc.collect()
    
    # Clear Daft session if exists
    try:
        daft.context.get_context().runner.shutdown()
    except:
        pass

@pytest.fixture(autouse=True, scope="session")
def configure_test_resources():
    """Configure resource limits for testing."""
    import os
    
    # Limit Ray resources for testing
    os.environ["RAY_memory_monitor_refresh_ms"] = "0"  # Disable memory monitor
    os.environ["RAY_memory_usage_threshold"] = "0.95"  # Set high threshold
    os.environ["RAY_object_store_memory"] = "500000000"  # Limit object store to 500MB
    
    yield
    
    # Final cleanup
    if ray.is_initialized():
        ray.shutdown()