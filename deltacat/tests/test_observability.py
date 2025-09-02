"""
Test suite for error handling and observability features in DeltaCAT.
This follows TDD approach - tests are written before implementation.
"""

import json
import logging
import os
import threading
import time
import uuid
from typing import Any, Dict, List, Optional
from unittest.mock import Mock, patch, MagicMock

import pytest
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from deltacat.logs import configure_deltacat_logger, JsonFormatter
from deltacat.observability import (
    get_correlation_id,
    set_correlation_id,
    CorrelationContextFilter,
    StructuredLogger,
    instrument_method,
    setup_opentelemetry,
    HealthCheckManager,
    HealthCheckStatus,
    ComponentHealth
)
from deltacat.exceptions import (
    DeltaCATException,
    CatalogOperationException,
    TableNotFoundException,
    InvalidOperationException,
    StorageException
)


class TestSpecificExceptionHandling:
    """Test that all bare except clauses are replaced with specific exceptions."""
    
    def test_catalog_daft_namespace_exists_handles_specific_exceptions(self):
        """Test that _has_namespace in daft catalog handles specific exceptions."""
        from deltacat.catalog.daft.daft_catalog import DaftCatalog
        from unittest.mock import Mock
        
        mock_dc_catalog = Mock()
        catalog = DaftCatalog(catalog=mock_dc_catalog, name="test_catalog")
        with patch('deltacat.catalog.list_tables') as mock_list:
            # Should handle CatalogOperationException
            mock_list.side_effect = CatalogOperationException("Catalog error")
            assert catalog._has_namespace("test_namespace") is False
            
            # Should handle TableNotFoundException
            mock_list.side_effect = TableNotFoundException("Not found")
            assert catalog._has_namespace("test_namespace") is False
            
            # Should handle NamespaceNotFoundError
            mock_list.side_effect = NamespaceNotFoundError("Not found")
            assert catalog._has_namespace("test_namespace") is False
            
            # Should re-raise unexpected exceptions
            mock_list.side_effect = ValueError("Unexpected error")
            with pytest.raises(ValueError):
                catalog._has_namespace("test_namespace")
    
    def test_catalog_daft_table_exists_handles_specific_exceptions(self):
        """Test that _has_table in daft catalog handles specific exceptions."""
        from deltacat.catalog.daft.daft_catalog import DaftCatalog
        from unittest.mock import Mock
        
        mock_dc_catalog = Mock()
        catalog = DaftCatalog(catalog=mock_dc_catalog, name="test_catalog")
        with patch.object(catalog, 'get_table') as mock_get:
            # Should handle TableNotFoundException
            mock_get.side_effect = TableNotFoundException("Not found")
            assert catalog._has_table("test_table") is False
            
            # Should handle CatalogOperationException
            mock_get.side_effect = CatalogOperationException("Catalog error")
            assert catalog._has_table("test_table") is False
            
            # Should re-raise unexpected exceptions
            mock_get.side_effect = ValueError("Unexpected error")
            with pytest.raises(ValueError):
                catalog._has_table("test_table")
    
    def test_unity_catalog_namespace_exists_handles_specific_exceptions(self):
        """Test that Unity catalog namespace_exists handles specific exceptions."""
        from deltacat.catalog.unity.impl import namespace_exists
        
        inner = Mock()
        config = Mock(catalog_name="test_catalog")
        
        # Should handle specific Databricks SDK exceptions
        from databricks.sdk.errors import ResourceDoesNotExist, PermissionDenied
        
        inner.workspace.schemas.list.side_effect = ResourceDoesNotExist("Not found")
        assert namespace_exists(inner, config, "test_namespace") is False
        
        inner.workspace.schemas.list.side_effect = PermissionDenied("No access")
        assert namespace_exists(inner, config, "test_namespace") is False
        
        # Should re-raise unexpected exceptions
        inner.workspace.schemas.list.side_effect = ValueError("Unexpected")
        with pytest.raises(ValueError):
            namespace_exists(inner, config, "test_namespace")
    
    def test_unity_catalog_table_exists_handles_specific_exceptions(self):
        """Test that Unity catalog table_exists handles specific exceptions."""
        from deltacat.catalog.unity.impl import table_exists
        
        inner = Mock()
        config = Mock(catalog_name="test_catalog")
        
        # Should handle specific Databricks SDK exceptions
        from databricks.sdk.errors import ResourceDoesNotExist, PermissionDenied
        
        inner.workspace.tables.get.side_effect = ResourceDoesNotExist("Not found")
        assert table_exists(inner, config, "test_namespace", "test_table") is False
        
        inner.workspace.tables.get.side_effect = PermissionDenied("No access")
        assert table_exists(inner, config, "test_namespace", "test_table") is False
        
        # Should re-raise unexpected exceptions
        inner.workspace.tables.get.side_effect = ValueError("Unexpected")
        with pytest.raises(ValueError):
            table_exists(inner, config, "test_namespace", "test_table")
    
    def test_sql_gateway_unregister_handles_specific_exceptions(self):
        """Test that SQL gateway unregister handles specific exceptions."""
        from deltacat.sql.gateway import SQLGateway
        
        gateway = SQLGateway()
        gateway.connection = Mock()
        
        # Should handle specific DuckDB exceptions
        import duckdb
        
        # Should silently handle when table doesn't exist
        gateway.connection.unregister.side_effect = duckdb.CatalogException("Table not found")
        gateway._cleanup_table("test_table")  # Should not raise
        
        # Should re-raise unexpected exceptions
        gateway.connection.unregister.side_effect = ValueError("Unexpected")
        with pytest.raises(ValueError):
            gateway._cleanup_table("test_table")
    
    def test_iceberg_format_init_handles_specific_exceptions(self):
        """Test that Iceberg format initialization handles specific exceptions."""
        from deltacat.storage.formats.iceberg import IcebergTableFormat
        
        format_obj = IcebergTableFormat()
        
        with patch('deltacat.storage.formats.iceberg.load_catalog') as mock_load:
            # Should handle specific pyiceberg exceptions
            from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
            
            mock_load.side_effect = NoSuchNamespaceError("Namespace not found")
            format_obj._ensure_initialized()  # Should handle gracefully
            
            mock_load.side_effect = NoSuchTableError("Table not found")
            format_obj._ensure_initialized()  # Should handle gracefully
            
            # Should re-raise unexpected exceptions
            mock_load.side_effect = ValueError("Unexpected")
            with pytest.raises(ValueError):
                format_obj._ensure_initialized()


class TestStructuredLogging:
    """Test structured logging with correlation IDs."""
    
    def test_correlation_id_generation_and_retrieval(self):
        """Test that correlation IDs can be generated and retrieved."""
        # Test generation
        corr_id = set_correlation_id()
        assert corr_id is not None
        assert isinstance(corr_id, str)
        assert len(corr_id) == 36  # Standard UUID length with hyphens
        
        # Test retrieval
        retrieved_id = get_correlation_id()
        assert retrieved_id == corr_id
        
        # Test custom correlation ID
        custom_id = "custom-correlation-123"
        set_correlation_id(custom_id)
        assert get_correlation_id() == custom_id
    
    def test_correlation_id_thread_safety(self):
        """Test that correlation IDs are thread-local."""
        results = {}
        
        def set_and_get_id(thread_id: int):
            corr_id = f"thread-{thread_id}-{uuid.uuid4()}"
            set_correlation_id(corr_id)
            time.sleep(0.01)  # Simulate some work
            results[thread_id] = get_correlation_id()
        
        threads = []
        for i in range(5):
            t = threading.Thread(target=set_and_get_id, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Each thread should have its own correlation ID
        assert len(results) == 5
        assert len(set(results.values())) == 5  # All unique
        for thread_id, corr_id in results.items():
            assert f"thread-{thread_id}" in corr_id
    
    def test_correlation_context_filter(self):
        """Test that CorrelationContextFilter adds correlation ID to log records."""
        filter_obj = CorrelationContextFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="Test message", args=(), exc_info=None
        )
        
        # Set correlation ID
        corr_id = set_correlation_id("test-correlation-123")
        
        # Apply filter
        filter_obj.filter(record)
        
        # Check that correlation ID was added
        assert hasattr(record, 'correlation_id')
        assert record.correlation_id == corr_id
    
    def test_structured_logger_with_correlation_id(self):
        """Test StructuredLogger includes correlation ID in logs."""
        logger = StructuredLogger("test_logger")
        
        # Set correlation ID
        corr_id = set_correlation_id("test-corr-456")
        
        # Create a mock handler to capture log output
        handler = Mock()
        logger.logger.addHandler(handler)
        
        # Log a message
        logger.info("Test message", extra_field="value")
        
        # Verify handler was called
        assert handler.handle.called
        record = handler.handle.call_args[0][0]
        
        # Check correlation ID in record
        assert hasattr(record, 'correlation_id')
        assert record.correlation_id == corr_id
        
        # Check extra fields
        assert record.extra_field == "value"
    
    def test_json_formatter_with_correlation_id(self):
        """Test JsonFormatter includes correlation ID in output."""
        formatter = JsonFormatter(
            fmt_dict={
                "level": "levelname",
                "message": "message",
                "correlation_id": "correlation_id"
            }
        )
        
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="Test message", args=(), exc_info=None
        )
        record.correlation_id = "test-corr-789"
        
        output = formatter.format(record)
        parsed = json.loads(output)
        
        assert parsed["correlation_id"] == "test-corr-789"
        assert parsed["message"] == "Test message"
        assert parsed["level"] == "INFO"


class TestOpenTelemetryInstrumentation:
    """Test OpenTelemetry instrumentation for distributed tracing."""
    
    @pytest.fixture
    def setup_tracing(self):
        """Set up in-memory tracing for tests."""
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        processor = SimpleSpanProcessor(exporter)
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        
        yield exporter
        
        # Clean up
        exporter.clear()
    
    def test_setup_opentelemetry_configures_tracing(self, setup_tracing):
        """Test that OpenTelemetry setup configures tracing correctly."""
        exporter = setup_tracing
        
        # Setup OpenTelemetry with test configuration
        setup_opentelemetry(
            service_name="deltacat-test",
            environment="test",
            export_endpoint="http://localhost:4317"
        )
        
        # Create a test span
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("test_operation") as span:
            span.set_attribute("test.attribute", "value")
        
        # Check exported spans
        spans = exporter.get_finished_spans()
        assert len(spans) > 0
        
        test_span = next((s for s in spans if s.name == "test_operation"), None)
        assert test_span is not None
        assert test_span.attributes.get("test.attribute") == "value"
    
    def test_instrument_method_decorator(self, setup_tracing):
        """Test that instrument_method decorator creates spans."""
        exporter = setup_tracing
        
        class TestService:
            @instrument_method(span_name="custom_operation")
            def process_data(self, data: str) -> str:
                return f"processed: {data}"
            
            @instrument_method()
            def another_method(self):
                return "result"
        
        service = TestService()
        
        # Call instrumented methods
        result1 = service.process_data("test_data")
        result2 = service.another_method()
        
        assert result1 == "processed: test_data"
        assert result2 == "result"
        
        # Check spans were created
        spans = exporter.get_finished_spans()
        span_names = [s.name for s in spans]
        
        assert "custom_operation" in span_names
        assert "TestService.another_method" in span_names
    
    def test_span_correlation_with_logs(self, setup_tracing):
        """Test that spans are correlated with logs via trace/span IDs."""
        exporter = setup_tracing
        
        @instrument_method(span_name="test_operation")
        def operation_with_logging():
            logger = StructuredLogger("test")
            
            # Get current span
            span = trace.get_current_span()
            span_context = span.get_span_context()
            
            # Log with span context
            logger.info(
                "Operation in progress",
                trace_id=format(span_context.trace_id, '032x'),
                span_id=format(span_context.span_id, '016x')
            )
            
            return span_context
        
        span_context = operation_with_logging()
        
        # Verify span was created
        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].name == "test_operation"
        assert spans[0].context.trace_id == span_context.trace_id
    
    def test_span_error_handling(self, setup_tracing):
        """Test that spans properly record errors."""
        exporter = setup_tracing
        
        @instrument_method(span_name="failing_operation")
        def failing_operation():
            raise CatalogOperationException("Test error")
        
        with pytest.raises(CatalogOperationException):
            failing_operation()
        
        # Check span recorded the error
        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        
        error_span = spans[0]
        assert error_span.name == "failing_operation"
        assert error_span.status.status_code == StatusCode.ERROR
        assert "CatalogOperationException" in error_span.status.description
        
        # Check exception attributes
        events = error_span.events
        assert len(events) > 0
        exception_event = events[0]
        assert exception_event.name == "exception"
        assert exception_event.attributes.get("exception.type") == "CatalogOperationException"


class TestHealthCheckEndpoints:
    """Test health check endpoints for service monitoring."""
    
    def test_health_check_manager_initialization(self):
        """Test HealthCheckManager initialization."""
        manager = HealthCheckManager(service_name="deltacat")
        
        assert manager.service_name == "deltacat"
        assert manager.get_status() == HealthCheckStatus.HEALTHY
        assert len(manager.get_components()) == 0
    
    def test_component_registration(self):
        """Test registering components for health checks."""
        manager = HealthCheckManager(service_name="deltacat")
        
        # Register components
        manager.register_component("database", check_fn=lambda: True)
        manager.register_component("cache", check_fn=lambda: True)
        manager.register_component("storage", check_fn=lambda: False)
        
        components = manager.get_components()
        assert len(components) == 3
        assert "database" in components
        assert "cache" in components
        assert "storage" in components
    
    def test_health_check_execution(self):
        """Test executing health checks."""
        manager = HealthCheckManager(service_name="deltacat")
        
        # Mock check functions
        db_check = Mock(return_value=True)
        cache_check = Mock(return_value=True)
        storage_check = Mock(return_value=False)
        
        manager.register_component("database", check_fn=db_check)
        manager.register_component("cache", check_fn=cache_check)
        manager.register_component("storage", check_fn=storage_check)
        
        # Execute health check
        result = manager.check_health()
        
        assert result["status"] == HealthCheckStatus.DEGRADED
        assert result["service"] == "deltacat"
        assert "components" in result
        
        # Check individual component results
        components = result["components"]
        assert components["database"]["healthy"] is True
        assert components["cache"]["healthy"] is True
        assert components["storage"]["healthy"] is False
        
        # Verify all checks were called
        db_check.assert_called_once()
        cache_check.assert_called_once()
        storage_check.assert_called_once()
    
    def test_health_check_with_metadata(self):
        """Test health checks with metadata."""
        manager = HealthCheckManager(service_name="deltacat")
        
        def db_check():
            return ComponentHealth(
                healthy=True,
                message="Database connection OK",
                metadata={"connections": 10, "latency_ms": 5}
            )
        
        manager.register_component("database", check_fn=db_check)
        
        result = manager.check_health()
        
        db_health = result["components"]["database"]
        assert db_health["healthy"] is True
        assert db_health["message"] == "Database connection OK"
        assert db_health["metadata"]["connections"] == 10
        assert db_health["metadata"]["latency_ms"] == 5
    
    def test_health_check_timeout(self):
        """Test health check with timeout."""
        manager = HealthCheckManager(service_name="deltacat", check_timeout=1.0)
        
        def slow_check():
            time.sleep(2.0)
            return True
        
        manager.register_component("slow_service", check_fn=slow_check)
        
        result = manager.check_health()
        
        assert result["status"] == HealthCheckStatus.UNHEALTHY
        slow_health = result["components"]["slow_service"]
        assert slow_health["healthy"] is False
        assert "timeout" in slow_health["message"].lower()
    
    def test_health_check_exception_handling(self):
        """Test health check handles exceptions in check functions."""
        manager = HealthCheckManager(service_name="deltacat")
        
        def failing_check():
            raise ValueError("Check failed")
        
        manager.register_component("failing_service", check_fn=failing_check)
        
        result = manager.check_health()
        
        assert result["status"] == HealthCheckStatus.UNHEALTHY
        failing_health = result["components"]["failing_service"]
        assert failing_health["healthy"] is False
        assert "ValueError" in failing_health["message"]
    
    def test_liveness_probe(self):
        """Test liveness probe endpoint."""
        manager = HealthCheckManager(service_name="deltacat")
        
        # Liveness should always return healthy if service is running
        liveness = manager.liveness_probe()
        
        assert liveness["status"] == "alive"
        assert liveness["service"] == "deltacat"
        assert "timestamp" in liveness
    
    def test_readiness_probe(self):
        """Test readiness probe endpoint."""
        manager = HealthCheckManager(service_name="deltacat")
        
        # Register a failing component
        manager.register_component("critical_service", check_fn=lambda: False)
        
        # Readiness should reflect component health
        readiness = manager.readiness_probe()
        
        assert readiness["ready"] is False
        assert readiness["service"] == "deltacat"
        assert "reason" in readiness
    
    def test_startup_probe(self):
        """Test startup probe endpoint."""
        manager = HealthCheckManager(service_name="deltacat")
        
        # Mock startup checks
        manager.register_startup_check("migrations", lambda: True)
        manager.register_startup_check("cache_warm", lambda: False)
        
        startup = manager.startup_probe()
        
        assert startup["started"] is False
        assert "checks" in startup
        assert startup["checks"]["migrations"] is True
        assert startup["checks"]["cache_warm"] is False


class TestIntegration:
    """Integration tests for all observability features working together."""
    
    def test_end_to_end_request_tracing(self, setup_tracing):
        """Test end-to-end request with correlation ID and tracing."""
        exporter = setup_tracing
        
        # Simulate an incoming request
        request_id = set_correlation_id()
        
        @instrument_method(span_name="handle_request")
        def handle_request():
            logger = StructuredLogger("handler")
            logger.info("Processing request", request_id=request_id)
            
            # Simulate calling a service
            result = process_data("test_data")
            
            logger.info("Request completed", result=result)
            return result
        
        @instrument_method(span_name="process_data")
        def process_data(data: str):
            logger = StructuredLogger("processor")
            logger.info("Processing data", data=data)
            
            # Simulate some processing
            time.sleep(0.01)
            
            return f"processed_{data}"
        
        # Execute request
        result = handle_request()
        
        assert result == "processed_test_data"
        assert get_correlation_id() == request_id
        
        # Verify spans were created with proper hierarchy
        spans = exporter.get_finished_spans()
        assert len(spans) == 2
        
        process_span = next(s for s in spans if s.name == "process_data")
        handle_span = next(s for s in spans if s.name == "handle_request")
        
        # Verify parent-child relationship
        assert process_span.parent.span_id == handle_span.context.span_id
    
    def test_error_propagation_with_observability(self, setup_tracing):
        """Test error propagation with logging and tracing."""
        exporter = setup_tracing
        
        @instrument_method(span_name="failing_service")
        def failing_service():
            logger = StructuredLogger("service")
            
            try:
                # Simulate an error
                raise StorageException("Storage unavailable")
            except StorageException as e:
                logger.error(
                    "Service failed",
                    error_type=type(e).__name__,
                    error_message=str(e)
                )
                raise
        
        with pytest.raises(StorageException):
            failing_service()
        
        # Check span recorded the error
        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        
        error_span = spans[0]
        assert error_span.status.status_code == StatusCode.ERROR
        assert "StorageException" in error_span.status.description
    
    def test_health_check_with_tracing(self, setup_tracing):
        """Test health check execution with tracing."""
        exporter = setup_tracing
        manager = HealthCheckManager(service_name="deltacat")
        
        @instrument_method(span_name="check_database")
        def check_database():
            # Simulate database check
            time.sleep(0.01)
            return True
        
        manager.register_component("database", check_fn=check_database)
        
        # Execute health check
        result = manager.check_health()
        
        assert result["status"] == HealthCheckStatus.HEALTHY
        
        # Verify span was created for health check
        spans = exporter.get_finished_spans()
        span_names = [s.name for s in spans]
        assert "check_database" in span_names