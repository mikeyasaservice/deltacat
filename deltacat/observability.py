"""
Observability module for DeltaCAT.
Provides structured logging, correlation IDs, OpenTelemetry instrumentation, and health checks.
"""

import functools
import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
# Import instrumentation packages if available
try:
    from opentelemetry.instrumentation.aws_lambda import AwsLambdaInstrumentor
except ImportError:
    AwsLambdaInstrumentor = None

try:
    from opentelemetry.instrumentation.boto3sqs import Boto3SQSInstrumentor
except ImportError:
    Boto3SQSInstrumentor = None

try:
    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
except ImportError:
    BotocoreInstrumentor = None

try:
    from opentelemetry.instrumentation.redis import RedisInstrumentor
except ImportError:
    RedisInstrumentor = None

try:
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
except ImportError:
    RequestsInstrumentor = None
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode

from deltacat.logs import JsonFormatter


# Thread-local storage for correlation IDs
_correlation_id_storage = threading.local()


def get_correlation_id() -> Optional[str]:
    """Get the current correlation ID from thread-local storage."""
    return getattr(_correlation_id_storage, 'correlation_id', None)


def set_correlation_id(correlation_id: Optional[str] = None) -> str:
    """
    Set the correlation ID in thread-local storage.
    
    Args:
        correlation_id: Optional correlation ID to set. If None, generates a new UUID.
    
    Returns:
        The correlation ID that was set.
    """
    if correlation_id is None:
        correlation_id = str(uuid.uuid4())
    _correlation_id_storage.correlation_id = correlation_id
    return correlation_id


def clear_correlation_id():
    """Clear the correlation ID from thread-local storage."""
    if hasattr(_correlation_id_storage, 'correlation_id'):
        delattr(_correlation_id_storage, 'correlation_id')


@contextmanager
def correlation_context(correlation_id: Optional[str] = None):
    """
    Context manager for correlation ID.
    
    Args:
        correlation_id: Optional correlation ID to use. If None, generates a new one.
    
    Yields:
        The correlation ID being used.
    """
    old_id = get_correlation_id()
    new_id = set_correlation_id(correlation_id)
    try:
        yield new_id
    finally:
        if old_id is not None:
            set_correlation_id(old_id)
        else:
            clear_correlation_id()


class CorrelationContextFilter(logging.Filter):
    """Logging filter that adds correlation ID to log records."""
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add correlation ID to the log record."""
        record.correlation_id = get_correlation_id() or "no-correlation-id"
        
        # Also add OpenTelemetry trace context if available
        span = trace.get_current_span()
        if span and span.is_recording():
            span_context = span.get_span_context()
            record.trace_id = format(span_context.trace_id, '032x')
            record.span_id = format(span_context.span_id, '016x')
        else:
            record.trace_id = "no-trace-id"
            record.span_id = "no-span-id"
        
        return True


class StructuredLogger:
    """
    Structured logger that includes correlation IDs and additional context.
    """
    
    def __init__(self, name: str, context: Optional[Dict[str, Any]] = None):
        """
        Initialize a structured logger.
        
        Args:
            name: Logger name.
            context: Optional static context to include in all log messages.
        """
        self.logger = logging.getLogger(name)
        self.context = context or {}
        
        # Add correlation filter if not already present
        if not any(isinstance(f, CorrelationContextFilter) for f in self.logger.filters):
            self.logger.addFilter(CorrelationContextFilter())
    
    def _log(self, level: int, msg: str, **kwargs):
        """Internal method to log with structured data."""
        extra = {**self.context, **kwargs}
        
        # Add correlation ID if not already in kwargs
        if 'correlation_id' not in extra:
            extra['correlation_id'] = get_correlation_id()
        
        # Add OpenTelemetry trace context
        span = trace.get_current_span()
        if span and span.is_recording():
            span_context = span.get_span_context()
            extra['trace_id'] = format(span_context.trace_id, '032x')
            extra['span_id'] = format(span_context.span_id, '016x')
        
        self.logger.log(level, msg, extra=extra)
    
    def debug(self, msg: str, **kwargs):
        """Log a debug message."""
        self._log(logging.DEBUG, msg, **kwargs)
    
    def info(self, msg: str, **kwargs):
        """Log an info message."""
        self._log(logging.INFO, msg, **kwargs)
    
    def warning(self, msg: str, **kwargs):
        """Log a warning message."""
        self._log(logging.WARNING, msg, **kwargs)
    
    def error(self, msg: str, **kwargs):
        """Log an error message."""
        self._log(logging.ERROR, msg, **kwargs)
    
    def critical(self, msg: str, **kwargs):
        """Log a critical message."""
        self._log(logging.CRITICAL, msg, **kwargs)
    
    def exception(self, msg: str, **kwargs):
        """Log an exception with traceback."""
        kwargs['exc_info'] = True
        self._log(logging.ERROR, msg, **kwargs)


def setup_opentelemetry(
    service_name: str,
    environment: str = "production",
    export_endpoint: Optional[str] = None,
    additional_attributes: Optional[Dict[str, Any]] = None
):
    """
    Set up OpenTelemetry instrumentation for the application.
    
    Args:
        service_name: Name of the service.
        environment: Environment name (e.g., "production", "staging").
        export_endpoint: Optional OTLP endpoint for exporting traces.
        additional_attributes: Additional resource attributes.
    """
    # Create resource with service information
    resource_attributes = {
        "service.name": service_name,
        "service.environment": environment,
        "service.version": "1.0.0",  # TODO: Get from package version
    }
    
    if additional_attributes:
        resource_attributes.update(additional_attributes)
    
    resource = Resource.create(resource_attributes)
    
    # Set up tracer provider
    provider = TracerProvider(resource=resource)
    
    # Add OTLP exporter if endpoint is provided
    if export_endpoint:
        otlp_exporter = OTLPSpanExporter(endpoint=export_endpoint, insecure=True)
        span_processor = BatchSpanProcessor(otlp_exporter)
        provider.add_span_processor(span_processor)
    
    # Set global tracer provider
    trace.set_tracer_provider(provider)
    
    # Instrument libraries
    try:
        if BotocoreInstrumentor:
            BotocoreInstrumentor().instrument()
        if Boto3SQSInstrumentor:
            Boto3SQSInstrumentor().instrument()
        if RedisInstrumentor:
            RedisInstrumentor().instrument()
        if RequestsInstrumentor:
            RequestsInstrumentor().instrument()
        
        # Instrument AWS Lambda if running in Lambda environment
        import os
        if os.environ.get("AWS_LAMBDA_FUNCTION_NAME") and AwsLambdaInstrumentor:
            AwsLambdaInstrumentor().instrument()
    except Exception as e:
        logger = StructuredLogger(__name__)
        logger.warning(f"Failed to instrument some libraries: {e}")


def instrument_method(
    span_name: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
    record_exception: bool = True,
    set_status_on_exception: bool = True
):
    """
    Decorator to instrument a method with OpenTelemetry tracing.
    
    Args:
        span_name: Optional custom span name. If None, uses class.method name.
        attributes: Optional attributes to add to the span.
        record_exception: Whether to record exceptions as span events.
        set_status_on_exception: Whether to set span status to ERROR on exception.
    
    Returns:
        Decorated function with tracing.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Determine span name
            actual_span_name = span_name
            if actual_span_name is None:
                # Try to get class name if this is a method
                if args and hasattr(args[0], '__class__'):
                    actual_span_name = f"{args[0].__class__.__name__}.{func.__name__}"
                else:
                    actual_span_name = func.__name__
            
            # Get tracer
            tracer = trace.get_tracer(__name__)
            
            # Start span
            with tracer.start_as_current_span(actual_span_name) as span:
                # Add attributes
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)
                
                # Add correlation ID as attribute
                correlation_id = get_correlation_id()
                if correlation_id:
                    span.set_attribute("correlation.id", correlation_id)
                
                try:
                    # Execute function
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    # Record exception
                    if record_exception:
                        span.record_exception(e)
                    
                    # Set error status
                    if set_status_on_exception:
                        span.set_status(
                            Status(
                                StatusCode.ERROR,
                                f"{type(e).__name__}: {str(e)}"
                            )
                        )
                    
                    # Re-raise exception
                    raise
        
        return wrapper
    return decorator


class HealthCheckStatus(str, Enum):
    """Health check status enumeration."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class ComponentHealth:
    """Health status of a component."""
    healthy: bool
    message: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


class HealthCheckManager:
    """
    Manager for health check endpoints.
    """
    
    def __init__(
        self,
        service_name: str,
        check_timeout: float = 5.0,
        max_workers: int = 10
    ):
        """
        Initialize health check manager.
        
        Args:
            service_name: Name of the service.
            check_timeout: Timeout for individual health checks in seconds.
            max_workers: Maximum number of workers for parallel health checks.
        """
        self.service_name = service_name
        self.check_timeout = check_timeout
        self.max_workers = max_workers
        self.components: Dict[str, Callable[[], Union[bool, ComponentHealth]]] = {}
        self.startup_checks: Dict[str, Callable[[], bool]] = {}
        self._status = HealthCheckStatus.HEALTHY
        self.logger = StructuredLogger(__name__)
    
    def register_component(
        self,
        name: str,
        check_fn: Callable[[], Union[bool, ComponentHealth]]
    ):
        """
        Register a component for health checking.
        
        Args:
            name: Component name.
            check_fn: Function that returns health status.
        """
        self.components[name] = check_fn
    
    def register_startup_check(
        self,
        name: str,
        check_fn: Callable[[], bool]
    ):
        """
        Register a startup check.
        
        Args:
            name: Check name.
            check_fn: Function that returns True if startup check passes.
        """
        self.startup_checks[name] = check_fn
    
    def get_status(self) -> HealthCheckStatus:
        """Get the current overall health status."""
        return self._status
    
    def get_components(self) -> List[str]:
        """Get list of registered components."""
        return list(self.components.keys())
    
    @instrument_method(span_name="health_check")
    def check_health(self) -> Dict[str, Any]:
        """
        Execute health checks for all components.
        
        Returns:
            Dictionary with health check results.
        """
        results = {
            "status": HealthCheckStatus.HEALTHY,
            "service": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "components": {}
        }
        
        # Check all components in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            for name, check_fn in self.components.items():
                future = executor.submit(self._execute_check, name, check_fn)
                futures[future] = name
            
            # Collect results
            unhealthy_count = 0
            degraded_count = 0
            
            for future in futures:
                name = futures[future]
                try:
                    health = future.result(timeout=self.check_timeout)
                    results["components"][name] = self._format_health(health)
                    
                    if not health.healthy:
                        unhealthy_count += 1
                except TimeoutError:
                    results["components"][name] = self._format_health(
                        ComponentHealth(
                            healthy=False,
                            message=f"Health check timed out after {self.check_timeout}s"
                        )
                    )
                    unhealthy_count += 1
                except Exception as e:
                    results["components"][name] = self._format_health(
                        ComponentHealth(
                            healthy=False,
                            message=f"Health check failed: {type(e).__name__}: {str(e)}"
                        )
                    )
                    unhealthy_count += 1
        
        # Determine overall status
        if unhealthy_count > 0:
            results["status"] = HealthCheckStatus.UNHEALTHY
        elif degraded_count > 0:
            results["status"] = HealthCheckStatus.DEGRADED
        
        self._status = results["status"]
        return results
    
    def _execute_check(
        self,
        name: str,
        check_fn: Callable[[], Union[bool, ComponentHealth]]
    ) -> ComponentHealth:
        """Execute a single health check."""
        try:
            result = check_fn()
            
            # Convert bool to ComponentHealth
            if isinstance(result, bool):
                return ComponentHealth(
                    healthy=result,
                    message="OK" if result else "Check failed"
                )
            
            return result
        except Exception as e:
            self.logger.error(
                f"Health check failed for component {name}",
                component=name,
                error=str(e)
            )
            raise
    
    def _format_health(self, health: ComponentHealth) -> Dict[str, Any]:
        """Format health status for output."""
        result = {
            "healthy": health.healthy,
            "message": health.message
        }
        
        if health.metadata:
            result["metadata"] = health.metadata
        
        return result
    
    def liveness_probe(self) -> Dict[str, Any]:
        """
        Liveness probe endpoint.
        
        Returns:
            Dictionary indicating if service is alive.
        """
        return {
            "status": "alive",
            "service": self.service_name,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def readiness_probe(self) -> Dict[str, Any]:
        """
        Readiness probe endpoint.
        
        Returns:
            Dictionary indicating if service is ready to accept traffic.
        """
        health_result = self.check_health()
        
        is_ready = health_result["status"] != HealthCheckStatus.UNHEALTHY
        
        result = {
            "ready": is_ready,
            "service": self.service_name,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if not is_ready:
            unhealthy_components = [
                name for name, health in health_result["components"].items()
                if not health["healthy"]
            ]
            result["reason"] = f"Unhealthy components: {', '.join(unhealthy_components)}"
        
        return result
    
    def startup_probe(self) -> Dict[str, Any]:
        """
        Startup probe endpoint.
        
        Returns:
            Dictionary indicating if service has started successfully.
        """
        results = {
            "started": True,
            "service": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "checks": {}
        }
        
        for name, check_fn in self.startup_checks.items():
            try:
                passed = check_fn()
                results["checks"][name] = passed
                if not passed:
                    results["started"] = False
            except Exception as e:
                results["checks"][name] = False
                results["started"] = False
                self.logger.error(
                    f"Startup check failed for {name}",
                    check=name,
                    error=str(e)
                )
        
        return results