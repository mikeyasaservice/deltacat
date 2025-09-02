"""Connection pooling for catalog operations."""

import logging
import threading
import time
from collections import deque
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, Generic, Optional, TypeVar, Union
import duckdb
from deltacat import logs
from deltacat.config.performance import ConnectionPoolConfig, get_performance_config
from deltacat.exceptions import DeltaCATException

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

T = TypeVar("T")


@dataclass
class PooledConnection(Generic[T]):
    """Wrapper for a pooled connection."""
    
    connection: T
    created_at: float
    last_used_at: float
    usage_count: int = 0
    is_valid: bool = True
    pool_id: str = ""
    
    def use(self) -> None:
        """Mark the connection as used."""
        self.last_used_at = time.time()
        self.usage_count += 1
    
    def is_expired(self, max_idle_time: int) -> bool:
        """Check if the connection has been idle too long."""
        return (time.time() - self.last_used_at) > max_idle_time


class ConnectionPoolError(DeltaCATException):
    """Exception raised for connection pool errors."""
    pass


class ConnectionPool(Generic[T]):
    """Generic connection pool implementation."""
    
    def __init__(
        self,
        name: str,
        create_connection: Callable[[], T],
        validate_connection: Optional[Callable[[T], bool]] = None,
        close_connection: Optional[Callable[[T], None]] = None,
        config: Optional[ConnectionPoolConfig] = None,
    ):
        """Initialize the connection pool.
        
        Args:
            name: Name of the pool for logging
            create_connection: Function to create a new connection
            validate_connection: Optional function to validate a connection
            close_connection: Optional function to close a connection
            config: Pool configuration
        """
        self.name = name
        self.create_connection = create_connection
        self.validate_connection = validate_connection or (lambda x: True)
        self.close_connection = close_connection or (lambda x: None)
        
        self.config = config or get_performance_config().connection_pool
        
        self._pool: Deque[PooledConnection[T]] = deque()
        self._in_use: Dict[int, PooledConnection[T]] = {}
        self._lock = threading.RLock() if self.config.thread_safe else None
        self._closed = False
        self._created_count = 0
        
        # Pre-create minimum connections
        for _ in range(self.config.min_size):
            conn = self._create_and_add_connection()
            self._pool.append(conn)
        
        logger.info(f"Initialized connection pool '{name}' with {self.config.min_size} connections")
    
    def _create_and_add_connection(self) -> PooledConnection[T]:
        """Create a new connection and add it to the pool."""
        if self._created_count >= self.config.max_size:
            raise ConnectionPoolError(f"Pool '{self.name}' has reached maximum size ({self.config.max_size})")
        
        try:
            conn = self.create_connection()
            pooled_conn = PooledConnection(
                connection=conn,
                created_at=time.time(),
                last_used_at=time.time(),
                pool_id=f"{self.name}_{self._created_count}",
            )
            self._created_count += 1
            return pooled_conn
        except Exception as e:
            logger.error(f"Failed to create connection for pool '{self.name}': {e}")
            raise ConnectionPoolError(f"Failed to create connection: {e}")
    
    def _validate_and_cleanup(self) -> None:
        """Validate connections and remove expired ones."""
        current_time = time.time()
        to_remove = []
        
        for conn in list(self._pool):
            # Check if expired
            if conn.is_expired(self.config.max_idle_time):
                to_remove.append(conn)
                continue
            
            # Check if still valid
            try:
                if not self.validate_connection(conn.connection):
                    conn.is_valid = False
                    to_remove.append(conn)
            except Exception:
                conn.is_valid = False
                to_remove.append(conn)
        
        # Remove invalid connections
        for conn in to_remove:
            self._pool.remove(conn)
            try:
                self.close_connection(conn.connection)
            except Exception as e:
                logger.warning(f"Error closing connection in pool '{self.name}': {e}")
            self._created_count -= 1
    
    @contextmanager
    def get_connection(self, timeout: Optional[int] = None):
        """Get a connection from the pool.
        
        Args:
            timeout: Optional timeout in seconds
            
        Yields:
            A connection from the pool
        """
        if self._closed:
            raise ConnectionPoolError(f"Pool '{self.name}' is closed")
        
        timeout = timeout or self.config.connection_timeout
        start_time = time.time()
        
        while True:
            with self._lock if self._lock else nullcontext():
                # Clean up expired connections
                self._validate_and_cleanup()
                
                # Try to get a connection from the pool
                if self._pool:
                    pooled_conn = self._pool.popleft()
                    pooled_conn.use()
                    self._in_use[id(pooled_conn.connection)] = pooled_conn
                    
                    try:
                        yield pooled_conn.connection
                    finally:
                        # Return connection to pool
                        with self._lock if self._lock else nullcontext():
                            del self._in_use[id(pooled_conn.connection)]
                            if pooled_conn.is_valid and not self._closed:
                                self._pool.append(pooled_conn)
                            else:
                                try:
                                    self.close_connection(pooled_conn.connection)
                                except Exception as e:
                                    logger.warning(f"Error returning connection to pool '{self.name}': {e}")
                                self._created_count -= 1
                    return
                
                # Try to create a new connection if below max size
                if self._created_count < self.config.max_size:
                    pooled_conn = self._create_and_add_connection()
                    self._pool.append(pooled_conn)
                    continue
                
                # Check timeout
                if (time.time() - start_time) > timeout:
                    raise ConnectionPoolError(
                        f"Timeout waiting for connection from pool '{self.name}' "
                        f"(timeout={timeout}s, in_use={len(self._in_use)}, pool_size={len(self._pool)})"
                    )
                
                # Wait a bit before retrying
                time.sleep(0.1)
    
    def close(self) -> None:
        """Close all connections in the pool."""
        with self._lock if self._lock else nullcontext():
            self._closed = True
            
            # Close pooled connections
            for conn in self._pool:
                try:
                    self.close_connection(conn.connection)
                except Exception as e:
                    logger.warning(f"Error closing connection in pool '{self.name}': {e}")
            
            # Close in-use connections
            for conn in self._in_use.values():
                try:
                    self.close_connection(conn.connection)
                except Exception as e:
                    logger.warning(f"Error closing in-use connection in pool '{self.name}': {e}")
            
            self._pool.clear()
            self._in_use.clear()
            
            logger.info(f"Closed connection pool '{self.name}'")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        with self._lock if self._lock else nullcontext():
            return {
                "name": self.name,
                "total_created": self._created_count,
                "available": len(self._pool),
                "in_use": len(self._in_use),
                "closed": self._closed,
            }


class DuckDBConnectionPool(ConnectionPool[duckdb.DuckDBPyConnection]):
    """Connection pool specifically for DuckDB connections."""
    
    def __init__(self, config: Optional[ConnectionPoolConfig] = None, **duckdb_config):
        """Initialize DuckDB connection pool.
        
        Args:
            config: Pool configuration
            **duckdb_config: DuckDB connection configuration
        """
        config = config or get_performance_config().connection_pool
        
        # Extract database path from config if provided
        db_path = duckdb_config.pop('database', ':memory:')
        db_config = {k: v for k, v in duckdb_config.items() if k != 'database'}
        
        def create_connection():
            return duckdb.connect(db_path, config=db_config)
        
        def validate_connection(conn):
            try:
                conn.execute("SELECT 1").fetchone()
                return True
            except Exception:
                return False
        
        def close_connection(conn):
            try:
                conn.close()
            except Exception:
                pass
        
        super().__init__(
            name="DuckDB",
            create_connection=create_connection,
            validate_connection=validate_connection,
            close_connection=close_connection,
            config=config,
        )


class ConnectionPoolManager:
    """Manager for multiple connection pools."""
    
    def __init__(self, config: Optional[ConnectionPoolConfig] = None):
        """Initialize the connection pool manager.
        
        Args:
            config: Pool configuration
        """
        self.config = config or get_performance_config().connection_pool
        self._pools: Dict[str, ConnectionPool] = {}
        self._lock = threading.RLock()
        
        logger.info("Initialized ConnectionPoolManager")
    
    def get_or_create_pool(
        self,
        pool_name: str,
        create_connection: Callable[[], Any],
        validate_connection: Optional[Callable[[Any], bool]] = None,
        close_connection: Optional[Callable[[Any], None]] = None,
    ) -> ConnectionPool:
        """Get or create a connection pool.
        
        Args:
            pool_name: Name of the pool
            create_connection: Function to create connections
            validate_connection: Optional validation function
            close_connection: Optional close function
            
        Returns:
            The connection pool
        """
        with self._lock:
            if pool_name not in self._pools:
                self._pools[pool_name] = ConnectionPool(
                    name=pool_name,
                    create_connection=create_connection,
                    validate_connection=validate_connection,
                    close_connection=close_connection,
                    config=self.config,
                )
                logger.info(f"Created new connection pool: {pool_name}")
            
            return self._pools[pool_name]
    
    def get_duckdb_pool(self, **duckdb_config) -> DuckDBConnectionPool:
        """Get or create a DuckDB connection pool.
        
        Args:
            **duckdb_config: DuckDB configuration
            
        Returns:
            DuckDB connection pool
        """
        pool_name = "duckdb_default"
        
        with self._lock:
            if pool_name not in self._pools:
                self._pools[pool_name] = DuckDBConnectionPool(
                    config=self.config,
                    **duckdb_config
                )
            
            return self._pools[pool_name]
    
    def close_pool(self, pool_name: str) -> None:
        """Close a specific pool.
        
        Args:
            pool_name: Name of the pool to close
        """
        with self._lock:
            if pool_name in self._pools:
                self._pools[pool_name].close()
                del self._pools[pool_name]
                logger.info(f"Closed connection pool: {pool_name}")
    
    def close_all(self) -> None:
        """Close all connection pools."""
        with self._lock:
            for pool_name, pool in self._pools.items():
                pool.close()
            self._pools.clear()
            logger.info("Closed all connection pools")
    
    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all pools."""
        with self._lock:
            return {
                pool_name: pool.get_stats()
                for pool_name, pool in self._pools.items()
            }


# Global connection pool manager
_global_pool_manager: Optional[ConnectionPoolManager] = None


def get_pool_manager() -> ConnectionPoolManager:
    """Get the global connection pool manager."""
    global _global_pool_manager
    if _global_pool_manager is None:
        _global_pool_manager = ConnectionPoolManager()
    return _global_pool_manager


def reset_pool_manager() -> None:
    """Reset the global connection pool manager."""
    global _global_pool_manager
    if _global_pool_manager:
        _global_pool_manager.close_all()
    _global_pool_manager = None


# Context manager helper
class nullcontext:
    """Null context manager for when thread safety is disabled."""
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass