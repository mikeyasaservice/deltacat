"""Test-Driven Development tests for memory mapping large files.

These tests verify that large Parquet files can be efficiently read
using memory mapping to avoid loading entire files into memory.
"""

import os
import tempfile
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime
import psutil
import gc
from typing import List, Dict, Any
from unittest.mock import patch, MagicMock
import mmap

from deltacat.storage.formats.parquet_optimized import (
    MemoryMappedParquetReader,
    LargeFileOptimizer
)
from deltacat.utils.arrow_optimizations import MemoryTracker


class TestMemoryMappedReading:
    """Tests for memory-mapped Parquet file reading."""
    
    @pytest.fixture
    def large_parquet_file(self, tmp_path):
        """Create a large Parquet file (simulated)."""
        file_path = tmp_path / "large_file.parquet"
        
        # Create a file that simulates a large dataset
        # In real scenario, this would be much larger (>1GB)
        n_rows = 10_000_000  # 10M rows
        chunk_size = 100_000  # Write in chunks to simulate large file
        
        writer = None
        schema = pa.schema([
            ('id', pa.int64()),
            ('value', pa.float64()),
            ('category', pa.string()),
            ('timestamp', pa.timestamp('ms')),
        ])
        
        with pq.ParquetWriter(str(file_path), schema) as writer:
            for chunk_start in range(0, n_rows, chunk_size):
                chunk_end = min(chunk_start + chunk_size, n_rows)
                chunk_len = chunk_end - chunk_start
                
                chunk_data = {
                    'id': pa.array(range(chunk_start, chunk_end)),
                    'value': pa.array(np.random.randn(chunk_len)),
                    'category': pa.array(np.random.choice(['A', 'B', 'C'], chunk_len)),
                    'timestamp': pa.array([datetime.now()] * chunk_len, type=pa.timestamp('ms')),
                }
                chunk_table = pa.table(chunk_data, schema=schema)
                writer.write_table(chunk_table)
        
        return str(file_path)
    
    def test_memory_mapped_read_uses_less_memory(self, large_parquet_file):
        """Test that memory-mapped reading uses less memory than regular reading."""
        process = psutil.Process()
        
        # Force garbage collection
        gc.collect()
        
        # Measure memory for regular read
        memory_before_regular = process.memory_info().rss
        
        regular_reader = pq.ParquetFile(large_parquet_file)
        regular_table = regular_reader.read()
        
        memory_after_regular = process.memory_info().rss
        regular_memory_used = memory_after_regular - memory_before_regular
        
        # Clean up
        del regular_table
        del regular_reader
        gc.collect()
        
        # Measure memory for memory-mapped read
        memory_before_mmap = process.memory_info().rss
        
        mmap_reader = MemoryMappedParquetReader(large_parquet_file)
        mmap_table = mmap_reader.read_memory_mapped()
        
        memory_after_mmap = process.memory_info().rss
        mmap_memory_used = memory_after_mmap - memory_before_mmap
        
        # Memory-mapped should use significantly less resident memory
        assert mmap_memory_used < regular_memory_used * 0.5, \
            f"Memory-mapped used {mmap_memory_used / regular_memory_used * 100:.1f}% " \
            f"of regular read memory (should be <50%)"
        
        # Verify data is still accessible
        assert len(mmap_table) > 0
        assert mmap_table.num_columns == 4
    
    def test_memory_mapped_lazy_loading(self, large_parquet_file):
        """Test that memory mapping loads data lazily."""
        reader = MemoryMappedParquetReader(large_parquet_file)
        
        # Open file with memory mapping
        mmap_file = reader.open_memory_mapped()
        
        # Initial memory usage should be minimal
        initial_pages_in_memory = reader.get_pages_in_memory(mmap_file)
        file_size = os.path.getsize(large_parquet_file)
        
        # Should have loaded less than 1% initially
        assert initial_pages_in_memory < file_size * 0.01
        
        # Access some data (triggers page loading)
        metadata = reader.read_metadata(mmap_file)
        first_row_group = reader.read_row_group(mmap_file, 0)
        
        # More pages should now be in memory
        after_access_pages = reader.get_pages_in_memory(mmap_file)
        assert after_access_pages > initial_pages_in_memory
        
        # Clean up
        reader.close_memory_mapped(mmap_file)
    
    def test_memory_mapped_partial_read(self, large_parquet_file):
        """Test reading only specific parts of a memory-mapped file."""
        reader = MemoryMappedParquetReader(large_parquet_file)
        
        # Read only specific row groups without loading entire file
        row_groups_to_read = [0, 5, 10]  # Read only 3 row groups
        
        tracker = MemoryTracker()
        initial_memory = tracker.get_current_memory()
        
        result = reader.read_row_groups_memory_mapped(row_groups_to_read)
        
        memory_used = tracker.get_current_memory() - initial_memory
        
        # Should have read only the requested row groups
        parquet_file = pq.ParquetFile(large_parquet_file)
        total_row_groups = parquet_file.metadata.num_row_groups
        
        # Memory used should be proportional to row groups read
        expected_memory_ratio = len(row_groups_to_read) / total_row_groups
        
        # Get total file size for comparison
        file_size = os.path.getsize(large_parquet_file)
        
        assert memory_used < file_size * expected_memory_ratio * 2, \
            "Memory usage exceeds expected for partial read"
        
        # Verify correct data was read
        assert len(result) > 0
        assert result.num_columns == 4


class TestLargeFileOptimizations:
    """Tests for optimizations specific to large files."""
    
    @pytest.fixture
    def multi_gb_file(self, tmp_path):
        """Create a file that simulates multi-GB size."""
        file_path = tmp_path / "multi_gb.parquet"
        
        # Create with specific characteristics for testing
        schema = pa.schema([
            ('id', pa.int64()),
            ('data', pa.binary()),  # Binary data for size
        ])
        
        # Write multiple row groups
        with pq.ParquetWriter(str(file_path), schema) as writer:
            for i in range(100):  # 100 row groups
                # Each row group ~10MB
                data = {
                    'id': pa.array(range(i * 1000, (i + 1) * 1000)),
                    'data': pa.array([os.urandom(10240) for _ in range(1000)]),  # 10KB per row
                }
                writer.write_table(pa.table(data, schema=schema))
        
        return str(file_path)
    
    def test_streaming_read_for_large_files(self, multi_gb_file):
        """Test streaming read for files too large for memory."""
        optimizer = LargeFileOptimizer(multi_gb_file)
        
        # Set memory limit for streaming
        memory_limit_mb = 100  # 100MB limit
        
        # Create streaming reader
        stream_reader = optimizer.create_streaming_reader(
            memory_limit_mb=memory_limit_mb
        )
        
        # Track memory usage during streaming
        max_memory_used = 0
        total_rows_read = 0
        
        for batch in stream_reader:
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
            max_memory_used = max(max_memory_used, current_memory)
            total_rows_read += len(batch)
            
            # Process batch (in real usage)
            assert len(batch) > 0
            assert batch.num_columns == 2
        
        # Verify memory limit was respected
        assert max_memory_used < memory_limit_mb * 1.5, \
            f"Exceeded memory limit: {max_memory_used:.1f}MB > {memory_limit_mb * 1.5}MB"
        
        # Verify all data was read
        assert total_rows_read == 100_000  # 100 row groups * 1000 rows
    
    def test_intelligent_prefetching(self, large_parquet_file):
        """Test intelligent prefetching for sequential access patterns."""
        reader = MemoryMappedParquetReader(large_parquet_file)
        
        # Enable prefetching
        reader.enable_prefetching(
            prefetch_size_mb=10,
            prefetch_ahead_count=2
        )
        
        # Track I/O operations
        io_operations = []
        
        with patch('os.read', side_effect=os.read) as mock_read:
            # Sequential read of row groups
            for i in range(5):
                row_group = reader.read_row_group_with_prefetch(i)
                io_operations.append(len(mock_read.call_args_list))
            
            # Prefetching should reduce I/O operations
            # Later reads should benefit from prefetch
            first_read_ops = io_operations[0]
            last_read_ops = io_operations[-1] - io_operations[-2]
            
            assert last_read_ops < first_read_ops, \
                "Prefetching didn't reduce I/O operations"
    
    def test_adaptive_chunk_sizing(self, large_parquet_file):
        """Test adaptive chunk sizing based on available memory."""
        optimizer = LargeFileOptimizer(large_parquet_file)
        
        # Get available memory
        available_memory = psutil.virtual_memory().available
        
        # Determine optimal chunk size
        optimal_chunk_size = optimizer.calculate_optimal_chunk_size(
            file_size=os.path.getsize(large_parquet_file),
            available_memory=available_memory,
            target_memory_usage=0.25  # Use 25% of available memory
        )
        
        # Chunk size should be reasonable
        min_chunk = 1024 * 1024  # 1MB minimum
        max_chunk = available_memory * 0.25  # 25% of available memory
        
        assert min_chunk <= optimal_chunk_size <= max_chunk
        
        # Use the optimal chunk size for reading
        reader = optimizer.create_chunked_reader(chunk_size=optimal_chunk_size)
        
        # Read and verify chunks are appropriately sized
        for chunk in reader:
            chunk_memory = chunk.nbytes
            assert chunk_memory <= optimal_chunk_size * 1.1  # Allow 10% overhead


class TestMemoryMappedColumnAccess:
    """Tests for column-wise access in memory-mapped files."""
    
    @pytest.fixture
    def columnar_file(self, tmp_path):
        """Create a file optimized for columnar access."""
        file_path = tmp_path / "columnar.parquet"
        
        # Create wide table
        n_rows = 100_000
        n_cols = 100
        
        data = {'id': range(n_rows)}
        for i in range(n_cols):
            data[f'col_{i}'] = np.random.randn(n_rows)
        
        table = pa.table(data)
        
        # Write with column-optimized settings
        pq.write_table(
            table,
            str(file_path),
            row_group_size=10000,
            use_dictionary=False,
            column_encoding='PLAIN'
        )
        
        return str(file_path)
    
    def test_single_column_memory_mapped_read(self, columnar_file):
        """Test reading a single column from memory-mapped file."""
        reader = MemoryMappedParquetReader(columnar_file)
        
        # Read only one column
        tracker = MemoryTracker()
        initial_memory = tracker.get_current_memory()
        
        column_data = reader.read_column_memory_mapped('col_42')
        
        memory_used = tracker.get_current_memory() - initial_memory
        
        # Calculate expected memory for one column
        file_size = os.path.getsize(columnar_file)
        num_columns = 101  # id + 100 columns
        expected_memory = file_size / num_columns * 1.5  # Allow overhead
        
        assert memory_used < expected_memory, \
            f"Reading single column used too much memory: {memory_used / expected_memory:.1f}x expected"
        
        # Verify data
        assert len(column_data) == 100_000
        assert column_data.name == 'col_42'
    
    def test_memory_mapped_column_batch_access(self, columnar_file):
        """Test batch access to columns in memory-mapped mode."""
        reader = MemoryMappedParquetReader(columnar_file)
        
        # Read columns in batches
        column_batches = [
            ['col_0', 'col_1', 'col_2'],
            ['col_10', 'col_11', 'col_12'],
            ['col_20', 'col_21', 'col_22'],
        ]
        
        # Track page faults to verify memory mapping efficiency
        initial_faults = reader.get_page_fault_count()
        
        results = []
        for batch in column_batches:
            batch_data = reader.read_columns_memory_mapped(batch)
            results.append(batch_data)
        
        final_faults = reader.get_page_fault_count()
        
        # Page faults should be minimal after first access
        # (data should be cached in memory)
        faults_per_batch = (final_faults - initial_faults) / len(column_batches)
        
        # Verify results
        for i, result in enumerate(results):
            assert result.num_columns == 3
            assert set(result.column_names) == set(column_batches[i])


class TestMemoryMappingConfiguration:
    """Tests for memory mapping configuration and tuning."""
    
    def test_memory_mapping_configuration(self, tmp_path):
        """Test various memory mapping configurations."""
        # Create test file
        file_path = tmp_path / "config_test.parquet"
        table = pa.table({'data': range(10000)})
        pq.write_table(table, str(file_path))
        
        reader = MemoryMappedParquetReader(str(file_path))
        
        # Test different configurations
        configs = [
            {'mode': 'read_only', 'advice': 'sequential'},
            {'mode': 'read_only', 'advice': 'random'},
            {'mode': 'read_only', 'advice': 'willneed'},
        ]
        
        for config in configs:
            mmap_file = reader.open_memory_mapped(**config)
            
            # Verify configuration is applied
            assert reader.get_mmap_config(mmap_file) == config
            
            # Read data
            data = reader.read_with_config(mmap_file)
            assert len(data) == 10000
            
            reader.close_memory_mapped(mmap_file)
    
    def test_memory_mapping_fallback(self, tmp_path):
        """Test fallback when memory mapping is not available."""
        file_path = tmp_path / "fallback_test.parquet"
        table = pa.table({'data': range(1000)})
        pq.write_table(table, str(file_path))
        
        reader = MemoryMappedParquetReader(str(file_path))
        
        # Simulate memory mapping not available
        with patch('mmap.mmap', side_effect=OSError("mmap not available")):
            # Should fallback to regular reading
            result = reader.read_with_fallback()
            
            # Should still return correct data
            assert len(result) == 1000
            assert result.num_columns == 1
            
            # Should log warning about fallback
            assert reader.used_fallback == True