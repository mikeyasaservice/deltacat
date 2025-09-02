"""Test-Driven Development tests for Arrow zero-copy optimizations.

These tests verify that Arrow operations use zero-copy when possible,
avoiding unnecessary memory allocations and copies.
"""

import gc
import pytest
import pyarrow as pa
import numpy as np
from typing import List, Any
import sys

from deltacat.utils.arrow_optimizations import (
    ZeroCopyOperations,
    MemoryTracker,
    ArrowOptimizer
)


class TestZeroCopySlicing:
    """Tests for zero-copy slicing operations."""
    
    @pytest.fixture
    def large_table(self):
        """Create a large Arrow table for testing."""
        n_rows = 1_000_000
        data = {
            'id': pa.array(np.arange(n_rows)),
            'value': pa.array(np.random.randn(n_rows)),
            'category': pa.array(['A', 'B', 'C'] * (n_rows // 3 + 1))[:n_rows],
        }
        return pa.table(data)
    
    def test_slice_is_zero_copy(self, large_table):
        """Test that slicing creates a zero-copy view."""
        optimizer = ArrowOptimizer()
        
        # Get memory address of original data
        original_buffers = optimizer.get_buffer_addresses(large_table)
        
        # Slice the table
        sliced = optimizer.zero_copy_slice(large_table, offset=1000, length=5000)
        
        # Get memory address of sliced data
        sliced_buffers = optimizer.get_buffer_addresses(sliced)
        
        # Buffer addresses should be the same (zero-copy)
        for col_name in large_table.column_names:
            assert original_buffers[col_name] == sliced_buffers[col_name], \
                f"Column {col_name} was copied, not zero-copy"
        
        # Verify slice content is correct
        assert len(sliced) == 5000
        assert sliced['id'][0].as_py() == 1000
        assert sliced['id'][-1].as_py() == 5999
    
    def test_multiple_slices_share_memory(self, large_table):
        """Test that multiple slices share the same underlying memory."""
        optimizer = ArrowOptimizer()
        
        # Create multiple slices
        slice1 = optimizer.zero_copy_slice(large_table, 0, 100)
        slice2 = optimizer.zero_copy_slice(large_table, 50, 100)
        slice3 = optimizer.zero_copy_slice(large_table, 100, 100)
        
        # All slices should share memory with original
        original_buffers = optimizer.get_buffer_addresses(large_table)
        
        for slice_table in [slice1, slice2, slice3]:
            slice_buffers = optimizer.get_buffer_addresses(slice_table)
            for col_name in large_table.column_names:
                assert original_buffers[col_name] == slice_buffers[col_name]
    
    def test_memory_usage_with_slicing(self, large_table):
        """Test that slicing doesn't increase memory usage significantly."""
        tracker = MemoryTracker()
        optimizer = ArrowOptimizer()
        
        # Measure initial memory
        initial_memory = tracker.get_current_memory()
        
        # Create many slices
        slices = []
        for i in range(100):
            slice_start = i * 1000
            slice_table = optimizer.zero_copy_slice(
                large_table, 
                offset=slice_start, 
                length=1000
            )
            slices.append(slice_table)
        
        # Measure memory after slicing
        after_slicing = tracker.get_current_memory()
        
        # Memory increase should be minimal (just metadata, not data)
        memory_increase = after_slicing - initial_memory
        table_size = tracker.get_table_memory_size(large_table)
        
        # Should use less than 1% of original table size
        assert memory_increase < table_size * 0.01, \
            f"Memory increased by {memory_increase / table_size * 100:.1f}% (should be <1%)"


class TestZeroCopyColumnOperations:
    """Tests for zero-copy column operations."""
    
    @pytest.fixture
    def sample_table(self):
        """Create a sample table."""
        return pa.table({
            'a': [1, 2, 3, 4, 5],
            'b': [10, 20, 30, 40, 50],
            'c': ['x', 'y', 'z', 'w', 'v'],
            'd': [1.1, 2.2, 3.3, 4.4, 5.5],
        })
    
    def test_column_selection_is_zero_copy(self, sample_table):
        """Test that selecting columns is zero-copy."""
        optimizer = ArrowOptimizer()
        
        # Select subset of columns
        selected = optimizer.zero_copy_select_columns(sample_table, ['a', 'c'])
        
        # Check that selected columns share memory with original
        original_a_buffer = optimizer.get_column_buffer_address(sample_table, 'a')
        selected_a_buffer = optimizer.get_column_buffer_address(selected, 'a')
        
        assert original_a_buffer == selected_a_buffer, "Column 'a' was copied"
        
        # Verify result
        assert selected.num_columns == 2
        assert selected.column_names == ['a', 'c']
        assert len(selected) == 5
    
    def test_column_reordering_is_zero_copy(self, sample_table):
        """Test that reordering columns doesn't copy data."""
        optimizer = ArrowOptimizer()
        
        # Reorder columns
        reordered = optimizer.zero_copy_reorder_columns(
            sample_table, 
            ['d', 'b', 'a', 'c']
        )
        
        # Each column should still share memory
        for col_name in sample_table.column_names:
            original_buffer = optimizer.get_column_buffer_address(sample_table, col_name)
            reordered_buffer = optimizer.get_column_buffer_address(reordered, col_name)
            assert original_buffer == reordered_buffer, f"Column {col_name} was copied"
        
        # Verify order
        assert reordered.column_names == ['d', 'b', 'a', 'c']
    
    def test_add_column_preserves_zero_copy(self, sample_table):
        """Test that adding a column preserves zero-copy for existing columns."""
        optimizer = ArrowOptimizer()
        
        # Add a new column
        new_column = pa.array([100, 200, 300, 400, 500])
        extended = optimizer.zero_copy_add_column(
            sample_table, 
            'e', 
            new_column
        )
        
        # Original columns should still be zero-copy
        for col_name in sample_table.column_names:
            original_buffer = optimizer.get_column_buffer_address(sample_table, col_name)
            extended_buffer = optimizer.get_column_buffer_address(extended, col_name)
            assert original_buffer == extended_buffer, f"Column {col_name} was copied"
        
        # New table should have the new column
        assert 'e' in extended.column_names
        assert extended['e'].to_pylist() == [100, 200, 300, 400, 500]


class TestZeroCopyConcat:
    """Tests for zero-copy concatenation."""
    
    def test_concat_with_same_schema_is_zero_copy(self):
        """Test concatenating tables with same schema is zero-copy."""
        optimizer = ArrowOptimizer()
        
        # Create tables with same schema
        table1 = pa.table({'a': [1, 2], 'b': [10, 20]})
        table2 = pa.table({'a': [3, 4], 'b': [30, 40]})
        table3 = pa.table({'a': [5, 6], 'b': [50, 60]})
        
        # Concatenate
        concatenated = optimizer.zero_copy_concat([table1, table2, table3])
        
        # Check that each chunk shares memory with original tables
        chunks_a = concatenated['a'].chunks
        assert len(chunks_a) == 3  # Should have 3 chunks
        
        # Each chunk should share memory with original
        assert optimizer.get_array_buffer_address(chunks_a[0]) == \
               optimizer.get_array_buffer_address(table1['a'].chunks[0])
        assert optimizer.get_array_buffer_address(chunks_a[1]) == \
               optimizer.get_array_buffer_address(table2['a'].chunks[0])
        assert optimizer.get_array_buffer_address(chunks_a[2]) == \
               optimizer.get_array_buffer_address(table3['a'].chunks[0])
        
        # Verify content
        assert concatenated['a'].to_pylist() == [1, 2, 3, 4, 5, 6]
        assert concatenated['b'].to_pylist() == [10, 20, 30, 40, 50, 60]
    
    def test_concat_memory_efficiency(self):
        """Test that concatenation is memory efficient."""
        optimizer = ArrowOptimizer()
        tracker = MemoryTracker()
        
        # Create multiple tables
        tables = []
        for i in range(10):
            table = pa.table({
                'id': pa.array(range(i * 1000, (i + 1) * 1000)),
                'value': pa.array(np.random.randn(1000))
            })
            tables.append(table)
        
        # Measure memory before concat
        initial_memory = tracker.get_current_memory()
        
        # Concatenate
        concatenated = optimizer.zero_copy_concat(tables)
        
        # Measure memory after concat
        after_concat = tracker.get_current_memory()
        
        # Memory increase should be minimal
        memory_increase = after_concat - initial_memory
        total_data_size = sum(tracker.get_table_memory_size(t) for t in tables)
        
        # Should use less than 5% additional memory
        assert memory_increase < total_data_size * 0.05


class TestZeroCopyFilter:
    """Tests for zero-copy filtering where possible."""
    
    def test_filter_with_boolean_mask(self):
        """Test filtering with boolean mask."""
        optimizer = ArrowOptimizer()
        
        # Create table
        table = pa.table({
            'id': range(1000),
            'value': np.random.randn(1000)
        })
        
        # Create boolean mask
        mask = pa.array([i % 2 == 0 for i in range(1000)])
        
        # Filter (this may not be zero-copy but should be efficient)
        filtered = optimizer.efficient_filter(table, mask)
        
        # Verify result
        assert len(filtered) == 500  # Half the rows
        assert all(filtered['id'][i].as_py() % 2 == 0 for i in range(len(filtered)))
    
    def test_take_is_efficient(self):
        """Test that take operation is memory efficient."""
        optimizer = ArrowOptimizer()
        tracker = MemoryTracker()
        
        # Create large table
        n_rows = 100_000
        table = pa.table({
            'id': range(n_rows),
            'data': np.random.randn(n_rows)
        })
        
        # Select specific indices (10% of data)
        indices = pa.array(np.random.choice(n_rows, size=n_rows // 10, replace=False))
        
        initial_memory = tracker.get_current_memory()
        
        # Take operation
        result = optimizer.efficient_take(table, indices)
        
        after_memory = tracker.get_current_memory()
        
        # Memory for result should be ~10% of original
        memory_increase = after_memory - initial_memory
        original_size = tracker.get_table_memory_size(table)
        
        assert memory_increase < original_size * 0.15  # Allow some overhead


class TestMemoryViews:
    """Test creation and usage of memory views."""
    
    def test_create_memory_view(self):
        """Test creating memory views of Arrow data."""
        optimizer = ArrowOptimizer()
        
        # Create array
        arr = pa.array([1, 2, 3, 4, 5])
        
        # Create memory view
        view = optimizer.create_memory_view(arr)
        
        # Modifications to view should affect original
        if hasattr(view, 'writeable') and view.writeable:
            view[0] = 100
            assert arr[0].as_py() == 100
        
        # View should not allocate new memory
        assert optimizer.get_array_buffer_address(arr) == id(view.obj)
    
    def test_numpy_zero_copy_conversion(self):
        """Test zero-copy conversion between Arrow and NumPy."""
        optimizer = ArrowOptimizer()
        
        # Create Arrow array
        arrow_array = pa.array(np.arange(1000, dtype=np.float64))
        
        # Convert to NumPy with zero-copy
        numpy_array = optimizer.arrow_to_numpy_zero_copy(arrow_array)
        
        # Should share memory
        arrow_buffer_addr = optimizer.get_array_buffer_address(arrow_array)
        numpy_buffer_addr = numpy_array.__array_interface__['data'][0]
        
        # The addresses might not be exactly equal but should be in same region
        assert abs(arrow_buffer_addr - numpy_buffer_addr) < 1000, \
            "NumPy array doesn't share memory with Arrow array"
        
        # Verify content
        assert np.array_equal(numpy_array, np.arange(1000, dtype=np.float64))


class TestOptimizationValidation:
    """Tests to validate that optimizations are actually working."""
    
    def test_validate_zero_copy_operation(self):
        """Test the validation mechanism for zero-copy operations."""
        optimizer = ArrowOptimizer()
        
        table = pa.table({'a': [1, 2, 3], 'b': [4, 5, 6]})
        
        # This should be zero-copy
        sliced = table.slice(0, 2)
        assert optimizer.validate_zero_copy(table, sliced, operation='slice')
        
        # This creates a copy
        filtered = table.filter(pa.array([True, False, True]))
        assert not optimizer.validate_zero_copy(table, filtered, operation='filter')
    
    def test_optimization_report(self):
        """Test generation of optimization report."""
        optimizer = ArrowOptimizer()
        
        # Enable tracking
        optimizer.enable_tracking()
        
        # Perform various operations
        table = pa.table({'a': range(1000), 'b': range(1000)})
        
        sliced = optimizer.zero_copy_slice(table, 0, 100)
        selected = optimizer.zero_copy_select_columns(table, ['a'])
        concatenated = optimizer.zero_copy_concat([sliced, sliced])
        
        # Get optimization report
        report = optimizer.get_optimization_report()
        
        # Report should contain operation statistics
        assert 'operations' in report
        assert 'zero_copy_count' in report
        assert 'copy_count' in report
        assert 'memory_saved_bytes' in report
        
        # Should have performed zero-copy operations
        assert report['zero_copy_count'] >= 3
        assert report['memory_saved_bytes'] > 0